package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Principal struct {
	StreamerID string    `json:"streamer_id"`
	ExpiresAt  time.Time `json:"expires_at"`
}
type entry struct {
	p     Principal
	until time.Time
}
type Validator struct {
	URL, Key string
	TTL      time.Duration
	Client   *http.Client
	mu       sync.Mutex
	cache    map[string]entry
}

func NewValidator(url, key string, ttl time.Duration) *Validator {
	return &Validator{URL: url, Key: key, TTL: ttl, Client: &http.Client{Timeout: 5 * time.Second}, cache: map[string]entry{}}
}
func (v *Validator) InvalidateStreamer(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for k, e := range v.cache {
		if e.p.StreamerID == id {
			delete(v.cache, k)
		}
	}
}
func (v *Validator) Validate(ctx context.Context, token string) (Principal, error) {
	if !strings.HasPrefix(token, "ok_") {
		return Principal{}, fmt.Errorf("invalid token")
	}
	v.mu.Lock()
	e, ok := v.cache[token]
	v.mu.Unlock()
	if ok && time.Now().Before(e.until) && time.Now().Before(e.p.ExpiresAt) {
		return e.p, nil
	}
	b, _ := json.Marshal(map[string]string{"token": token})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.URL, bytes.NewReader(b))
	if err != nil {
		return Principal{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Api-Key", v.Key)
	resp, err := v.Client.Do(req)
	if err != nil {
		return Principal{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Principal{}, fmt.Errorf("token rejected: %s", resp.Status)
	}
	var p Principal
	if err = json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return p, err
	}
	if p.StreamerID == "" {
		return p, fmt.Errorf("missing streamer_id")
	}
	v.mu.Lock()
	v.cache[token] = entry{p, time.Now().Add(v.TTL)}
	v.mu.Unlock()
	return p, nil
}
