package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidatorCachesAndInvalidates(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Internal-Api-Key") != "secret" {
			t.Error("missing internal key")
		}
		json.NewEncoder(w).Encode(Principal{StreamerID: "streamer", ExpiresAt: time.Now().Add(time.Hour)})
	}))
	defer s.Close()
	v := NewValidator(s.URL, "secret", time.Minute)
	if _, e := v.Validate(context.Background(), "ok_valid"); e != nil {
		t.Fatal(e)
	}
	if _, e := v.Validate(context.Background(), "ok_valid"); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
	v.InvalidateStreamer("streamer")
	if _, e := v.Validate(context.Background(), "ok_valid"); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
}
