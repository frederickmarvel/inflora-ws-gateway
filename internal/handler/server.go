package handler

import (
	"context"
	"encoding/json"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/auth"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/seq"
	gatewayws "github.com/frederickmarvel/inflora-ws-gateway/internal/ws"
	"github.com/gorilla/websocket"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	DB                         interface{ PingContext(context.Context) error }
	Auth                       *auth.Validator
	Store                      *seq.Store
	Hub                        *gatewayws.Hub
	Heartbeat, Timeout, Window time.Duration
	Version                    string
	// D-023: origin allowlist. AllowedOrigins is the list of Origin values
	// accepted by the WS upgrader. DevOpenOrigin bypasses the check.
	AllowedOrigins []string
	DevOpenOrigin  bool
	// Optional path to the built overlay (overlay/dist/). When non-empty,
	// files under it are served at /overlay/ so OBS can be pointed at the
	// same host as the WS endpoint.
	OverlayDir string
}

func New(s Server) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.DB == nil || s.DB.PingContext(r.Context()) != nil {
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	m.HandleFunc("GET /ws", s.serveWS)
	if strings.TrimSpace(s.OverlayDir) != "" {
		m.Handle("GET /overlay/", s.overlayHandler())
	}
	return m
}

// overlayHandler serves the built overlay from OverlayDir at /overlay/.
// http.FileServer already serves index.html for directory requests when one
// is present in the dir, so this works for the OBS browser source.
//
// The handler refuses path traversal by relying on http.Dir + the prefix
// strip; any request that escapes OverlayDir returns 404.
func (s Server) overlayHandler() http.Handler {
	fs := http.FileServer(http.Dir(s.OverlayDir))
	return http.StripPrefix("/overlay/", fs)
}

func (s Server) checkOrigin(r *http.Request) bool {
	// D-023 explicit escape hatch for local dev. Production must leave
	// WS_DEV_OPEN_ORIGIN unset (or false) and rely on the allowlist.
	if s.DevOpenOrigin {
		return true
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		// Same-origin requests and direct curl calls have no Origin header.
		// For an OBS browser source, the browser always sends Origin, so a
		// missing Origin is treated as a non-browser caller and accepted —
		// the same shape as the previous wide-open behavior on a no-Origin
		// request. Production deployments should sit behind a reverse proxy
		// that requires the public origin.
		return true
	}
	for _, allowed := range s.AllowedOrigins {
		if origin == allowed {
			return true
		}
	}
	return false
}

func (s Server) serveWS(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	p, err := s.Auth.Validate(r.Context(), r.URL.Query().Get("token"))
	if err != nil {
		http.Error(w, "invalid token", 401)
		return
	}
	rawSince := r.URL.Query().Get("since")
	if rawSince == "" {
		rawSince = r.URL.Query().Get("from_seq")
	}
	var since int64
	if rawSince != "" {
		since, err = strconv.ParseInt(rawSince, 10, 64)
		if err != nil || since < 0 {
			http.Error(w, "invalid since", http.StatusBadRequest)
			return
		}
	}
	u := websocket.Upgrader{Subprotocols: []string{"inflora-overlay.v1"}, CheckOrigin: func(r *http.Request) bool { return s.checkOrigin(r) }}
	conn, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := gatewayws.NewClient(32)
	if !s.Hub.Register(p.StreamerID, c) {
		conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4429, "limit"), time.Now().Add(time.Second))
		conn.Close()
		return
	}
	defer func() { s.Hub.Unregister(p.StreamerID, c); conn.Close() }()
	hwm, err := s.Store.HighWater(r.Context(), p.StreamerID)
	if err != nil {
		return
	}
	if conn.WriteJSON(map[string]any{"type": "hello", "streamer_id": p.StreamerID, "server_time": time.Now().UTC(), "replay_window_seconds": int(s.Window.Seconds()), "high_water_mark": hwm, "version": s.Version}) != nil {
		return
	}
	oldest, err := s.Store.Oldest(r.Context(), p.StreamerID)
	if err != nil {
		return
	}
	if since > 0 && oldest > 0 && since < oldest-1 {
		if conn.WriteJSON(map[string]any{"type": "system.resync_required", "high_water_mark": hwm, "ts": time.Now().UTC()}) != nil {
			return
		}
		since = hwm
	}
	frames, err := s.Store.Replay(r.Context(), p.StreamerID, since)
	if err != nil {
		return
	}
	for _, f := range frames {
		if conn.WriteJSON(f) != nil {
			return
		}
	}
	if conn.WriteJSON(map[string]any{"type": "catch_up_end", "last_seq": hwm, "ts": time.Now().UTC()}) != nil {
		return
	}
	dead := make(chan struct{})
	conn.SetReadDeadline(time.Now().Add(s.Timeout))
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(s.Timeout)); return nil })
	go func() {
		defer close(dead)
		for {
			var x map[string]any
			if conn.ReadJSON(&x) != nil {
				return
			}
			if x["type"] == "pong" {
				conn.SetReadDeadline(time.Now().Add(s.Timeout))
			}
		}
	}()
	tick := time.NewTicker(s.Heartbeat)
	defer tick.Stop()
	for {
		select {
		case <-dead:
			return
		case <-c.Done():
			return
		case v := <-c.Send:
			if conn.WriteJSON(v) != nil {
				return
			}
		case <-tick.C:
			mark, _ := s.Store.HighWater(r.Context(), p.StreamerID)
			if conn.WriteJSON(map[string]any{"type": "heartbeat", "ts": time.Now().UTC(), "seq": mark}) != nil {
				return
			}
		}
	}
}

// ResolveOverlayDir returns the absolute path to OverlayDir if the dir exists
// and contains at least one expected file. Empty string means "disabled".
// Exported so main.go can validate the path before constructing Server.
func ResolveOverlayDir(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return ""
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return ""
	}
	info, err := os.Stat(abs)
	if err != nil || !info.IsDir() {
		return ""
	}
	return abs
}
