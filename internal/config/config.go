package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName, LogLevel, LogFormat, HTTPAddr, DBDSN, NATSURL, NATSStream, InternalKey, TolkienURL string
	DBMaxOpen, DBMaxIdle, MaxConnections                                                            int
	HeartbeatInterval, HeartbeatTimeout, ReplayWindow, AuthCacheTTL, ShutdownTimeout                time.Duration
	// D-023: origin allowlist + dev override. If DevOpenOrigin is true the
	// CheckOrigin function returns true for every request. In production it
	// must be false and AllowedOrigins must be set to the public web origin.
	AllowedOrigins []string
	DevOpenOrigin  bool
	// WS_OVERLAY_DIR points at a directory containing the built overlay
	// (dist/overlay.js + index.html). When set, the gateway serves it under
	// /overlay/ so OBS can be pointed at the same host as the WS endpoint.
	OverlayDir string
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envBool(k string, d bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	if v == "" {
		return d
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
func envCSV(k, d string) []string {
	v := env(k, d)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func duration(k string, d time.Duration) time.Duration {
	v := env(k, "")
	if v == "" {
		return d
	}
	n, e := strconv.Atoi(v)
	if e != nil || n <= 0 {
		return d
	}
	return time.Duration(n) * time.Second
}
func integer(k string, d int) int {
	n, e := strconv.Atoi(env(k, ""))
	if e != nil {
		return d
	}
	return n
}
func Load() (Config, error) {
	c := Config{
		ServiceName:    env("SERVICE_NAME", "ws-gateway"),
		LogLevel:       env("LOG_LEVEL", "info"),
		LogFormat:      env("LOG_FORMAT", "json"),
		HTTPAddr:       env("WS_ADDR", ":8083"),
		DBDSN:          os.Getenv("DB_DSN"),
		NATSURL:        env("NATS_URL", "nats://localhost:4222"),
		NATSStream:     env("NATS_STREAM_NAME", "inflora-events"),
		InternalKey:    os.Getenv("INTERNAL_API_KEY"),
		TolkienURL:     env("TOLKIEN_VALIDATE_TOKEN_URL", "http://localhost:8080/v1/internal/validate-overlay-token"),
		DBMaxOpen:      integer("DB_MAX_OPEN_CONNS", 20),
		DBMaxIdle:      integer("DB_MAX_IDLE_CONNS", 5),
		MaxConnections: integer("WS_MAX_CONNECTIONS_PER_STREAMER", 100),
		HeartbeatInterval: duration("WS_HEARTBEAT_INTERVAL_SEC", 10*time.Second),
		HeartbeatTimeout:  duration("WS_HEARTBEAT_TIMEOUT_SEC", 30*time.Second),
		ReplayWindow:      duration("WS_REPLAY_WINDOW_SEC", 10*time.Minute),
		AuthCacheTTL:      duration("WS_AUTH_CACHE_TTL_SEC", time.Minute),
		ShutdownTimeout:   duration("SHUTDOWN_TIMEOUT_SEC", 15*time.Second),
		// D-023: production default is "https://app.inflora.app"; override
		// with WS_ALLOWED_ORIGINS in dev/staging. WS_DEV_OPEN_ORIGIN=true
		// reverts to permissive (D-023 explicit escape hatch).
		AllowedOrigins: envCSV("WS_ALLOWED_ORIGINS", "https://app.inflora.app"),
		DevOpenOrigin:  envBool("WS_DEV_OPEN_ORIGIN", false),
		// Optional: serve the built overlay (overlay/dist/) at /overlay/.
		OverlayDir: env("WS_OVERLAY_DIR", ""),
	}
	if c.DBDSN == "" {
		return c, fmt.Errorf("DB_DSN is required")
	}
	if c.InternalKey == "" {
		return c, fmt.Errorf("INTERNAL_API_KEY is required")
	}
	return c, nil
}
