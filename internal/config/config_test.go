package config

import "testing"

func TestLoadRequiresSecretsAndAppliesDefaults(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://example")
	t.Setenv("INTERNAL_API_KEY", "secret")
	t.Setenv("WS_ADDR", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8083" || cfg.MaxConnections != 100 || cfg.AuthCacheTTL.Seconds() != 60 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}
