package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runnel.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaults(t *testing.T) {
	cfg, err := Load("", env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Store != "sqlite:runnel.db" || cfg.Server.Addr != ":8700" || !cfg.Maintainer.Enabled || !cfg.Webhooks.Enabled {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if runnel.Limits(cfg.Limits) != runnel.DefaultLimits || runnel.Retention(cfg.Maintainer.Retention) != runnel.DefaultRetention {
		t.Fatal("limits and retention must start from the library defaults")
	}
}

func TestYAMLOverlaysDefaults(t *testing.T) {
	path := writeYAML(t, `
store: postgres://localhost/app
server:
  max_claim_wait: 10s
maintainer:
  retention:
    completed: 2h
limits:
  max_payload: 2048
`)
	cfg, err := Load(path, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store != "postgres://localhost/app" || cfg.Server.MaxClaimWait != 10*time.Second {
		t.Fatalf("yaml not applied: %+v", cfg)
	}
	if cfg.Server.Addr != ":8700" || cfg.Maintainer.Retention.Dead != runnel.DefaultRetention.Dead || cfg.Limits.MaxFallbacks != runnel.DefaultLimits.MaxFallbacks {
		t.Fatal("keys absent from yaml must keep their defaults")
	}
	if cfg.Maintainer.Retention.Completed != 2*time.Hour || cfg.Limits.MaxPayload != 2048 {
		t.Fatal("nested yaml keys not applied")
	}
}

func TestEnvOverridesYAML(t *testing.T) {
	path := writeYAML(t, "store: sqlite:file.db\nserver:\n  addr: \":9000\"\n")
	cfg, err := Load(path, env(map[string]string{
		"RUNNEL_STORE":                     "redis://localhost:6379/0",
		"RUNNEL_ADDR":                      ":9100",
		"RUNNEL_SERVER_MAX_BODY":           "1024",
		"RUNNEL_WEBHOOKS_ENABLED":          "false",
		"RUNNEL_MAINTAINER_RETENTION_DEAD": "1h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store != "redis://localhost:6379/0" || cfg.Server.Addr != ":9100" || cfg.Server.MaxBody != 1024 {
		t.Fatalf("env not applied: %+v", cfg.Server)
	}
	if cfg.Webhooks.Enabled || cfg.Maintainer.Retention.Dead != time.Hour {
		t.Fatal("nested env not applied")
	}
}

func TestRejects(t *testing.T) {
	cases := map[string]struct {
		yaml string
		env  map[string]string
		want string
	}{
		"unknown key":    {yaml: "sever:\n  addr: x\n", want: "sever"},
		"bad env number": {env: map[string]string{"RUNNEL_SERVER_MAX_BODY": "big"}, want: "RUNNEL_SERVER_MAX_BODY"},
		"bad env bool":   {env: map[string]string{"RUNNEL_WEBHOOKS_ENABLED": "maybe"}, want: "RUNNEL_WEBHOOKS_ENABLED"},
		"short token":    {env: map[string]string{"RUNNEL_ADMIN_TOKEN": "short"}, want: "admin token"},
		"bad log format": {yaml: "log:\n  format: xml\n", want: "log.format"},
		"bad log level":  {yaml: "log:\n  level: loud\n", want: "log.level"},
		"negative value": {yaml: "webhooks:\n  concurrency: -1\n", want: "webhooks.concurrency"},
		"empty store":    {yaml: "store: \"\"\n", want: "store is required"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			path := ""
			if tc.yaml != "" {
				path = writeYAML(t, tc.yaml)
			}
			cfg, err := Load(path, env(tc.env))
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestEmptyFile(t *testing.T) {
	cfg, err := Load(writeYAML(t, ""), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg != Default() {
		t.Fatal("an empty file must leave the defaults")
	}
}
