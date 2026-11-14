package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
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
	if cfg.Store != "sqlite:runnel.db" || cfg.Server.Addr != ":8700" || !cfg.Maintainer.Enabled || !cfg.Callbacks.Enabled {
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
	if cfg.Server.Addr != ":8700" || cfg.Maintainer.Retention.Dead != runnel.DefaultRetention.Dead || cfg.Limits.MaxMeta != runnel.DefaultLimits.MaxMeta {
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
		"RUNNEL_CALLBACKS_ENABLED":         "false",
		"RUNNEL_MAINTAINER_RETENTION_DEAD": "1h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store != "redis://localhost:6379/0" || cfg.Server.Addr != ":9100" || cfg.Server.MaxBody != 1024 {
		t.Fatalf("env not applied: %+v", cfg.Server)
	}
	if cfg.Callbacks.Enabled || cfg.Maintainer.Retention.Dead != time.Hour {
		t.Fatal("nested env not applied")
	}
}

func TestRejects(t *testing.T) {
	cases := map[string]struct {
		yaml string
		env  map[string]string
		want string
	}{
		"unknown key":      {yaml: "sever:\n  addr: x\n", want: "sever"},
		"bad env number":   {env: map[string]string{"RUNNEL_SERVER_MAX_BODY": "big"}, want: "RUNNEL_SERVER_MAX_BODY"},
		"bad env bool":     {env: map[string]string{"RUNNEL_CALLBACKS_ENABLED": "maybe"}, want: "RUNNEL_CALLBACKS_ENABLED"},
		"short token":      {env: map[string]string{"RUNNEL_ADMIN_TOKEN": "short"}, want: "admin token"},
		"bad log format":   {yaml: "log:\n  format: xml\n", want: "log.format"},
		"bad log level":    {yaml: "log:\n  level: loud\n", want: "log.level"},
		"negative value":   {yaml: "callbacks:\n  concurrency: -1\n", want: "callbacks.concurrency"},
		"empty store":      {yaml: "store: \"\"\n", want: "store is required"},
		"bad auth":         {yaml: "server:\n  auth: maybe\n", want: "server.auth"},
		"auth none, token": {env: map[string]string{"RUNNEL_SERVER_AUTH": "none", "RUNNEL_ADMIN_TOKEN": "0123456789abcdef01234567"}, want: "auth none"},
		"bad admin user":   {yaml: "server:\n  admin_user: \"a:b\"\n", want: "admin_user"},
		"cert, no key":     {yaml: "server:\n  tls:\n    cert: a.pem\n", want: "go together"},
		"ca, no cert":      {yaml: "server:\n  tls:\n    client_ca: ca.pem\n", want: "client_ca needs"},
		"names, no ca":     {env: map[string]string{"RUNNEL_SERVER_TLS_CLIENT_NAMES": "dash"}, want: "need server.tls.client_ca"},
		"old tls":          {yaml: "server:\n  tls:\n    min_version: \"1.1\"\n", want: "min_version"},
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
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatal("an empty file must leave the defaults")
	}
}

func TestHybridSettings(t *testing.T) {
	path := writeYAML(t, "memory:\n  heap_percent: 60\nhybrid:\n  sync: periodic\n  hot_jobs: 5000\n  refill_batch: 100\n  cold_cache_bytes: 1024\n")
	cfg, err := Load(path, env(map[string]string{"RUNNEL_HYBRID_SPILL_BATCH": "77"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	opt, err := cfg.Hybrid.Options()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Memory.HeapPercent != 60 || opt.HotJobs != 5000 || opt.RefillBatch != 100 || opt.SpillBatch != 77 ||
		opt.Cold.CacheBytes != 1024 || opt.WakeBatch != hybrid.DefaultOptions.WakeBatch {
		t.Fatalf("hybrid settings %+v", opt)
	}
	cfg.Hybrid.Sync = "sometimes"
	if cfg.Validate() == nil {
		t.Fatal("bad sync mode accepted")
	}
}

func TestExampleFile(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "deploy", "runnel.example.yaml"), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	want, _ := Default().Hybrid.Options()
	if got, _ := cfg.Hybrid.Options(); got != want {
		t.Fatalf("example hybrid settings differ from the defaults:\n got %+v\nwant %+v", got, want)
	}
}
