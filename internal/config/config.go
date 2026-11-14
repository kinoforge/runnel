// Package config loads the settings of the runnel binary: built-in defaults,
// then an optional YAML file, then RUNNEL_* environment variables.
//
// Every key maps to an environment variable named after its YAML path, such as
// server.max_body to RUNNEL_SERVER_MAX_BODY. Fields with an env tag use that
// name instead. Durations and numbers left at zero use the library defaults.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/kinoforge/runnel"
)

const minAdminToken = 24

type Config struct {
	Store      string     `yaml:"store"`
	Log        Log        `yaml:"log"`
	Server     Server     `yaml:"server"`
	Maintainer Maintainer `yaml:"maintainer"`
	Callbacks  Callbacks  `yaml:"callbacks"`
	Limits     Limits     `yaml:"limits"`
	Memory     Memory     `yaml:"memory"`
	Hybrid     Hybrid     `yaml:"hybrid"`
	Tracing    Tracing    `yaml:"tracing"`
}

type Tracing struct {
	Enabled      bool          `yaml:"enabled"`
	FlushTimeout time.Duration `yaml:"flush_timeout"`
}

type Log struct {
	Format string `yaml:"format"`
	Level  string `yaml:"level"`
}

type Server struct {
	Addr               string        `yaml:"addr" env:"RUNNEL_ADDR"`
	AdminToken         string        `yaml:"admin_token" env:"RUNNEL_ADMIN_TOKEN"`
	MaxBody            int64         `yaml:"max_body"`
	MaxClaimWait       time.Duration `yaml:"max_claim_wait"`
	MaxResultWait      time.Duration `yaml:"max_result_wait"`
	RetryAfter         time.Duration `yaml:"retry_after"`
	AllowHTTPCallbacks bool          `yaml:"allow_http_callbacks"`
	ReadHeaderTimeout  time.Duration `yaml:"read_header_timeout"`
	ReadTimeout        time.Duration `yaml:"read_timeout"`
	WriteTimeout       time.Duration `yaml:"write_timeout"`
	IdleTimeout        time.Duration `yaml:"idle_timeout"`
	MaxHeaderBytes     int           `yaml:"max_header_bytes"`
	ShutdownTimeout    time.Duration `yaml:"shutdown_timeout"`
	DrainDelay         time.Duration `yaml:"drain_delay"`
	MaxBatch           int           `yaml:"max_batch"`
	MaxDelay           time.Duration `yaml:"max_delay"`
	MaxClaimQueues     int           `yaml:"max_claim_queues"`
	MaxClaim           int           `yaml:"max_claim"`
	DefaultLease       time.Duration `yaml:"default_lease"`
	MinLease           time.Duration `yaml:"min_lease"`
	MaxLease           time.Duration `yaml:"max_lease"`
	MaxExtend          int           `yaml:"max_extend"`
	KeyCacheTTL        time.Duration `yaml:"key_cache_ttl"`
	HealthTimeout      time.Duration `yaml:"health_timeout"`
}

type Maintainer struct {
	Enabled    bool          `yaml:"enabled"`
	Interval   time.Duration `yaml:"interval"`
	PruneEvery time.Duration `yaml:"prune_every"`
	Batch      int           `yaml:"batch"`
	Archive    bool          `yaml:"archive"`
	Retention  Retention     `yaml:"retention"`
}

// Retention mirrors runnel.Retention so it converts directly.
type Retention struct {
	Completed   time.Duration `yaml:"completed"`
	Cancelled   time.Duration `yaml:"cancelled"`
	Dead        time.Duration `yaml:"dead"`
	Events      time.Duration `yaml:"events"`
	Undelivered time.Duration `yaml:"undelivered"`
}

type Callbacks struct {
	Enabled       bool          `yaml:"enabled"`
	Concurrency   int           `yaml:"concurrency"`
	Timeout       time.Duration `yaml:"timeout"`
	AllowPrivate  bool          `yaml:"allow_private"`
	MaxAttempts   int           `yaml:"max_attempts"`
	BackoffBase   time.Duration `yaml:"backoff_base"`
	BackoffMax    time.Duration `yaml:"backoff_max"`
	MaxRetryAfter time.Duration `yaml:"max_retry_after"`
	UniqueFor     time.Duration `yaml:"unique_for"`
	SecretTTL     time.Duration `yaml:"secret_ttl"`
}

// Limits mirrors runnel.Limits so it converts directly.
type Limits struct {
	MaxPayload     int `yaml:"max_payload"`
	MaxDependsOn   int `yaml:"max_depends_on"`
	MaxMeta        int `yaml:"max_meta"`
	MaxMetaKey     int `yaml:"max_meta_key"`
	MaxMetaValue   int `yaml:"max_meta_value"`
	MaxUniqueKey   int `yaml:"max_unique_key"`
	MaxCallbackURL int `yaml:"max_callback_url"`
}

func Default() Config {
	return Config{
		Store: "sqlite:runnel.db",
		Log:   Log{Format: "text", Level: "info"},
		Server: Server{
			Addr:              ":8700",
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       time.Minute,
			WriteTimeout:      2 * time.Minute,
			IdleTimeout:       2 * time.Minute,
			MaxHeaderBytes:    64 << 10,
			ShutdownTimeout:   15 * time.Second,
		},
		Maintainer: Maintainer{Enabled: true, Retention: Retention(runnel.DefaultRetention)},
		Callbacks:  Callbacks{Enabled: true},
		Limits:     Limits(runnel.DefaultLimits),
		Memory:     Memory{HeapPercent: 70},
		Hybrid:     defaultHybrid(),
		Tracing:    Tracing{FlushTimeout: 5 * time.Second},
	}
}

// Load returns the defaults overlaid with the YAML file at path, when path is
// set, and then with the environment read through getenv. It does not
// validate, so callers can apply flags first.
func Load(path string, getenv func(string) string) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return Config{}, err
		}
		dec := yaml.NewDecoder(bytes.NewReader(b))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return Config{}, fmt.Errorf("config %s: %w", path, err)
		}
	}
	if err := applyEnv(reflect.ValueOf(&cfg).Elem(), envPrefix, getenv); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.Store == "" {
		errs = append(errs, errors.New("store is required"))
	}
	if c.Server.AdminToken != "" && len(c.Server.AdminToken) < minAdminToken {
		errs = append(errs, fmt.Errorf("admin token must be at least %d characters", minAdminToken))
	}
	if c.Log.Format != "text" && c.Log.Format != "json" {
		errs = append(errs, fmt.Errorf("log.format must be text or json, not %q", c.Log.Format))
	}
	if _, err := c.Log.level(); err != nil {
		errs = append(errs, fmt.Errorf("log.level: %w", err))
	}
	if c.Memory.HeapPercent > 100 || c.Hybrid.HotPercent > 100 {
		errs = append(errs, errors.New("memory.heap_percent and hybrid.hot_percent must be at most 100"))
	}
	if _, err := c.Hybrid.Options(); err != nil {
		errs = append(errs, fmt.Errorf("hybrid.sync: %w", err))
	}
	errs = append(errs, negatives(reflect.ValueOf(c), "")...)
	return errors.Join(errs...)
}

func (l Log) level() (slog.Level, error) {
	var lvl slog.Level
	err := lvl.UnmarshalText([]byte(l.Level))
	return lvl, err
}

func (l Log) NewLogger(w io.Writer) *slog.Logger {
	lvl, _ := l.level()
	opts := &slog.HandlerOptions{Level: lvl}
	if l.Format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
