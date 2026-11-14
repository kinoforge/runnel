package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/config"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/tracing"
)

type serveFlags struct {
	addr, token                       *string
	noMaintain, archive, noHooks      *bool
	privateHooks, httpHooks, jsonLogs *bool
}

func newServeFlags(fs *flag.FlagSet) serveFlags {
	return serveFlags{
		addr:         fs.String("addr", "", "listen address (default :8700)"),
		token:        fs.String("admin-token", "", "admin token (default RUNNEL_ADMIN_TOKEN)"),
		noMaintain:   fs.Bool("no-maintain", false, "do not run lease recovery, start waits, cron, callbacks and pruning here"),
		archive:      fs.Bool("archive", false, "copy pruned jobs to the archive"),
		noHooks:      fs.Bool("no-callbacks", false, "do not deliver callbacks"),
		privateHooks: fs.Bool("allow-private-callbacks", false, "allow callbacks to private and loopback addresses"),
		httpHooks:    fs.Bool("allow-http-callbacks", false, "allow plain http callback urls"),
		jsonLogs:     fs.Bool("json-logs", false, "log as JSON"),
	}
}

func (f serveFlags) apply(fs *flag.FlagSet, cfg *config.Config) {
	fs.Visit(func(fl *flag.Flag) {
		switch fl.Name {
		case "addr":
			cfg.Server.Addr = *f.addr
		case "admin-token":
			cfg.Server.AdminToken = *f.token
		case "no-maintain":
			cfg.Maintainer.Enabled = !*f.noMaintain
		case "archive":
			cfg.Maintainer.Archive = *f.archive
		case "no-callbacks":
			cfg.Callbacks.Enabled = !*f.noHooks
		case "allow-private-callbacks":
			cfg.Callbacks.AllowPrivate = *f.privateHooks
		case "allow-http-callbacks":
			cfg.Server.AllowHTTPCallbacks = *f.httpHooks
		case "json-logs":
			if *f.jsonLogs {
				cfg.Log.Format = "json"
			}
		}
	})
}

func serve(ctx context.Context, args []string) error {
	c := newCmd("serve")
	flags := newServeFlags(c.fs)
	if _, err := c.parse(args, 0); err != nil {
		return err
	}
	cfg, err := c.settings()
	if err != nil {
		return err
	}
	flags.apply(c.fs, &cfg)
	s, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	log := cfg.Log.NewLogger(os.Stderr)
	if cfg.Maintainer.Enabled {
		go runMaintainer(ctx, s, cfg, log)
	}
	api := server.New(s, serverConfig(cfg, log))
	var handler http.Handler = api
	if cfg.Tracing.Enabled {
		tp, err := tracing.Setup(ctx, buildVersion())
		if err != nil {
			return err
		}
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), cfg.Tracing.FlushTimeout)
			defer cancel()
			if err := tp.Shutdown(sctx); err != nil {
				log.Warn("sending the last spans failed", "err", err)
			}
		}()
		handler = tracing.Handler(api, tp)
	}
	hs := httpServer(cfg.Server, handler)
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("runnel listening", "addr", cfg.Server.Addr, "admin", cfg.Server.AdminToken != "")
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	api.Drain()
	if cfg.Server.DrainDelay > 0 {
		log.Info("draining", "for", cfg.Server.DrainDelay)
		time.Sleep(cfg.Server.DrainDelay)
	}
	sctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	return hs.Shutdown(sctx)
}

func serverConfig(cfg config.Config, log *slog.Logger) server.Config {
	return server.Config{
		AdminToken:         cfg.Server.AdminToken,
		MaxBody:            cfg.Server.MaxBody,
		Limits:             runnel.Limits(cfg.Limits),
		AllowHTTPCallbacks: cfg.Server.AllowHTTPCallbacks,
		MaxClaimWait:       cfg.Server.MaxClaimWait,
		MaxResultWait:      cfg.Server.MaxResultWait,
		RetryAfter:         cfg.Server.RetryAfter,
		MaxBatch:           cfg.Server.MaxBatch,
		MaxDelay:           cfg.Server.MaxDelay,
		MaxClaimQueues:     cfg.Server.MaxClaimQueues,
		MaxClaim:           cfg.Server.MaxClaim,
		DefaultLease:       cfg.Server.DefaultLease,
		MinLease:           cfg.Server.MinLease,
		MaxLease:           cfg.Server.MaxLease,
		MaxExtend:          cfg.Server.MaxExtend,
		KeyCacheTTL:        cfg.Server.KeyCacheTTL,
		HealthTimeout:      cfg.Server.HealthTimeout,
		Logger:             log,
	}
}

func httpServer(cfg config.Server, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
}

func runMaintainer(ctx context.Context, s runnel.Store, cfg config.Config, log *slog.Logger) {
	m := runnel.NewMaintainer(s, runnel.MaintainerConfig{
		Interval:   cfg.Maintainer.Interval,
		PruneEvery: cfg.Maintainer.PruneEvery,
		Retention:  runnel.Retention(cfg.Maintainer.Retention),
		Archive:    cfg.Maintainer.Archive,
		Batch:      cfg.Maintainer.Batch,
		Logger:     log,
		Callbacks: runnel.CallbackConfig{
			Enabled:       cfg.Callbacks.Enabled,
			Concurrency:   cfg.Callbacks.Concurrency,
			Timeout:       cfg.Callbacks.Timeout,
			AllowPrivate:  cfg.Callbacks.AllowPrivate,
			MaxAttempts:   cfg.Callbacks.MaxAttempts,
			Backoff:       runnel.Backoff{Base: runnel.Duration(cfg.Callbacks.BackoffBase), Max: runnel.Duration(cfg.Callbacks.BackoffMax)},
			MaxRetryAfter: cfg.Callbacks.MaxRetryAfter,
			UniqueFor:     cfg.Callbacks.UniqueFor,
			SecretTTL:     cfg.Callbacks.SecretTTL,
		},
	})
	if err := m.Run(ctx); err != nil {
		log.Error("maintainer stopped", "err", err)
	}
}
