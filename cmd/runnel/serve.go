package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/config"
	"github.com/kinoforge/runnel/server"
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
		noMaintain:   fs.Bool("no-maintain", false, "do not run lease recovery, start waits, cron, webhooks and pruning here"),
		archive:      fs.Bool("archive", false, "copy pruned jobs to the archive"),
		noHooks:      fs.Bool("no-webhooks", false, "do not deliver webhooks"),
		privateHooks: fs.Bool("allow-private-webhooks", false, "allow webhooks to private and loopback addresses"),
		httpHooks:    fs.Bool("allow-http-webhooks", false, "allow plain http webhook urls"),
		jsonLogs:     fs.Bool("json-logs", false, "log as JSON"),
	}
}

// apply overrides cfg with the flags given on the command line only.
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
		case "no-webhooks":
			cfg.Webhooks.Enabled = !*f.noHooks
		case "allow-private-webhooks":
			cfg.Webhooks.AllowPrivate = *f.privateHooks
		case "allow-http-webhooks":
			cfg.Server.AllowHTTPWebhooks = *f.httpHooks
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
	hs := httpServer(cfg.Server, server.New(s, serverConfig(cfg, log)))
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("runnel listening", "addr", cfg.Server.Addr, "admin", cfg.Server.AdminToken != "")
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	return hs.Shutdown(sctx)
}

func serverConfig(cfg config.Config, log *slog.Logger) server.Config {
	return server.Config{
		AdminToken:        cfg.Server.AdminToken,
		MaxBody:           cfg.Server.MaxBody,
		Limits:            runnel.Limits(cfg.Limits),
		AllowHTTPWebhooks: cfg.Server.AllowHTTPWebhooks,
		MaxClaimWait:      cfg.Server.MaxClaimWait,
		MaxResultWait:     cfg.Server.MaxResultWait,
		RetryAfter:        cfg.Server.RetryAfter,
		Logger:            log,
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
		Webhooks: runnel.WebhookConfig{
			Enabled:      cfg.Webhooks.Enabled,
			Concurrency:  cfg.Webhooks.Concurrency,
			Timeout:      cfg.Webhooks.Timeout,
			AllowPrivate: cfg.Webhooks.AllowPrivate,
		},
	})
	if err := m.Run(ctx); err != nil {
		log.Error("maintainer stopped", "err", err)
	}
}
