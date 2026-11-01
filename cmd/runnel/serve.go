package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/server"
)

func serve(ctx context.Context, args []string) error {
	c := newCmd("serve")
	addr := c.fs.String("addr", env("RUNNEL_ADDR", ":8700"), "listen address")
	token := c.fs.String("admin-token", "", "admin token (default RUNNEL_ADMIN_TOKEN)")
	noMaintain := c.fs.Bool("no-maintain", false, "do not run lease recovery, start waits, cron, webhooks and pruning here")
	archive := c.fs.Bool("archive", false, "copy pruned jobs to the archive")
	noHooks := c.fs.Bool("no-webhooks", false, "do not deliver webhooks")
	privateHooks := c.fs.Bool("allow-private-webhooks", false, "allow webhooks to private and loopback addresses")
	httpHooks := c.fs.Bool("allow-http-webhooks", false, "allow plain http webhook urls")
	jsonLogs := c.fs.Bool("json-logs", false, "log as JSON")
	if _, err := c.parse(args, 0); err != nil {
		return err
	}
	if *token == "" {
		*token = os.Getenv("RUNNEL_ADMIN_TOKEN")
	}
	if *token != "" && len(*token) < 24 {
		return errors.New("admin token must be at least 24 characters")
	}
	var log *slog.Logger
	if *jsonLogs {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	} else {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	srv := server.New(s, server.Config{AdminToken: *token, AllowHTTPWebhooks: *httpHooks, Logger: log})
	if !*noMaintain {
		m := runnel.NewMaintainer(s, runnel.MaintainerConfig{Archive: *archive, Logger: log,
			Webhooks: runnel.WebhookConfig{Enabled: !*noHooks, AllowPrivate: *privateHooks}})
		go func() {
			if err := m.Run(ctx); err != nil {
				log.Error("maintainer stopped", "err", err)
			}
		}()
	}
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: time.Minute,
		WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10}
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("runnel listening", "addr", *addr, "admin", *token != "")
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}
