package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/sqlite"
)

type email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := sqlite.Open("example.db")
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	client := runnel.NewClient(store, "mailer")
	if err := client.SetQueue(ctx, runnel.QueueConfig{Name: "emails", Concurrency: 5, TenantSlots: 1, Rate: 20}); err != nil {
		return err
	}
	if err := client.SetPool(ctx, runnel.PoolConfig{Name: "smtp-primary", Slots: 3}); err != nil {
		return err
	}

	worker := runnel.NewWorker(store, "mailer", runnel.WorkerConfig{Queues: []string{"emails", "emails-backup"}, Concurrency: 8})
	worker.HandleFunc("email.send", func(ctx context.Context, j *runnel.Job) error {
		var m email
		if err := j.Bind(&m); err != nil {
			return runnel.Fail(err)
		}
		if m.To == "" {
			return runnel.Fail(errors.New("no recipient"))
		}
		fmt.Printf("sending %q to %s via %s (attempt %d)\n", m.Subject, m.To, j.Pool, j.Attempt)
		return j.SetResult(map[string]string{"message_id": runnel.NewID()})
	})
	go func() {
		if err := worker.Run(ctx); err != nil {
			log.Print(err)
		}
	}()
	go func() {
		_ = runnel.NewMaintainer(store, runnel.MaintainerConfig{Namespaces: []string{"mailer"}}).Run(ctx)
	}()

	res, err := client.Enqueue(ctx, "emails", "email.send", email{To: "ada@example.com", Subject: "Welcome"},
		runnel.Tenant("acme"), runnel.Pool("smtp-primary"), runnel.MaxAttempts(3),
		runnel.Unique("welcome:ada@example.com", 24*time.Hour),
		runnel.StartWithin(30*time.Second), runnel.Fallbacks(runnel.Target{Queue: "emails-backup", Pool: "smtp-backup"}))
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	j, err := client.Wait(wctx, res.ID)
	if err != nil {
		return err
	}
	fmt.Println("job", j.ID, j.State, string(j.Result))
	return nil
}
