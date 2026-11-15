package server_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
	"github.com/kinoforge/runnel/server"
)

type skewClock struct{ by time.Duration }

func (c skewClock) Now() time.Time { return time.Now().Add(c.by).UTC() }

type skewedStore struct {
	runnel.Store
	by time.Duration
}

func (s skewedStore) Clock() runnel.Clock { return skewClock{by: s.by} }

func TestRemoteDeadlineOnServerClock(t *testing.T) {
	spec.Rule(t, "K01", "T06")
	for _, by := range []time.Duration{-2 * time.Second, 2 * time.Second} {
		t.Run(by.String(), func(t *testing.T) {
			_, store := setup(t)
			srv := httptest.NewServer(server.New(skewedStore{Store: store, by: by}, server.Config{AdminToken: admin}))
			defer srv.Close()
			call(t, srv, admin, "PUT", "/v1/namespaces/clock", nil)
			c := remote.New(srv.URL, key(t, srv, "clock", "producer", "reader", "worker"), "clock")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			type end struct {
				at    time.Time
				cause error
			}
			ended := make(chan end, 1)
			w := remote.NewWorker(c, runnel.WorkerConfig{Queues: []string{"q"}, Concurrency: 1, Lease: 5 * time.Second, ShutdownTimeout: time.Second})
			w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
				<-ctx.Done()
				ended <- end{at: skewClock{by: by}.Now(), cause: context.Cause(ctx)}
				return ctx.Err()
			})
			wctx, stop := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = w.Run(wctx)
			}()
			defer func() {
				stop()
				<-done
			}()
			res, err := c.Enqueue(ctx, "q", "k", nil, runnel.TTL(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			j, err := c.Get(ctx, res.ID)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-ended:
				if !errors.Is(got.cause, runnel.ErrDeadline) || got.at.Before(j.Deadline) || got.at.After(j.Deadline.Add(time.Second)) {
					t.Errorf("handler stopped by %v at %v on the server's clock, deadline %v", got.cause, got.at, j.Deadline)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("handler never stopped")
			}
			for ctx.Err() == nil {
				if j, err = c.Get(ctx, res.ID); err == nil && j.State.Final() {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if j.State != runnel.StateExpired {
				t.Errorf("job past its deadline ended %s, want expired", j.State)
			}
		})
	}
}
