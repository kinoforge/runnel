package redisstore

import (
	"context"
	"sync"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/redis/go-redis/v9"
)

const (
	endedBuffer = 256
)

const defaultPrefix = "rn"

// Store keeps jobs in Redis, Valkey or Dragonfly. Every operation is one Lua
// script, so it is atomic; a namespace's keys share a hash tag, so a namespace
// lives on one cluster slot.
type Store struct {
	rdb    redis.UniversalClient
	prefix string

	scripts

	clockOnce sync.Once
	clock     runnel.Clock
}

// Open connects to the server a redis:// or rediss:// URL names.
func Open(rawURL string) (*Store, error) {
	opt, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	return New(redis.NewClient(opt), defaultPrefix), nil
}

// New returns a store over rdb with keys under prefix (default "rn").
func New(rdb redis.UniversalClient, prefix string) *Store {
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Store{rdb: rdb, prefix: prefix, scripts: newScripts()}
}

// Client returns the Redis client.
func (s *Store) Client() redis.UniversalClient { return s.rdb }

func (s *Store) p(ns string) string {
	if ns == "" {
		ns = "_"
	}
	return s.prefix + ":{" + ns + "}:"
}

func (s *Store) Migrate(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// Time returns the server's clock.
// Clock returns the store's shared clock, which follows Time.
func (s *Store) Clock() runnel.Clock {
	s.clockOnce.Do(func() { s.clock = runnel.NewStoreClock(s) })
	return s.clock
}

func (s *Store) Time(ctx context.Context) (time.Time, error) {
	t, err := s.rdb.Time(ctx).Result()
	return t.UTC(), err
}

func (s *Store) Close() error { return s.rdb.Close() }

// Subscribe signals when jobs in ns may be ready to claim.
func (s *Store) Subscribe(ctx context.Context, ns string) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	return ch, s.listen(ctx, s.p(ns)+"wake", func(string) {
		select {
		case ch <- struct{}{}:
		default:
		}
	})
}

// Ended sends the id of each job in ns that reaches a final state.
func (s *Store) Ended(ctx context.Context, ns string) (<-chan string, error) {
	ch := make(chan string, endedBuffer)
	return ch, s.listen(ctx, s.p(ns)+"end", func(id string) {
		select {
		case ch <- id:
		default:
		}
	})
}

func (s *Store) listen(ctx context.Context, channel string, fn func(string)) error {
	sub := s.rdb.Subscribe(ctx, channel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}
	go func() {
		defer sub.Close()
		msgs := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-msgs:
				if !ok {
					return
				}
				fn(m.Payload)
			}
		}
	}()
	return nil
}

var (
	_ runnel.Store      = (*Store)(nil)
	_ runnel.Waker      = (*Store)(nil)
	_ runnel.EndWatcher = (*Store)(nil)
	_ runnel.TimeSource = (*Store)(nil)
)
