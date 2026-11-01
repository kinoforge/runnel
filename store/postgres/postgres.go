package postgres

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/sqlstore"
)

const (
	maxOpenConns    = 20
	maxIdleConns    = 10
	connMaxIdleTime = 5 * time.Minute
	endedBuffer     = 256
	listenRetryMin  = time.Second
	listenRetryMax  = 30 * time.Second
)

// Store is a Postgres store. Besides the runnel.Store methods it wakes workers
// and waiters through LISTEN/NOTIFY on one shared connection.
type Store struct {
	*sqlstore.Store
	dsn string

	mu     sync.Mutex
	wakes  map[string][]chan struct{}
	ends   map[string][]chan string
	listen bool
	stop   context.CancelFunc
}

// Open connects to Postgres. The DSN is a pgx connection string or URL.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxIdleTime(connMaxIdleTime)
	return &Store{Store: sqlstore.New(db, sqlstore.Postgres()), dsn: dsn,
		wakes: map[string][]chan struct{}{}, ends: map[string][]chan string{}}, nil
}

// Subscribe signals when jobs in ns may be ready to claim.
func (s *Store) Subscribe(ctx context.Context, ns string) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	s.register(ctx, func() { s.wakes[ns] = append(s.wakes[ns], ch) }, func() { s.wakes[ns] = without(s.wakes[ns], ch) })
	return ch, nil
}

// Ended sends the id of each job in ns that reaches a final state.
func (s *Store) Ended(ctx context.Context, ns string) (<-chan string, error) {
	ch := make(chan string, endedBuffer)
	s.register(ctx, func() { s.ends[ns] = append(s.ends[ns], ch) }, func() { s.ends[ns] = without(s.ends[ns], ch) })
	return ch, nil
}

func without[T comparable](list []T, v T) []T {
	for i, c := range list {
		if c == v {
			return append(list[:i:i], list[i+1:]...)
		}
	}
	return list
}

func (s *Store) register(ctx context.Context, add, remove func()) {
	s.mu.Lock()
	add()
	start := !s.listen
	s.listen = true
	if start {
		lctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		s.stop = cancel
		go s.loop(lctx)
	}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		remove()
		s.mu.Unlock()
	}()
}

// Close stops the listener and closes the pool.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.stop != nil {
		s.stop()
	}
	s.mu.Unlock()
	return s.Store.Close()
}

func (s *Store) loop(ctx context.Context) {
	pause := listenRetryMin
	for {
		err := s.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pause):
			}
			pause = min(pause*2, listenRetryMax)
			continue
		}
		pause = time.Second
	}
}

func (s *Store) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	for _, ch := range []string{"runnel_wake", "runnel_end"} {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return err
		}
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		s.dispatch(n.Channel, n.Payload)
	}
}

func (s *Store) dispatch(channel, payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch channel {
	case "runnel_wake":
		for _, ch := range s.wakes[payload] {
			notify(ch, struct{}{})
		}
	case "runnel_end":
		ns, ids, ok := strings.Cut(payload, "|")
		if !ok {
			return
		}
		for id := range strings.SplitSeq(ids, ",") {
			for _, ch := range s.ends[ns] {
				notify(ch, id)
			}
		}
	}
}

func notify[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

var (
	_ runnel.Waker      = (*Store)(nil)
	_ runnel.EndWatcher = (*Store)(nil)
)
