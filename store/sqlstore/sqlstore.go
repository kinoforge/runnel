package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	configTTL    = time.Second
	retryBackoff = 5 * time.Millisecond
)

// Store keeps jobs in a SQL database, in the rn_ tables its migrations create.
// Claims of queues without limits take rows with SKIP LOCKED; claims under
// limits serialize per namespace so counts stay exact.
type Store struct {
	db *sql.DB
	d  Dialect

	mu      sync.Mutex
	cfgs    map[string]*nsConfig
	wakes   map[string]bool
	ends    map[string][]string
	kick    chan struct{}
	stopped chan struct{}
	closed  bool

	clockOnce sync.Once
	clock     runnel.Clock
}

type nsConfig struct {
	queues map[string]runnel.QueueConfig
	pools  map[string]runnel.PoolConfig
	at     time.Time
}

func (c *nsConfig) poolLimits() bool {
	for _, p := range c.pools {
		if p.Limited() {
			return true
		}
	}
	return false
}

func limited(q runnel.QueueConfig) bool {
	return q.MaxRunning > 0 || q.GroupMaxRunning > 0 || q.Rate > 0 || q.GroupRate > 0
}

func New(db *sql.DB, d Dialect) *Store {
	return &Store{db: db, d: d, cfgs: map[string]*nsConfig{}, wakes: map[string]bool{}, ends: map[string][]string{}}
}

func (s *Store) loadConfigs(ctx context.Context, q querier, ns string) (*nsConfig, error) {
	queues, err := s.queueConfigs(ctx, q, ns)
	if err != nil {
		return nil, err
	}
	pools, err := s.poolConfigs(ctx, q, ns)
	if err != nil {
		return nil, err
	}
	return &nsConfig{queues: queues, pools: pools, at: time.Now()}, nil
}

func (s *Store) configs(ctx context.Context, q querier, ns string) (*nsConfig, error) {
	s.mu.Lock()
	c := s.cfgs[ns]
	s.mu.Unlock()
	if c != nil && time.Since(c.at) < configTTL {
		return c, nil
	}
	c, err := s.loadConfigs(ctx, q, ns)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cfgs[ns] = c
	s.mu.Unlock()
	return c, nil
}

func (s *Store) forget(ns string) {
	s.mu.Lock()
	delete(s.cfgs, ns)
	s.mu.Unlock()
}

// DB returns the connection pool, for tuning or for transactions to pass to
// InsertTx.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Dialect() Dialect { return s.d }

func (s *Store) q(query string) string { return s.d.rebind(query) }

// Time returns the database's clock, so every process measures leases the same
// way.
// Clock returns the store's shared clock, which follows Time.
func (s *Store) Clock() runnel.Clock {
	s.clockOnce.Do(func() { s.clock = runnel.NewStoreClock(s) })
	return s.clock
}

func (s *Store) Time(ctx context.Context) (time.Time, error) {
	if s.d.NowQuery == "" {
		return time.Now().UTC(), nil
	}
	var ms int64
	if err := s.db.QueryRowContext(ctx, s.d.NowQuery).Scan(&ms); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) Migrate(ctx context.Context) error {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	unlock, err := s.d.MigrateLock(ctx, c)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := c.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS rn_migrations (version INT NOT NULL PRIMARY KEY)"+s.d.TableSuffix); err != nil {
		return err
	}
	var current sql.NullInt64
	if err := c.QueryRowContext(ctx, "SELECT MAX(version) FROM rn_migrations").Scan(&current); err != nil {
		return err
	}
	for i, stmts := range s.d.migrations() {
		v := int64(i + 1)
		if v <= current.Int64 {
			continue
		}
		for _, st := range stmts {
			if _, err := c.ExecContext(ctx, st); err != nil {
				return fmt.Errorf("runnel migration %d: %w", v, err)
			}
		}
		if _, err := c.ExecContext(ctx, s.q("INSERT INTO rn_migrations (version) VALUES (?)"), v); err != nil {
			return err
		}
	}
	return nil
}

type txn struct {
	*sql.Tx
	wake bool
	ends []string
}

func (s *Store) tx(ctx context.Context, ns string, lock bool, fn func(tx *txn) error) error {
	var err error
	for try := range 4 {
		if try > 0 {
			t := time.NewTimer(time.Duration(try*try) * retryBackoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return err
			case <-t.C:
			}
		}
		var t *txn
		if t, err = s.attempt(ctx, ns, lock, fn); err == nil {
			s.notify(ns, t.wake, t.ends)
			return nil
		}
		if s.d.Retryable == nil || !s.d.Retryable(err) {
			return err
		}
	}
	return err
}

func (s *Store) attempt(ctx context.Context, ns string, lock bool, fn func(tx *txn) error) (*txn, error) {
	raw, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: s.d.Isolation})
	if err != nil {
		return nil, err
	}
	tx := &txn{Tx: raw}
	if lock {
		if err := s.lock(ctx, tx, ns); err != nil {
			_ = raw.Rollback()
			return nil, err
		}
	}
	if err := fn(tx); err != nil {
		_ = raw.Rollback()
		return nil, err
	}
	return tx, raw.Commit()
}

func (s *Store) lock(ctx context.Context, tx *txn, ns string) error {
	if !s.d.LockRows {
		return nil
	}
	sel := s.q("SELECT ns FROM rn_locks WHERE ns=? FOR UPDATE")
	var got string
	err := tx.QueryRowContext(ctx, sel, ns).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, s.q(s.d.Ignore("rn_locks", []string{"ns"})), ns); err != nil {
			return err
		}
		err = tx.QueryRowContext(ctx, sel, ns).Scan(&got)
	}
	return err
}

var (
	_ runnel.Store      = (*Store)(nil)
	_ runnel.TimeSource = (*Store)(nil)
	_ runnel.TxInserter = (*Store)(nil)
)
