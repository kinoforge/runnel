package stores

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/wal"
	"github.com/kinoforge/runnel/store/mysql"
	"github.com/kinoforge/runnel/store/postgres"
	"github.com/kinoforge/runnel/store/redisstore"
	"github.com/kinoforge/runnel/store/sqlite"
)

// Open opens and migrates the store a URL names: postgres://, mysql://,
// sqlite:path, redis://, rediss://, valkey:// and dragonfly://, hybrid:dir with an
// optional ?sync=each|periodic|off, or memory: for a store that keeps nothing.
func Open(ctx context.Context, dsn string) (runnel.Store, error) {
	scheme, rest, ok := strings.Cut(dsn, ":")
	if !ok {
		return nil, fmt.Errorf("runnel: store %q needs a scheme like postgres://, mysql://, sqlite:, redis://", redact(dsn))
	}
	var s runnel.Store
	var err error
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		s, err = postgres.Open(dsn)
	case "mysql":
		s, err = openMySQL(dsn)
	case "sqlite", "sqlite3", "file":
		s, err = sqlite.Open(strings.TrimPrefix(rest, "//"))
	case "redis", "rediss", "valkey", "valkeys", "dragonfly":
		u := dsn
		switch strings.ToLower(scheme) {
		case "valkey", "dragonfly":
			u = "redis:" + rest
		case "valkeys":
			u = "rediss:" + rest
		}
		s, err = redisstore.Open(u)
	case "hybrid":
		s, err = openHybrid(strings.TrimPrefix(rest, "//"))
	case "memory", "mem":
		s = hybrid.Memory()
	default:
		return nil, fmt.Errorf("runnel: unknown store scheme %q", scheme)
	}
	if err != nil {
		return nil, err
	}
	if err := s.Migrate(ctx); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("runnel: preparing store: %w", err)
	}
	return s, nil
}

func openMySQL(dsn string) (runnel.Store, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, errors.New("runnel: bad mysql url")
	}
	pass, _ := u.User.Password()
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":3306"
	}
	q := u.Query()
	q.Set("parseTime", "false")
	return mysql.Open(fmt.Sprintf("%s:%s@tcp(%s)%s?%s", u.User.Username(), pass, host, u.Path, q.Encode()))
}

var syncModes = map[string]wal.SyncMode{"": wal.SyncEach, "each": wal.SyncEach, "periodic": wal.SyncPeriodic, "off": wal.SyncOff}

func openHybrid(rest string) (runnel.Store, error) {
	dir, query, _ := strings.Cut(rest, "?")
	if dir == "" {
		return nil, errors.New("runnel: hybrid store needs a directory, like hybrid:/var/lib/runnel")
	}
	q, err := url.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("runnel: bad hybrid options: %w", err)
	}
	mode, ok := syncModes[q.Get("sync")]
	if !ok {
		return nil, fmt.Errorf("runnel: hybrid sync %q must be each, periodic or off", q.Get("sync"))
	}
	return hybrid.Open(hybrid.Options{Dir: dir, Sync: mode})
}

func redact(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		u.User = url.User(u.User.Username())
		return u.String()
	}
	if len(dsn) > 12 {
		return dsn[:12] + "..."
	}
	return dsn
}
