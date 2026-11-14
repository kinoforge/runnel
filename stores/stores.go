package stores

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/wal"
	"github.com/kinoforge/runnel/store/mysql"
	"github.com/kinoforge/runnel/store/postgres"
	"github.com/kinoforge/runnel/store/redisstore"
	"github.com/kinoforge/runnel/store/sqlite"
	"github.com/kinoforge/runnel/store/sqlstore"
)

// Open opens and migrates the store a URL names: postgres://, mysql://,
// sqlite:path, redis://, rediss://, valkey:// and dragonfly://, hybrid:dir with an
// optional ?sync=each|periodic|off, or memory: for a store that keeps nothing.
func Open(ctx context.Context, dsn string) (runnel.Store, error) {
	return OpenWith(ctx, dsn, hybrid.Options{})
}

// OpenWith is Open with the options a hybrid store takes; the URL's
// directory and sync mode win over them.
func OpenWith(ctx context.Context, dsn string, hy hybrid.Options) (runnel.Store, error) {
	scheme, rest, ok := strings.Cut(dsn, ":")
	if !ok {
		return nil, fmt.Errorf("runnel: store %q needs a scheme like postgres://, mysql://, sqlite:, redis://", redact(dsn))
	}
	var s runnel.Store
	var err error
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		var pool sqlstore.Pool
		if dsn, pool, err = takePool(dsn); err == nil {
			s, err = postgres.OpenWith(dsn, pool)
		}
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
		s, err = openHybrid(strings.TrimPrefix(rest, "//"), hy)
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
	dsn, pool, err := takePool(dsn)
	if err != nil {
		return nil, err
	}
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
	return mysql.OpenWith(fmt.Sprintf("%s:%s@tcp(%s)%s?%s", u.User.Username(), pass, host, u.Path, q.Encode()), pool)
}

func openHybrid(rest string, opt hybrid.Options) (runnel.Store, error) {
	dir, query, _ := strings.Cut(rest, "?")
	if dir == "" {
		return nil, errors.New("runnel: hybrid store needs a directory, like hybrid:/var/lib/runnel")
	}
	q, err := url.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("runnel: bad hybrid options: %w", err)
	}
	if q.Has("sync") {
		if opt.Sync, err = wal.ParseSync(q.Get("sync")); err != nil {
			return nil, fmt.Errorf("runnel: hybrid %w", err)
		}
	}
	opt.Dir = dir
	return hybrid.Open(opt)
}

var poolParams = []string{"max_conns", "max_idle_conns", "conn_max_lifetime", "conn_max_idle_time"}

func takePool(dsn string) (string, sqlstore.Pool, error) {
	var p sqlstore.Pool
	u, err := url.Parse(dsn)
	if err != nil || u.RawQuery == "" {
		return dsn, p, nil
	}
	q := u.Query()
	for _, k := range poolParams {
		v := q.Get(k)
		q.Del(k)
		if v == "" {
			continue
		}
		if err := setPool(&p, k, v); err != nil {
			return "", p, fmt.Errorf("runnel: store %s: %w", k, err)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), p, nil
}

func setPool(p *sqlstore.Pool, key, v string) error {
	switch key {
	case "max_conns", "max_idle_conns":
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return errors.New("must be a positive number")
		}
		if key == "max_conns" {
			p.MaxOpen = n
		} else {
			p.MaxIdle = n
		}
	default:
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return errors.New("must be a positive duration like 5m")
		}
		if key == "conn_max_lifetime" {
			p.MaxLifetime = d
		} else {
			p.MaxIdleTime = d
		}
	}
	return nil
}

// HybridDir returns the directory of a hybrid store URL.
func HybridDir(dsn string) (string, error) {
	scheme, rest, _ := strings.Cut(dsn, ":")
	dir, _, _ := strings.Cut(strings.TrimPrefix(rest, "//"), "?")
	if !strings.EqualFold(scheme, "hybrid") || dir == "" {
		return "", fmt.Errorf("runnel: restore needs a hybrid store, like hybrid:/var/lib/runnel, not %q", redact(dsn))
	}
	return dir, nil
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
