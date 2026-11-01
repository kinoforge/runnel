package stores

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/mysql"
	"github.com/kinoforge/runnel/store/postgres"
	"github.com/kinoforge/runnel/store/redisstore"
	"github.com/kinoforge/runnel/store/sqlite"
)

// Open opens and migrates the store a URL names: postgres://, mysql://,
// sqlite:path, or redis://, rediss://, valkey:// and dragonfly://.
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
