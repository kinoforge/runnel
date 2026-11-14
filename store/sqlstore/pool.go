package sqlstore

import (
	"database/sql"
	"time"
)

type Pool struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

var DefaultPool = Pool{MaxOpen: 20, MaxIdle: 10, MaxLifetime: 30 * time.Minute, MaxIdleTime: 5 * time.Minute}

func (p Pool) Apply(db *sql.DB) {
	d := DefaultPool
	db.SetMaxOpenConns(pick(p.MaxOpen, d.MaxOpen))
	db.SetMaxIdleConns(min(pick(p.MaxIdle, d.MaxIdle), pick(p.MaxOpen, d.MaxOpen)))
	db.SetConnMaxLifetime(pick(p.MaxLifetime, d.MaxLifetime))
	db.SetConnMaxIdleTime(pick(p.MaxIdleTime, d.MaxIdleTime))
}

func pick[T int | time.Duration](v, fallback T) T {
	if v > 0 {
		return v
	}
	return fallback
}
