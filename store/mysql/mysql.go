package mysql

import (
	"database/sql"
	"errors"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/kinoforge/runnel/store/sqlstore"
)

// Open connects to MySQL with a go-sql-driver DSN such as
// user:pass@tcp(host:3306)/db. Sessions run in UTC.
func Open(dsn string) (*sqlstore.Store, error) {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ClientFoundRows = true
	cfg.MultiStatements = false
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	conn, err := driver.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	d := sqlstore.MySQL()
	d.Retryable = func(err error) bool {
		var me *driver.MySQLError
		return errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205)
	}
	return sqlstore.New(db, d), nil
}
