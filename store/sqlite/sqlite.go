package sqlite

import (
	"database/sql"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/kinoforge/runnel/store/sqlstore"
)

// Open opens or creates the SQLite file at path in WAL mode, or a private in-
// memory database for "" and ":memory:". One process should own the file.
func Open(path string) (*sqlstore.Store, error) {
	if path == "" || path == ":memory:" {
		path = "file::memory:"
	} else if !strings.HasPrefix(path, "file:") {
		path = "file:" + path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	params := url.Values{}
	params.Add("_pragma", "busy_timeout(10000)")
	params.Add("_pragma", "journal_mode(WAL)")
	params.Add("_pragma", "synchronous(NORMAL)")
	params.Add("_pragma", "foreign_keys(OFF)")
	params.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", path+sep+params.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return sqlstore.New(db, sqlstore.SQLite()), nil
}
