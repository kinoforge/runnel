package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	pgMigrateLock    = "727366"
	mysqlLockWait    = "60"
	maxNotifyPayload = 7000
)

// Dialect holds what differs between SQL databases.
type Dialect struct {
	Name        string
	Numbered    bool
	Blob        string
	Double      string
	Serial      string
	TableSuffix string
	Isolation   sql.IsolationLevel
	LockRows    bool
	NowQuery    string
	Upsert      func(table string, cols, keys, update []string) string
	Ignore      func(table string, cols []string) string
	MigrateLock func(ctx context.Context, c *sql.Conn) (func(), error)
	Notify      func(ctx context.Context, db *sql.DB, wakes []string, ends map[string][]string) error
	NotifyTx    func(ctx context.Context, tx *sql.Tx, ns string) error
	LightCommit string
	Retryable   func(err error) bool
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func conflictUpsert(table string, cols, keys, update []string) string {
	sets := make([]string, len(update))
	for i, c := range update {
		sets[i] = c + "=excluded." + c
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		table, strings.Join(cols, ", "), placeholders(len(cols)), strings.Join(keys, ", "), strings.Join(sets, ", "))
}

func conflictIgnore(table string, cols []string) string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", table, strings.Join(cols, ", "), placeholders(len(cols)))
}

// Postgres is the dialect of PostgreSQL 13 and later.
func Postgres() Dialect {
	return Dialect{
		Name: "postgres", Numbered: true, Blob: "BYTEA", Double: "DOUBLE PRECISION", Serial: "BIGSERIAL PRIMARY KEY",
		LockRows: true, Upsert: conflictUpsert, Ignore: conflictIgnore,
		NowQuery: "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
		MigrateLock: func(ctx context.Context, c *sql.Conn) (func(), error) {
			if _, err := c.ExecContext(ctx, "SELECT pg_advisory_lock("+pgMigrateLock+")"); err != nil {
				return nil, err
			}
			return func() { _, _ = c.ExecContext(context.Background(), "SELECT pg_advisory_unlock("+pgMigrateLock+")") }, nil
		},
		Notify:      postgresNotify,
		LightCommit: "SET LOCAL synchronous_commit TO OFF",
		NotifyTx: func(ctx context.Context, tx *sql.Tx, ns string) error {
			_, err := tx.ExecContext(ctx, "SELECT pg_notify('runnel_wake', $1)", ns)
			return err
		},
		Retryable: sqlStateIn("40001", "40P01"),
	}
}

// MySQL is the dialect of MySQL 8 and later.
func MySQL() Dialect {
	return Dialect{
		Name: "mysql", Blob: "LONGBLOB", Double: "DOUBLE", Serial: "BIGINT AUTO_INCREMENT PRIMARY KEY",
		TableSuffix: " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", Isolation: sql.LevelReadCommitted, LockRows: true,
		NowQuery: "SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) DIV 1000",
		Upsert: func(table string, cols, keys, update []string) string {
			sets := make([]string, len(update))
			for i, c := range update {
				sets[i] = c + "=VALUES(" + c + ")"
			}
			return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
				table, strings.Join(cols, ", "), placeholders(len(cols)), strings.Join(sets, ", "))
		},
		Ignore: func(table string, cols []string) string {
			return fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), placeholders(len(cols)))
		},
		MigrateLock: func(ctx context.Context, c *sql.Conn) (func(), error) {
			var got sql.NullInt64
			if err := c.QueryRowContext(ctx, "SELECT GET_LOCK('runnel_migrate', "+mysqlLockWait+")").Scan(&got); err != nil {
				return nil, err
			}
			if got.Int64 != 1 {
				return nil, errors.New("runnel: could not take the migration lock")
			}
			return func() { _, _ = c.ExecContext(context.Background(), "SELECT RELEASE_LOCK('runnel_migrate')") }, nil
		},
	}
}

// SQLite is the dialect of SQLite 3.35 and later.
func SQLite() Dialect {
	return Dialect{
		Name: "sqlite", Blob: "BLOB", Double: "REAL", Serial: "INTEGER PRIMARY KEY AUTOINCREMENT",
		Upsert: conflictUpsert, Ignore: conflictIgnore,
		MigrateLock: func(context.Context, *sql.Conn) (func(), error) { return func() {}, nil },
	}
}

func sqlStateIn(codes ...string) func(error) bool {
	return func(err error) bool {
		var se interface{ SQLState() string }
		return errors.As(err, &se) && slices.Contains(codes, se.SQLState())
	}
}

func postgresNotify(ctx context.Context, db *sql.DB, wakes []string, ends map[string][]string) error {
	var channels, payloads []string
	for _, ns := range wakes {
		channels, payloads = append(channels, "runnel_wake"), append(payloads, ns)
	}
	for ns, ids := range ends {
		var b strings.Builder
		for _, id := range ids {
			if b.Len() > 0 && b.Len()+len(id) > maxNotifyPayload {
				channels, payloads = append(channels, "runnel_end"), append(payloads, ns+"|"+b.String())
				b.Reset()
			}
			if b.Len() > 0 {
				b.WriteByte(',')
			}
			b.WriteString(id)
		}
		if b.Len() > 0 {
			channels, payloads = append(channels, "runnel_end"), append(payloads, ns+"|"+b.String())
		}
	}
	if len(channels) == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx, "SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)", channels, payloads)
	return err
}

func (d Dialect) skipLocked() string {
	if d.LockRows {
		return " FOR UPDATE SKIP LOCKED"
	}
	return ""
}

func (d Dialect) forUpdate() string {
	if d.LockRows {
		return " FOR UPDATE"
	}
	return ""
}

func (d Dialect) forShare() string {
	if d.LockRows {
		return " FOR SHARE"
	}
	return ""
}

func (d Dialect) rebind(q string) string {
	if !d.Numbered {
		return q
	}
	var b strings.Builder
	b.Grow(len(q) + 16)
	n := 0
	for _, r := range q {
		if r == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (d Dialect) migrations() [][]string {
	r := strings.NewReplacer("{blob}", d.Blob, "{double}", d.Double, "{serial}", d.Serial, "{t}", d.TableSuffix)
	v1 := []string{
		`CREATE TABLE rn_jobs (
			id VARCHAR(36) NOT NULL PRIMARY KEY,
			ns VARCHAR(64) NOT NULL,
			queue VARCHAR(128) NOT NULL,
			kind VARCHAR(128) NOT NULL,
			state VARCHAR(16) NOT NULL,
			priority INT NOT NULL,
			grp VARCHAR(128) NOT NULL,
			pool VARCHAR(128) NOT NULL,
			attempt INT NOT NULL,
			max_attempts INT NOT NULL,
			run_at BIGINT NOT NULL,
			waiting_since BIGINT NOT NULL,
			start_by BIGINT NOT NULL,
			start_timeout BIGINT NOT NULL,
			deadline BIGINT NOT NULL,
			worker VARCHAR(200) NOT NULL,
			lease_until BIGINT NOT NULL,
			version BIGINT NOT NULL,
			pending INT NOT NULL,
			unique_key VARCHAR(255) NOT NULL,
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL,
			started_at BIGINT NOT NULL,
			finished_at BIGINT NOT NULL,
			data {blob} NOT NULL
		){t}`,
		`CREATE INDEX rn_jobs_ready ON rn_jobs (ns, state, queue, priority DESC, run_at, id)`,
		`CREATE INDEX rn_jobs_lease ON rn_jobs (ns, state, lease_until)`,
		`CREATE INDEX rn_jobs_start ON rn_jobs (ns, state, start_by)`,
		`CREATE INDEX rn_jobs_deadline ON rn_jobs (ns, state, deadline)`,
		`CREATE INDEX rn_jobs_finished ON rn_jobs (ns, state, finished_at)`,
		`CREATE INDEX rn_jobs_group ON rn_jobs (ns, queue, grp, state)`,
		`CREATE INDEX rn_jobs_list ON rn_jobs (ns, id)`,
		`CREATE TABLE rn_deps (
			ns VARCHAR(64) NOT NULL,
			dep_id VARCHAR(36) NOT NULL,
			job_id VARCHAR(36) NOT NULL,
			PRIMARY KEY (dep_id, job_id)
		){t}`,
		`CREATE TABLE rn_uniques (
			ns VARCHAR(64) NOT NULL,
			ukey VARCHAR(255) NOT NULL,
			job_id VARCHAR(36) NOT NULL,
			until_at BIGINT NOT NULL,
			PRIMARY KEY (ns, ukey)
		){t}`,
		`CREATE TABLE rn_events (
			seq {serial},
			ns VARCHAR(64) NOT NULL,
			job_id VARCHAR(36) NOT NULL,
			queue VARCHAR(128) NOT NULL,
			kind VARCHAR(128) NOT NULL,
			state VARCHAR(16) NOT NULL,
			at_ms BIGINT NOT NULL
		){t}`,
		`CREATE INDEX rn_events_ns ON rn_events (ns, seq)`,
		`CREATE INDEX rn_events_at ON rn_events (at_ms)`,
		`CREATE TABLE rn_cursors (
			ns VARCHAR(64) NOT NULL,
			name VARCHAR(200) NOT NULL,
			val BIGINT NOT NULL,
			PRIMARY KEY (ns, name)
		){t}`,
		`CREATE TABLE rn_configs (
			ns VARCHAR(64) NOT NULL,
			kind VARCHAR(32) NOT NULL,
			name VARCHAR(128) NOT NULL,
			val {blob} NOT NULL,
			PRIMARY KEY (ns, kind, name)
		){t}`,
		`CREATE TABLE rn_buckets (
			ns VARCHAR(64) NOT NULL,
			bkey VARCHAR(300) NOT NULL,
			tokens {double} NOT NULL,
			at_ms BIGINT NOT NULL,
			PRIMARY KEY (ns, bkey)
		){t}`,
		`CREATE TABLE rn_locks (ns VARCHAR(64) NOT NULL PRIMARY KEY){t}`,
		`CREATE TABLE rn_archive (
			id VARCHAR(36) NOT NULL PRIMARY KEY,
			ns VARCHAR(64) NOT NULL,
			queue VARCHAR(128) NOT NULL,
			state VARCHAR(16) NOT NULL,
			finished_at BIGINT NOT NULL,
			data {blob} NOT NULL
		){t}`,
		`CREATE INDEX rn_archive_ns ON rn_archive (ns, finished_at)`,
	}
	out := [][]string{v1}
	for i := range out {
		for k := range out[i] {
			out[i][k] = r.Replace(out[i][k])
		}
	}
	return out
}
