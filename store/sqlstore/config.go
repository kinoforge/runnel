package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/kinoforge/runnel"
)

func (s *Store) queueConfigs(ctx context.Context, q querier, ns string) (map[string]runnel.QueueConfig, error) {
	out := map[string]runnel.QueueConfig{}
	err := s.eachConfig(ctx, q, ns, runnel.ConfigQueue, func(name string, b []byte) error {
		var c runnel.QueueConfig
		if err := json.Unmarshal(b, &c); err != nil {
			return err
		}
		out[name] = c
		return nil
	})
	return out, err
}

func (s *Store) resourceConfigs(ctx context.Context, q querier, ns string) (map[string]runnel.ResourceConfig, error) {
	out := map[string]runnel.ResourceConfig{}
	err := s.eachConfig(ctx, q, ns, runnel.ConfigResource, func(name string, b []byte) error {
		var c runnel.ResourceConfig
		if err := json.Unmarshal(b, &c); err != nil {
			return err
		}
		out[name] = c
		return nil
	})
	return out, err
}

func (s *Store) eachConfig(ctx context.Context, q querier, ns string, kind runnel.ConfigKind, fn func(string, []byte) error) error {
	rows, err := q.QueryContext(ctx, s.q("SELECT name, val FROM rn_configs WHERE ns=? AND kind=?"), ns, string(kind))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var val []byte
		if err := rows.Scan(&name, &val); err != nil {
			return err
		}
		if err := fn(name, val); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) PutConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	defer s.forget(ns)
	up := s.d.Upsert("rn_configs", []string{"ns", "kind", "name", "val"}, []string{"ns", "kind", "name"}, []string{"val"})
	_, err := s.db.ExecContext(ctx, s.q(up), ns, string(kind), name, value)
	return err
}

func (s *Store) Configs(ctx context.Context, ns string, kind runnel.ConfigKind) (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.eachConfig(ctx, s.db, ns, kind, func(name string, b []byte) error {
		out[name] = b
		return nil
	})
	return out, err
}

// ConfigsTx reads configuration inside the caller's transaction.
func (s *Store) ConfigsTx(ctx context.Context, tx *sql.Tx, ns string, kind runnel.ConfigKind) (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.eachConfig(ctx, tx, ns, kind, func(name string, b []byte) error {
		out[name] = b
		return nil
	})
	return out, err
}

func (s *Store) DeleteConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string) error {
	defer s.forget(ns)
	_, err := s.db.ExecContext(ctx, s.q("DELETE FROM rn_configs WHERE ns=? AND kind=? AND name=?"), ns, string(kind), name)
	return err
}
