package sqlstore

import (
	"context"

	"github.com/kinoforge/runnel"
)

func (s *Store) Storage(ctx context.Context) (runnel.StorageInfo, error) {
	st := s.db.Stats()
	info := runnel.StorageInfo{Kind: s.d.Name, Details: map[string]int64{"open_connections": int64(st.OpenConnections), "in_use_connections": int64(st.InUse)}}
	if s.d.SizeQuery == "" {
		return info, nil
	}
	err := s.db.QueryRowContext(ctx, s.d.SizeQuery).Scan(&info.Bytes)
	return info, err
}
