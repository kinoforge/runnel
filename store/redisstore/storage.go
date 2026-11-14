package redisstore

import (
	"bufio"
	"context"
	"strconv"
	"strings"

	"github.com/kinoforge/runnel"
)

var memoryFields = map[string]string{"used_memory": "", "used_memory_rss": "rss_bytes", "used_memory_peak": "peak_bytes", "maxmemory": "max_bytes"}

func (s *Store) Storage(ctx context.Context) (runnel.StorageInfo, error) {
	info := runnel.StorageInfo{Kind: "redis", Details: map[string]int64{}}
	raw, err := s.rdb.Info(ctx, "memory").Result()
	if err != nil {
		return info, err
	}
	sc := bufio.NewScanner(strings.NewReader(raw))
	for sc.Scan() {
		k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), ":")
		name, known := memoryFields[k]
		if !ok || !known {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		if name == "" {
			info.Bytes = n
		} else {
			info.Details[name] = n
		}
	}
	return info, sc.Err()
}
