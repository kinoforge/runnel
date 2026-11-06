package hybrid

import (
	"encoding/json"

	"github.com/kinoforge/runnel"
)

func (sp *space) setUnique(key string, u unique) {
	sp.dropUnique(key)
	sp.uniques[key] = u
	if u.until > 0 {
		sp.expiry.ReplaceOrInsert(uniqueKey{u.until, key})
	}
}

func (sp *space) dropUnique(key string) {
	if old, ok := sp.uniques[key]; ok {
		sp.expiry.Delete(uniqueKey{old.until, key})
		delete(sp.uniques, key)
	}
}

func (sp *space) setConfig(kind runnel.ConfigKind, name string, val []byte) {
	m := sp.configs[kind]
	if m == nil {
		m = map[string][]byte{}
		sp.configs[kind] = m
	}
	m[name] = val
	sp.queueCfg, sp.poolCfg = nil, nil
}

func (sp *space) delConfig(kind runnel.ConfigKind, name string) {
	delete(sp.configs[kind], name)
	sp.queueCfg, sp.poolCfg = nil, nil
}

func (sp *space) queueConfig(q string) runnel.QueueConfig {
	if sp.queueCfg == nil {
		sp.queueCfg = decodeAll[runnel.QueueConfig](sp.configs[runnel.ConfigQueue])
	}
	return sp.queueCfg[q]
}

func (sp *space) poolConfig(r string) runnel.PoolConfig {
	if sp.poolCfg == nil {
		sp.poolCfg = decodeAll[runnel.PoolConfig](sp.configs[runnel.ConfigPool])
	}
	return sp.poolCfg[r]
}

// decodeAll skips values that do not decode; the client validates configs
// before it stores them.
func decodeAll[T any](raw map[string][]byte) map[string]T {
	out := make(map[string]T, len(raw))
	for name, b := range raw {
		var v T
		if json.Unmarshal(b, &v) == nil {
			out[name] = v
		}
	}
	return out
}
