package runnel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

func (c *Client) UpdateQueue(ctx context.Context, name string, fn func(*QueueConfig)) error {
	defer c.forget()
	_, err := UpdateTyped(ctx, c.store, c.ns, ConfigQueue, name, func(q *QueueConfig, _ bool) error {
		q.Name = name
		fn(q)
		q.Name = name
		return q.Validate()
	})
	return err
}

func (c *Client) UpdatePool(ctx context.Context, name string, fn func(*PoolConfig)) error {
	_, err := UpdateTyped(ctx, c.store, c.ns, ConfigPool, name, func(p *PoolConfig, _ bool) error {
		p.Name = name
		fn(p)
		p.Name = name
		return p.Validate()
	})
	return err
}

func UpdateTyped[T any](ctx context.Context, s Store, ns string, kind ConfigKind, name string, fn func(v *T, found bool) error) (T, error) {
	for {
		var v T
		raw, err := s.Configs(ctx, ns, kind)
		if err != nil {
			return v, err
		}
		old, found := raw[name]
		if found {
			if err := json.Unmarshal(old, &v); err != nil {
				return v, fmt.Errorf("%s config %q: %w", kind, name, err)
			}
		}
		if err := fn(&v, found); err != nil {
			return v, err
		}
		next, err := json.Marshal(v)
		if err != nil {
			return v, err
		}
		if found && bytes.Equal(old, next) {
			return v, nil
		}
		ok, err := s.SwapConfig(ctx, ns, kind, name, old, next)
		if err != nil || ok {
			return v, err
		}
		if err := ctx.Err(); err != nil {
			return v, err
		}
	}
}
