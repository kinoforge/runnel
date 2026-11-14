package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/kinoforge/runnel"
)

var errNoETag = errors.New("runnel api: the server sent no ETag; updates need Runnel 0.0.3-beta.2 or later")

func (c *Client) SetQueue(ctx context.Context, q runnel.QueueConfig) error {
	return c.do(ctx, http.MethodPut, c.path("/queues/"+url.PathEscape(q.Name)), q, nil)
}

func (c *Client) SetPool(ctx context.Context, p runnel.PoolConfig) error {
	return c.do(ctx, http.MethodPut, c.path("/pools/"+url.PathEscape(p.Name)), p, nil)
}

func (c *Client) UpdateQueue(ctx context.Context, name string, fn func(*runnel.QueueConfig)) error {
	return update(ctx, c, "/queues/", name, func(q *runnel.QueueConfig) error {
		q.Name = name
		fn(q)
		q.Name = name
		return q.Validate()
	})
}

func (c *Client) UpdatePool(ctx context.Context, name string, fn func(*runnel.PoolConfig)) error {
	return update(ctx, c, "/pools/", name, func(p *runnel.PoolConfig) error {
		p.Name = name
		fn(p)
		p.Name = name
		return p.Validate()
	})
}

func update[T any](ctx context.Context, c *Client, kind, name string, fn func(*T) error) error {
	p := c.path(kind + url.PathEscape(name))
	for {
		var v T
		_, got, err := c.send(ctx, http.MethodGet, p, nil, &v, nil)
		found := err == nil
		if err != nil && !errors.Is(err, runnel.ErrNotFound) {
			return err
		}
		before, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if err := fn(&v); err != nil {
			return err
		}
		after, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if found && bytes.Equal(before, after) {
			return nil
		}
		cond := http.Header{"If-None-Match": {"*"}}
		if found {
			tag := got.Get("ETag")
			if tag == "" {
				return errNoETag
			}
			cond = http.Header{"If-Match": {tag}}
		}
		_, _, err = c.send(ctx, http.MethodPut, p, v, nil, cond)
		if !errors.Is(err, runnel.ErrStale) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
