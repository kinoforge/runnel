package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	httpTimeout     = 60 * time.Second
	maxErrorBody    = 64 << 10
	resultWait      = 30 * time.Second
	quickPoll       = 500 * time.Millisecond
	workerPoll      = 50 * time.Millisecond
	workerClaimWait = 10 * time.Second
)

// Client talks to a Runnel server for one namespace.
type Client struct {
	base string
	key  string
	ns   string
	http *http.Client
}

// New returns a client for the server at baseURL, authenticating with an API
// key.
func New(baseURL, key, namespace string) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), key: key, ns: namespace, http: &http.Client{Timeout: httpTimeout}}
}

// WithHTTPClient replaces the HTTP client. Its timeout must exceed the long-poll
// waits, 30 seconds by default.
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	c.http = h
	return c
}

// APIError is a problem document the server answered with. It unwraps to the
// matching runnel error, so errors.Is works across the wire.
type APIError struct {
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("runnel api: %d %s: %s", e.Status, e.Title, e.Detail)
}

// Unwrap maps the status to a runnel error.
func (e *APIError) Unwrap() error {
	switch e.Status {
	case http.StatusNotFound:
		return runnel.ErrNotFound
	case http.StatusConflict:
		return runnel.ErrConflict
	case http.StatusTooManyRequests:
		if e.Title == "queue full" {
			return runnel.ErrQueueFull
		}
		return runnel.ErrGroupFull
	case http.StatusUnprocessableEntity:
		return runnel.ErrDependency
	case http.StatusBadRequest:
		return runnel.ErrInvalid
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	_, err := c.send(ctx, method, path, in, out, nil)
	return err
}

func (c *Client) send(ctx context.Context, method, path string, in, out any, header http.Header) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, err
	}
	maps.Copy(req.Header, header)
	req.Header.Set("Authorization", "Bearer "+c.key)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		e := &APIError{Status: resp.StatusCode}
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(e)
		e.Status = resp.StatusCode
		return resp.StatusCode, e
	}
	if out == nil {
		return resp.StatusCode, nil
	}
	return resp.StatusCode, json.NewDecoder(resp.Body).Decode(out)
}

func waitHeader(ctx context.Context, most time.Duration) http.Header {
	if dl, ok := ctx.Deadline(); ok {
		most = min(most, time.Until(dl))
	}
	return http.Header{"Prefer": {"wait=" + strconv.Itoa(max(1, int(most/time.Second)))}}
}

func (c *Client) path(p string) string { return "/v1/ns/" + url.PathEscape(c.ns) + p }

type jobRequest struct {
	Queue        string            `json:"queue"`
	Kind         string            `json:"kind"`
	Payload      json.RawMessage   `json:"payload,omitempty"`
	Priority     int               `json:"priority,omitempty"`
	Group        string            `json:"group,omitempty"`
	Pool         string            `json:"pool,omitempty"`
	RunAt        time.Time         `json:"run_at,omitzero"`
	Delay        runnel.Duration   `json:"delay,omitempty"`
	MaxAttempts  int               `json:"max_attempts,omitempty"`
	UniqueKey    string            `json:"unique_key,omitempty"`
	UniqueFor    runnel.Duration   `json:"unique_for,omitempty"`
	StartTimeout runnel.Duration   `json:"start_timeout,omitempty"`
	Deadline     time.Time         `json:"deadline,omitzero"`
	TTL          runnel.Duration   `json:"ttl,omitempty"`
	Timeout      runnel.Duration   `json:"timeout,omitempty"`
	Backoff      runnel.Backoff    `json:"backoff,omitzero"`
	DependsOn    []string          `json:"depends_on,omitempty"`
	CallbackURL  string            `json:"callback_url,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
}

func request(j *runnel.Job) jobRequest {
	delay, ttl := j.Offsets()
	return jobRequest{Queue: j.Queue, Kind: j.Kind, Payload: j.Payload, Priority: j.Priority, Group: j.Group, Pool: j.Pool,
		RunAt: j.RunAt, Delay: runnel.Duration(delay), MaxAttempts: j.MaxAttempts, UniqueKey: j.UniqueKey, UniqueFor: j.UniqueFor,
		StartTimeout: j.StartTimeout, Deadline: j.Deadline, TTL: runnel.Duration(ttl),
		Timeout: j.Timeout, Backoff: j.Backoff, DependsOn: j.DependsOn, CallbackURL: j.CallbackURL, Meta: j.Meta}
}

func (c *Client) Enqueue(ctx context.Context, queue, kind string, payload any, opts ...runnel.Option) (runnel.Inserted, error) {
	j, err := runnel.NewJob(queue, kind, payload, opts...)
	if err != nil {
		return runnel.Inserted{}, err
	}
	var out struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	if err := c.do(ctx, http.MethodPost, c.path("/jobs"), request(j), &out); err != nil {
		return runnel.Inserted{}, err
	}
	return runnel.Inserted{ID: out.ID, Duplicate: out.Duplicate}, nil
}

// EnqueueAndWait enqueues a job and waits for it to finish. When ctx ends
// first it returns the job as last read together with the context's error.
func (c *Client) EnqueueAndWait(ctx context.Context, queue, kind string, payload any, opts ...runnel.Option) (*runnel.Job, error) {
	j, err := runnel.NewJob(queue, kind, payload, opts...)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	status, err := c.send(ctx, http.MethodPost, c.path("/jobs"), request(j), &raw, waitHeader(ctx, resultWait))
	if err != nil {
		return nil, err
	}
	var out runnel.Job
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if status == http.StatusOK && out.State.Final() {
		return &out, nil
	}
	return c.Wait(ctx, out.ID)
}

// Wait long-polls a job until it is completed, dead or cancelled.
func (c *Client) Wait(ctx context.Context, id string) (*runnel.Job, error) {
	var last *runnel.Job
	for {
		began := time.Now()
		var j runnel.Job
		_, err := c.send(ctx, http.MethodGet, c.path("/jobs/"+url.PathEscape(id)), nil, &j, waitHeader(ctx, resultWait))
		if err != nil {
			if last != nil && ctx.Err() != nil {
				return last, ctx.Err()
			}
			return nil, err
		}
		if j.State.Final() {
			return &j, nil
		}
		last = &j
		if quick := quickPoll - time.Since(began); quick > 0 {
			t := time.NewTimer(quick)
			select {
			case <-ctx.Done():
			case <-t.C:
			}
			t.Stop()
		}
		if ctx.Err() != nil {
			return last, ctx.Err()
		}
	}
}

func (c *Client) Get(ctx context.Context, id string) (*runnel.Job, error) {
	var j runnel.Job
	if err := c.do(ctx, http.MethodGet, c.path("/jobs/"+url.PathEscape(id)), nil, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// List returns a page of jobs and the cursor of the next page.
func (c *Client) List(ctx context.Context, q runnel.Query) ([]*runnel.Job, string, error) {
	v := url.Values{}
	for k, s := range map[string]string{"queue": q.Queue, "kind": q.Kind, "group": q.Group, "state": string(q.State), "cursor": q.Cursor} {
		if s != "" {
			v.Set(k, s)
		}
	}
	if q.Limit > 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	var out struct {
		Items []*runnel.Job `json:"items"`
		Next  string        `json:"next"`
	}
	if err := c.do(ctx, http.MethodGet, c.path("/jobs?"+v.Encode()), nil, &out); err != nil {
		return nil, "", err
	}
	return out.Items, out.Next, nil
}

func (c *Client) Cancel(ctx context.Context, id string) (*runnel.Job, error) {
	var j runnel.Job
	return &j, c.do(ctx, http.MethodPost, c.path("/jobs/"+url.PathEscape(id)+"/cancel"), struct{}{}, &j)
}

// Retry runs a dead or cancelled job again.
func (c *Client) Retry(ctx context.Context, id string) (*runnel.Job, error) {
	var j runnel.Job
	return &j, c.do(ctx, http.MethodPost, c.path("/jobs/"+url.PathEscape(id)+"/retry"), struct{}{}, &j)
}

func (c *Client) Stats(ctx context.Context) ([]runnel.QueueStats, error) {
	var out struct {
		Queues []runnel.QueueStats `json:"queues"`
	}
	return out.Queues, c.do(ctx, http.MethodGet, c.path("/stats"), nil, &out)
}

type source struct {
	c    *Client
	wait time.Duration
}

// Source returns a runnel.Source over the server's lease protocol. Claims long-
// poll for up to longPoll.
func (c *Client) Source(longPoll time.Duration) runnel.Source { return &source{c: c, wait: longPoll} }

func (s *source) Claim(ctx context.Context, worker string, queues []string, limit int, lease time.Duration) ([]*runnel.Job, error) {
	var out struct {
		Jobs []*runnel.Job `json:"jobs"`
	}
	in := map[string]any{"worker": worker, "queues": queues, "limit": limit, "lease": runnel.Duration(lease), "wait": runnel.Duration(s.wait)}
	return out.Jobs, s.c.do(ctx, http.MethodPost, s.c.path("/claim"), in, &out)
}

func (s *source) Extend(ctx context.Context, worker string, ids []string, lease time.Duration) ([]string, error) {
	var out struct {
		Lost []string `json:"lost"`
	}
	in := map[string]any{"worker": worker, "ids": ids, "lease": runnel.Duration(lease)}
	return out.Lost, s.c.do(ctx, http.MethodPost, s.c.path("/extend"), in, &out)
}

func (s *source) Finish(ctx context.Context, worker string, j *runnel.Job, o runnel.Outcome) error {
	in := map[string]any{"worker": worker, "version": j.Version, "outcome": o, "payload": j.Payload, "meta": j.Meta}
	return s.c.do(ctx, http.MethodPost, s.c.path("/jobs/"+url.PathEscape(j.ID)+"/finish"), in, nil)
}

func (s *source) Jobs(ctx context.Context, ids []string) ([]*runnel.Job, error) {
	var out struct {
		Jobs []*runnel.Job `json:"jobs"`
	}
	return out.Jobs, s.c.do(ctx, http.MethodPost, s.c.path("/dependencies"), map[string]any{"ids": ids}, &out)
}

func (s *source) Wake(context.Context) <-chan struct{} { return nil }

// NewWorker returns a worker that claims from the server. Workers on any machine
// share the server's limits.
func NewWorker(c *Client, cfg runnel.WorkerConfig) *runnel.Worker {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = workerPoll
	}
	return runnel.NewSourceWorker(c.Source(workerClaimWait), cfg)
}
