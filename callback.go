package runnel

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	maxDrainBody    = 64 << 10
	dialKeepAlive   = 30 * time.Second
	maxIdleConns    = 50
	idleConnTimeout = 90 * time.Second
)

const (
	// CallbackKind is the kind of the jobs that deliver callbacks.
	CallbackKind = "runnel.callback"
	// CallbackQueue is the queue callback deliveries run on. It is reserved.
	CallbackQueue = "runnel.callbacks"
)

type CallbackConfig struct {
	Enabled       bool
	Queue         string
	Concurrency   int
	AllowPrivate  bool
	Timeout       time.Duration
	MaxAttempts   int
	Backoff       Backoff
	MaxRetryAfter time.Duration
	UniqueFor     time.Duration
	SecretTTL     time.Duration
	Client        *http.Client
}

var DefaultCallbackConfig = CallbackConfig{
	Queue: CallbackQueue, Concurrency: 4, Timeout: 10 * time.Second, MaxAttempts: 10,
	Backoff:       Backoff{Base: Duration(5 * time.Second), Max: Duration(30 * time.Minute)},
	MaxRetryAfter: time.Hour, UniqueFor: 48 * time.Hour, SecretTTL: 30 * time.Second,
}

func (c CallbackConfig) withDefaults() CallbackConfig {
	d := DefaultCallbackConfig
	c.Queue = cmp.Or(c.Queue, d.Queue)
	c.Concurrency = positive(c.Concurrency, d.Concurrency)
	c.Timeout = positive(c.Timeout, d.Timeout)
	c.MaxAttempts = positive(c.MaxAttempts, d.MaxAttempts)
	c.Backoff.Base = positive(c.Backoff.Base, d.Backoff.Base)
	c.Backoff.Max = positive(c.Backoff.Max, d.Backoff.Max)
	c.MaxRetryAfter = positive(c.MaxRetryAfter, d.MaxRetryAfter)
	c.UniqueFor = positive(c.UniqueFor, d.UniqueFor)
	c.SecretTTL = positive(c.SecretTTL, d.SecretTTL)
	return c
}

func positive[T int | Duration | time.Duration](v, fallback T) T {
	if v > 0 {
		return v
	}
	return fallback
}

// JobSummary is the body of a callback: what a receiver needs to know about the
// finished job.
type JobSummary struct {
	ID         string            `json:"id"`
	Namespace  string            `json:"namespace"`
	Queue      string            `json:"queue"`
	Kind       string            `json:"kind"`
	State      State             `json:"state"`
	Group      string            `json:"group,omitempty"`
	Attempt    int               `json:"attempt"`
	Meta       map[string]string `json:"meta,omitempty"`
	Result     json.RawMessage   `json:"result,omitempty"`
	LastError  string            `json:"last_error,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	FinishedAt time.Time         `json:"finished_at,omitzero"`
}

func summarize(j *Job) JobSummary {
	s := JobSummary{ID: j.ID, Namespace: j.Namespace, Queue: j.Queue, Kind: j.Kind, State: j.State, Group: j.Group,
		Attempt: j.Attempt, Meta: j.Meta, Result: j.Result, CreatedAt: j.CreatedAt, FinishedAt: j.FinishedAt}
	if n := len(j.Errors); n > 0 {
		s.LastError = j.Errors[n-1].Error
	}
	return s
}

type callbackTask struct {
	URL      string     `json:"url"`
	Delivery int64      `json:"delivery"`
	Event    string     `json:"event"`
	Job      JobSummary `json:"job"`
}

// NewCallbackJob returns the job that delivers event for j. Delivery numbers keep
// deliveries unique, so an event is posted once even when it is seen twice.
func NewCallbackJob(j *Job, event string, delivery int64, cfg CallbackConfig) (*Job, error) {
	cfg = cfg.withDefaults()
	body, err := json.Marshal(callbackTask{URL: j.CallbackURL, Delivery: delivery, Event: event, Job: summarize(j)})
	if err != nil {
		return nil, err
	}
	return &Job{Queue: cfg.Queue, Kind: CallbackKind, Payload: body, MaxAttempts: cfg.MaxAttempts,
		UniqueKey: fmt.Sprintf("callback:%d", delivery), UniqueFor: Duration(cfg.UniqueFor), Backoff: cfg.Backoff}, nil
}

// SecretFunc returns the callback secret of a namespace.
type SecretFunc func(ctx context.Context, namespace string) (string, error)

type callbackHandler struct {
	secret        SecretFunc
	client        *http.Client
	maxRetryAfter time.Duration
}

// NewCallbackSender returns the handler of callback jobs, signing each body with
// the namespace's secret.
func NewCallbackSender(secret SecretFunc, cfg CallbackConfig) Handler {
	cfg = cfg.withDefaults()
	client := cfg.Client
	if client == nil {
		client = SafeHTTPClient(cfg.Timeout, cfg.AllowPrivate)
	}
	if secret == nil {
		secret = func(context.Context, string) (string, error) { return "", nil }
	}
	return &callbackHandler{secret: secret, client: client, maxRetryAfter: cfg.MaxRetryAfter}
}

// NewCallbackHandler returns a callback sender that reads secrets from the
// namespaces in s.
func NewCallbackHandler(s Store, cfg CallbackConfig) Handler {
	ttl := cfg.withDefaults().SecretTTL
	var mu sync.Mutex
	var cache map[string]string
	var loaded time.Time
	return NewCallbackSender(func(ctx context.Context, ns string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if cache == nil || time.Since(loaded) > ttl {
			all, err := Namespaces(ctx, s)
			if err != nil {
				return "", err
			}
			cache = map[string]string{}
			for name, n := range all {
				cache[name] = n.CallbackSecret
			}
			loaded = time.Now()
		}
		return cache[ns], nil
	}, cfg)
}

// Sign returns the Runnel-Signature header for body: t=<unix seconds>,v1=<hex
// HMAC-SHA256 of "<t>.<body>">. Receivers should recompute it and reject old
// timestamps.
func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", ts)
	mac.Write(body)
	return "t=" + strconv.FormatInt(ts, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func (h *callbackHandler) Work(ctx context.Context, j *Job) error {
	var t callbackTask
	if err := j.Bind(&t); err != nil {
		return Fail(err)
	}
	if err := ValidCallbackURL(t.URL, DefaultLimits.MaxCallbackURL); err != nil {
		return Fail(err)
	}
	body, err := json.Marshal(CallbackBody{Event: t.Event, Delivery: t.Delivery, Job: t.Job})
	if err != nil {
		return Fail(err)
	}
	secret, err := h.secret(ctx, t.Job.Namespace)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.URL, bytes.NewReader(body))
	if err != nil {
		return Fail(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "runnel-callback/1")
	req.Header.Set("Runnel-Event", t.Event)
	req.Header.Set("Runnel-Delivery", strconv.FormatInt(t.Delivery, 10))
	for _, h := range traceHeaders {
		if v := t.Job.Meta[h]; v != "" {
			req.Header.Set(h, v)
		}
	}
	if secret != "" {
		req.Header.Set("Runnel-Signature", Sign(secret, time.Now().Unix(), body))
	}
	resp, err := h.client.Do(req)
	if err != nil {
		if errors.Is(err, errBlockedAddress) {
			return Fail(err)
		}
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 && time.Duration(s)*time.Second <= h.maxRetryAfter {
			return RetryIn(time.Duration(s)*time.Second, fmt.Errorf("callback answered %d", resp.StatusCode))
		}
		return fmt.Errorf("callback answered %d", resp.StatusCode)
	default:
		return Fail(fmt.Errorf("callback answered %d", resp.StatusCode))
	}
}

// TraceParent and TraceState are the W3C trace context headers. A job
// carries them in its meta under the same names, so a trace goes on through
// workers and callbacks.
const (
	TraceParent = "traceparent"
	TraceState  = "tracestate"
)

var traceHeaders = []string{TraceParent, TraceState}

var errBlockedAddress = errors.New("runnel: callback address is private or reserved")

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("fc00::/7"),
}

// PublicAddress reports whether a is a public unicast address, so not loopback,
// private, link-local, multicast or unspecified.
func PublicAddress(a netip.Addr) bool {
	a = a.Unmap()
	if !a.IsValid() || a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsMulticast() || a.IsUnspecified() || a.IsInterfaceLocalMulticast() {
		return false
	}
	for _, p := range blockedPrefixes {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

// SafeHTTPClient returns a client that never follows redirects and, unless
// allowPrivate is set, refuses to connect to addresses that are not public. The
// check runs on every connection, after DNS resolution.
func SafeHTTPClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: dialKeepAlive}
	if !allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			a, err := netip.ParseAddr(host)
			if err != nil || !PublicAddress(a) {
				return errBlockedAddress
			}
			return nil
		}
	}
	tr := &http.Transport{DialContext: dialer.DialContext, Proxy: nil, MaxIdleConns: maxIdleConns, IdleConnTimeout: idleConnTimeout,
		TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout}
	return &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}
