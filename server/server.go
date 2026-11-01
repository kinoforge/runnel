package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/metrics"
)

//go:embed openapi.yaml
var openAPI []byte

// Config configures a server. AdminToken guards the namespace and key routes;
// without it they are closed. MaxBody caps request bodies (4 MiB). MaxClaimWait
// and MaxResultWait cap long-polls for claims (30s) and results (60s).
// RetryAfter is the hint sent with 202 and 429 answers (1s).
type Config struct {
	AdminToken        string
	MaxBody           int64
	Limits            runnel.Limits
	AllowHTTPWebhooks bool
	MaxClaimWait      time.Duration
	MaxResultWait     time.Duration
	RetryAfter        time.Duration
	Logger            *slog.Logger
	Registry          *prometheus.Registry
}

// Server is the Runnel HTTP API: namespaces, keys, jobs, configuration and the
// worker lease protocol. It implements http.Handler.
type Server struct {
	store   runnel.Store
	clock   runnel.Clock
	cfg     Config
	log     *slog.Logger
	mux     *http.ServeMux
	obs     *metrics.Observer
	reqs    *prometheus.CounterVec
	admin   [32]byte
	hasRoot bool

	patterns []string

	mu      sync.Mutex
	keys    map[string]keyEntry
	loaded  time.Time
	clients map[string]*runnel.Client
	sources map[string]runnel.Source
	hubs    map[string]*hub
}

type keyEntry struct {
	ns  string
	key runnel.APIKey
}

type principal struct {
	admin bool
	ns    string
	key   runnel.APIKey
}

func (p principal) id() string {
	if p.admin {
		return "admin"
	}
	return p.key.ID
}

func (p principal) can(ns, role string) bool {
	if p.admin {
		return true
	}
	return p.ns == ns && p.key.Has(role)
}

// New returns a server over store.
func New(store runnel.Store, cfg Config) *Server {
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 4 << 20
	}
	if cfg.Limits == (runnel.Limits{}) {
		cfg.Limits = runnel.DefaultLimits
	}
	if cfg.MaxClaimWait <= 0 {
		cfg.MaxClaimWait = 30 * time.Second
	}
	if cfg.MaxResultWait <= 0 {
		cfg.MaxResultWait = 60 * time.Second
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registry == nil {
		cfg.Registry = prometheus.NewRegistry()
		cfg.Registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	}
	s := &Server{store: store, clock: runnel.ClockOf(store), cfg: cfg, log: cfg.Logger, mux: http.NewServeMux(), clients: map[string]*runnel.Client{}, sources: map[string]runnel.Source{}, hubs: map[string]*hub{}}
	if cfg.AdminToken != "" {
		s.admin, s.hasRoot = sha256.Sum256([]byte(cfg.AdminToken)), true
	}
	s.obs = metrics.NewObserver(cfg.Registry)
	s.reqs = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "runnel_http_requests_total", Help: "API requests."}, []string{"route", "code"})
	cfg.Registry.MustRegister(s.reqs, metrics.NewStatsCollector(store, s.namespaceNames))
	s.routes()
	return s
}

// Observer returns the metrics observer, to pass to local workers so their
// attempts are counted with the remote ones.
func (s *Server) Observer() runnel.Observer { return s.obs }

// ServeHTTP serves the API.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	s.mux.ServeHTTP(w, r)
}

type handler func(w http.ResponseWriter, r *http.Request, p principal) error

func (s *Server) handle(pattern, role string, h handler) {
	route := pattern
	s.patterns = append(s.patterns, pattern)
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		rw := &statusWriter{ResponseWriter: w, code: 200}
		defer func() { s.reqs.WithLabelValues(route, strconv.Itoa(rw.code)).Inc() }()
		p, ok := s.authenticate(r)
		if !ok {
			s.problem(rw, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		ns := r.PathValue("ns")
		if role == "" && !p.admin {
			s.problem(rw, http.StatusForbidden, "forbidden", "admin token required")
			return
		}
		if role != "" {
			if !runnel.ValidNamespace(ns) {
				s.problem(rw, http.StatusNotFound, "not found", "unknown namespace")
				return
			}
			if !p.can(ns, role) {
				s.problem(rw, http.StatusForbidden, "forbidden", "this key lacks the "+role+" role for namespace "+ns)
				return
			}
		}
		r.Body = http.MaxBytesReader(rw, r.Body, s.cfg.MaxBody)
		if err := h(rw, r, p); err != nil {
			s.fail(rw, err)
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if _, err := s.store.Configs(ctx, "", runnel.ConfigNamespace); err != nil {
			s.problem(w, http.StatusServiceUnavailable, "unavailable", "store unreachable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.cfg.Registry, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openAPI)
	})
	s.patterns = append(s.patterns, "GET /healthz", "GET /metrics", "GET /openapi.yaml")

	s.handle("GET /v1/namespaces", "", s.listNamespaces)
	s.handle("PUT /v1/namespaces/{name}", "", s.putNamespace)
	s.handle("GET /v1/namespaces/{name}", "", s.getNamespace)
	s.handle("DELETE /v1/namespaces/{name}", "", s.deleteNamespace)
	s.handle("POST /v1/namespaces/{name}/keys", "", s.createKey)
	s.handle("DELETE /v1/namespaces/{name}/keys/{id}", "", s.deleteKey)

	s.handle("POST /v1/ns/{ns}/jobs", runnel.RoleProducer, s.enqueue)
	s.handle("POST /v1/ns/{ns}/flows", runnel.RoleProducer, s.enqueueFlow)
	s.handle("GET /v1/ns/{ns}/jobs", runnel.RoleReader, s.listJobs)
	s.handle("GET /v1/ns/{ns}/jobs/{id}", runnel.RoleReader, s.getJob)
	s.handle("POST /v1/ns/{ns}/jobs/{id}/cancel", runnel.RoleProducer, s.cancelJob)
	s.handle("POST /v1/ns/{ns}/jobs/{id}/retry", runnel.RoleAdmin, s.retryJob)
	s.handle("GET /v1/ns/{ns}/stats", runnel.RoleReader, s.stats)
	s.handle("GET /v1/ns/{ns}/events", runnel.RoleReader, s.events)

	s.handle("POST /v1/ns/{ns}/claim", runnel.RoleWorker, s.claim)
	s.handle("POST /v1/ns/{ns}/extend", runnel.RoleWorker, s.extend)
	s.handle("POST /v1/ns/{ns}/jobs/{id}/finish", runnel.RoleWorker, s.finish)
	s.handle("POST /v1/ns/{ns}/dependencies", runnel.RoleWorker, s.dependencies)

	configRoutes(s, "queues", runnel.ConfigQueue, func(c runnel.QueueConfig) (string, error) { return c.Name, c.Validate() })
	configRoutes(s, "pools", runnel.ConfigPool, func(c runnel.PoolConfig) (string, error) { return c.Name, c.Validate() })
	configRoutes(s, "schedules", runnel.ConfigSchedule, func(c runnel.Schedule) (string, error) {
		if c.Webhook != "" {
			if err := s.checkWebhook(c.Webhook); err != nil {
				return c.Name, err
			}
		}
		return c.Name, c.Validate()
	})
}

func configRoutes[T any](s *Server, path string, kind runnel.ConfigKind, check func(T) (string, error)) {
	s.handle("GET /v1/ns/{ns}/"+path, runnel.RoleReader, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		all, err := runnel.LoadTyped[T](r.Context(), s.store, r.PathValue("ns"), kind)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": all})
		return nil
	})
	s.handle("GET /v1/ns/{ns}/"+path+"/{name}", runnel.RoleReader, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		all, err := runnel.LoadTyped[T](r.Context(), s.store, r.PathValue("ns"), kind)
		if err != nil {
			return err
		}
		v, ok := all[r.PathValue("name")]
		if !ok {
			return runnel.ErrNotFound
		}
		writeJSON(w, http.StatusOK, v)
		return nil
	})
	s.handle("PUT /v1/ns/{ns}/"+path+"/{name}", runnel.RoleAdmin, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		var v T
		if err := decode(r, &v); err != nil {
			return err
		}
		name, err := check(v)
		if err != nil {
			return err
		}
		if name != r.PathValue("name") {
			return fmt.Errorf("%w: name in body must match the path", runnel.ErrInvalid)
		}
		if err := runnel.PutTyped(r.Context(), s.store, r.PathValue("ns"), kind, name, v); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, v)
		return nil
	})
	s.handle("DELETE /v1/ns/{ns}/"+path+"/{name}", runnel.RoleAdmin, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		if err := s.store.DeleteConfig(r.Context(), r.PathValue("ns"), kind, r.PathValue("name")); err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	})
}

func (s *Server) authenticate(r *http.Request) (principal, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" || len(token) > 512 {
		return principal{}, false
	}
	if s.hasRoot {
		sum := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(sum[:], s.admin[:]) == 1 {
			return principal{admin: true}, true
		}
	}
	id, secret, ok := runnel.ParseAPIKey(token)
	if !ok {
		return principal{}, false
	}
	e, ok := s.lookupKey(r.Context(), id)
	if !ok || !e.key.Matches(secret, time.Now()) {
		return principal{}, false
	}
	return principal{ns: e.ns, key: e.key}, true
}

func (s *Server) lookupKey(ctx context.Context, id string) (keyEntry, bool) {
	s.mu.Lock()
	e, ok := s.keys[id]
	stale := time.Since(s.loaded) > 5*time.Second || (!ok && time.Since(s.loaded) > time.Second)
	s.mu.Unlock()
	if !stale {
		return e, ok
	}
	if err := s.reloadKeys(ctx); err != nil {
		s.log.Error("runnel: loading keys failed", "err", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok = s.keys[id]
	return e, ok
}

func (s *Server) reloadKeys(ctx context.Context) error {
	all, err := runnel.Namespaces(ctx, s.store)
	if err != nil {
		return err
	}
	stored, err := runnel.Keys(ctx, s.store)
	if err != nil {
		return err
	}
	keys := map[string]keyEntry{}
	for id, k := range stored {
		if _, ok := all[k.Namespace]; ok {
			keys[id] = keyEntry{ns: k.Namespace, key: k}
		}
	}
	s.mu.Lock()
	s.keys, s.loaded = keys, time.Now()
	s.mu.Unlock()
	return nil
}

func (s *Server) namespaceNames(ctx context.Context) ([]string, error) {
	all, err := runnel.Namespaces(ctx, s.store)
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(all)), nil
}

func (s *Server) source(ns string) runnel.Source {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.sources[ns]
	if src == nil {
		src = runnel.NewStoreSource(s.store, ns)
		s.sources[ns] = src
	}
	return src
}

func (s *Server) client(ns string) *runnel.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.clients[ns]
	if c == nil {
		c = runnel.NewClient(s.store, ns).WithLimits(s.cfg.Limits)
		s.clients[ns] = c
	}
	return c
}

type problemBody struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func (s *Server) problem(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemBody{Type: "about:blank", Title: title, Status: status, Detail: detail})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		s.problem(w, http.StatusRequestEntityTooLarge, "too large", "request body too large")
	case errors.Is(err, runnel.ErrInvalid):
		s.problem(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, runnel.ErrNotFound):
		s.problem(w, http.StatusNotFound, "not found", "")
	case errors.Is(err, runnel.ErrTenantFull):
		w.Header().Set("Retry-After", s.retryAfter())
		s.problem(w, http.StatusTooManyRequests, "tenant full", err.Error())
	case errors.Is(err, runnel.ErrQueueFull):
		w.Header().Set("Retry-After", s.retryAfter())
		s.problem(w, http.StatusTooManyRequests, "queue full", err.Error())
	case errors.Is(err, runnel.ErrDependency):
		s.problem(w, http.StatusUnprocessableEntity, "dependency", err.Error())
	case errors.Is(err, runnel.ErrConflict), errors.Is(err, runnel.ErrFinal), errors.Is(err, runnel.ErrNotFinal):
		s.problem(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, context.Canceled):
		s.problem(w, 499, "cancelled", "")
	default:
		s.log.Error("runnel: request failed", "err", err)
		s.problem(w, http.StatusInternalServerError, "internal error", "")
	}
}

func (s *Server) retryAfter() string {
	return strconv.Itoa(int(max(1, s.cfg.RetryAfter.Round(time.Second)/time.Second)))
}

func (s *Server) preferWait(r *http.Request) time.Duration {
	for _, h := range r.Header.Values("Prefer") {
		for _, part := range strings.FieldsFunc(h, func(c rune) bool { return c == ',' || c == ';' }) {
			v, ok := strings.CutPrefix(strings.TrimSpace(part), "wait=")
			if !ok {
				continue
			}
			n, err := strconv.Atoi(strings.Trim(v, `"`))
			if err != nil || n <= 0 {
				return 0
			}
			if most := int(s.cfg.MaxResultWait / time.Second); n > most {
				return s.cfg.MaxResultWait
			}
			return time.Duration(n) * time.Second
		}
	}
	return 0
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return err
		}
		return fmt.Errorf("%w: body: %v", runnel.ErrInvalid, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: body must hold one JSON value", runnel.ErrInvalid)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data after body", runnel.ErrInvalid)
	}
	return nil
}

func (s *Server) checkWebhook(u string) error {
	if err := runnel.ValidWebhookURL(u, s.cfg.Limits.MaxWebhookURL); err != nil {
		return err
	}
	if !s.cfg.AllowHTTPWebhooks && !strings.HasPrefix(u, "https://") {
		return fmt.Errorf("%w: webhook must use https", runnel.ErrInvalid)
	}
	return nil
}

var workerPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,100}$`)

func workerID(p principal, raw string) (string, error) {
	if !workerPattern.MatchString(raw) {
		return "", fmt.Errorf("%w: worker must match %s", runnel.ErrInvalid, workerPattern)
	}
	return p.id() + ":" + raw, nil
}
