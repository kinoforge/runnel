package server

import (
	"crypto/sha256"
	_ "embed"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

//go:embed openapi.yaml
var openAPI []byte

type Config struct {
	AdminToken         string
	Version            string
	MaxBody            int64
	Limits             runnel.Limits
	AllowHTTPCallbacks bool
	MaxClaimWait       time.Duration
	MaxResultWait      time.Duration
	RetryAfter         time.Duration
	MaxBatch           int
	MaxDelay           time.Duration
	MaxClaimQueues     int
	MaxClaim           int
	DefaultLease       time.Duration
	MinLease           time.Duration
	MaxLease           time.Duration
	MaxExtend          int
	KeyCacheTTL        time.Duration
	HealthTimeout      time.Duration
	Logger             *slog.Logger
	Registry           *prometheus.Registry
}

var DefaultConfig = Config{
	MaxBody: 4 << 20, Limits: runnel.DefaultLimits, MaxClaimWait: 30 * time.Second, MaxResultWait: 60 * time.Second,
	RetryAfter: time.Second, MaxBatch: 1000, MaxDelay: 365 * 24 * time.Hour, MaxClaimQueues: 64, MaxClaim: 100,
	DefaultLease: 30 * time.Second, MinLease: 5 * time.Second, MaxLease: 10 * time.Minute, MaxExtend: 10000,
	KeyCacheTTL: 5 * time.Second, HealthTimeout: 2 * time.Second,
}

func (c Config) withDefaults() Config {
	d := DefaultConfig
	c.MaxBody = or(c.MaxBody, d.MaxBody)
	if c.Limits == (runnel.Limits{}) {
		c.Limits = d.Limits
	}
	c.MaxClaimWait = or(c.MaxClaimWait, d.MaxClaimWait)
	c.MaxResultWait = or(c.MaxResultWait, d.MaxResultWait)
	c.RetryAfter = or(c.RetryAfter, d.RetryAfter)
	c.MaxBatch = or(c.MaxBatch, d.MaxBatch)
	c.MaxDelay = or(c.MaxDelay, d.MaxDelay)
	c.MaxClaimQueues = or(c.MaxClaimQueues, d.MaxClaimQueues)
	c.MaxClaim = or(c.MaxClaim, d.MaxClaim)
	c.DefaultLease = or(c.DefaultLease, d.DefaultLease)
	c.MinLease = or(c.MinLease, d.MinLease)
	c.MaxLease = or(c.MaxLease, d.MaxLease)
	c.MaxExtend = or(c.MaxExtend, d.MaxExtend)
	c.KeyCacheTTL = or(c.KeyCacheTTL, d.KeyCacheTTL)
	c.HealthTimeout = or(c.HealthTimeout, d.HealthTimeout)
	return c
}

func or[T int | int64 | time.Duration](v, fallback T) T {
	if v > 0 {
		return v
	}
	return fallback
}

// Server is the Runnel HTTP API: namespaces, keys, jobs, configuration and the
// worker lease protocol. It implements http.Handler.
type Server struct {
	store    runnel.Store
	clock    runnel.Clock
	cfg      Config
	log      *slog.Logger
	mux      *http.ServeMux
	obs      *metrics.Observer
	reqs     *prometheus.CounterVec
	admin    [32]byte
	started  time.Time
	hasRoot  bool
	draining atomic.Bool

	patterns []string

	mu      sync.Mutex
	keys    map[string]keyEntry
	loaded  time.Time
	clients map[string]*runnel.Client
	sources map[string]runnel.Source
	hubs    map[string]*hub
}

func New(store runnel.Store, cfg Config) *Server {
	cfg = cfg.withDefaults()
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registry == nil {
		cfg.Registry = prometheus.NewRegistry()
		cfg.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	}
	s := &Server{store: store, clock: runnel.ClockOf(store), started: time.Now().UTC(), cfg: cfg, log: cfg.Logger, mux: http.NewServeMux(), clients: map[string]*runnel.Client{}, sources: map[string]runnel.Source{}, hubs: map[string]*hub{}}
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
		rw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
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

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

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
