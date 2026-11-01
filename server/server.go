package server

import (
	"crypto/sha256"
	_ "embed"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultMaxBody       = 4 << 20
	defaultMaxClaimWait  = 30 * time.Second
	defaultMaxResultWait = 60 * time.Second
	defaultRetryAfter    = time.Second
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

// New returns a server over store.
func New(store runnel.Store, cfg Config) *Server {
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = defaultMaxBody
	}
	if cfg.Limits == (runnel.Limits{}) {
		cfg.Limits = runnel.DefaultLimits
	}
	if cfg.MaxClaimWait <= 0 {
		cfg.MaxClaimWait = defaultMaxClaimWait
	}
	if cfg.MaxResultWait <= 0 {
		cfg.MaxResultWait = defaultMaxResultWait
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = defaultRetryAfter
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
