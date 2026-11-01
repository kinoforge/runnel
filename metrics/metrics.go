package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kinoforge/runnel"
)

const (
	statsTTL     = 5 * time.Second
	statsTimeout = 5 * time.Second
)

// Observer counts attempts in Prometheus: runnel_jobs_started_total,
// runnel_jobs_finished_total by outcome and runnel_job_duration_seconds.
type Observer struct {
	started  *prometheus.CounterVec
	finished *prometheus.CounterVec
	took     *prometheus.HistogramVec
}

// NewObserver registers the attempt metrics with reg.
func NewObserver(reg prometheus.Registerer) *Observer {
	o := &Observer{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "runnel_jobs_started_total", Help: "Job attempts started."},
			[]string{"namespace", "queue", "kind"}),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "runnel_jobs_finished_total", Help: "Job attempts finished, by outcome."},
			[]string{"namespace", "queue", "kind", "outcome"}),
		took: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "runnel_job_duration_seconds", Help: "Attempt run time.",
			Buckets: prometheus.ExponentialBuckets(0.005, 3, 12)}, []string{"namespace", "queue", "kind"}),
	}
	reg.MustRegister(o.started, o.finished, o.took)
	return o
}

// Started counts a started attempt.
func (o *Observer) Started(j *runnel.Job) {
	o.started.WithLabelValues(j.Namespace, j.Queue, j.Kind).Inc()
}

// Finished counts a finished attempt and its run time.
func (o *Observer) Finished(j *runnel.Job, out runnel.Outcome, took time.Duration) {
	o.finished.WithLabelValues(j.Namespace, j.Queue, j.Kind, string(out.Kind)).Inc()
	if took > 0 {
		o.took.WithLabelValues(j.Namespace, j.Queue, j.Kind).Observe(took.Seconds())
	}
}

// StatsCollector exports job counts per namespace, queue and state as
// runnel_jobs, read from the store at scrape time.
type StatsCollector struct {
	store      runnel.Store
	namespaces func(context.Context) ([]string, error)
	desc       *prometheus.Desc

	mu     sync.Mutex
	cached []prometheus.Metric
	at     time.Time
}

// NewStatsCollector returns a collector over the namespaces names returns.
func NewStatsCollector(s runnel.Store, namespaces func(context.Context) ([]string, error)) *StatsCollector {
	return &StatsCollector{store: s, namespaces: namespaces,
		desc: prometheus.NewDesc("runnel_queue_jobs", "Jobs per queue and state.", []string{"namespace", "queue", "state"}, nil)}
}

// Describe implements prometheus.Collector.
func (c *StatsCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector.
func (c *StatsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.at) > statsTTL {
		c.cached = c.load()
		c.at = time.Now()
	}
	for _, m := range c.cached {
		ch <- m
	}
}

func (c *StatsCollector) load() []prometheus.Metric {
	ctx, cancel := context.WithTimeout(context.Background(), statsTimeout)
	defer cancel()
	nss, err := c.namespaces(ctx)
	if err != nil {
		return c.cached
	}
	var out []prometheus.Metric
	for _, ns := range nss {
		stats, err := c.store.Stats(ctx, ns)
		if err != nil {
			continue
		}
		for _, q := range stats {
			for _, st := range runnel.States {
				out = append(out, prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(q.Counts[st]), ns, q.Queue, string(st)))
			}
		}
	}
	return out
}
