package server

import (
	"net/http"

	"github.com/kinoforge/runnel"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.cfg.Registry, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openAPI)
	})
	s.patterns = append(s.patterns, "GET /healthz", "GET /readyz", "GET /metrics", "GET /openapi.yaml")
	s.handle("GET /v1/backup", "", s.backup)
	s.handle("GET /v1/system", "", s.system)

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

	configRoutes(s, "queues", runnel.ConfigQueue, s.checkQueue)
	configRoutes(s, "pools", runnel.ConfigPool, s.checkPool)
	configRoutes(s, "schedules", runnel.ConfigSchedule, s.checkSchedule)
	s.mountRPC()
}

func (s *Server) checkQueue(c runnel.QueueConfig) (string, error) {
	return c.Name, s.withCallback(c.CallbackURL, c.Validate)
}

func (s *Server) checkPool(c runnel.PoolConfig) (string, error) { return c.Name, c.Validate() }

func (s *Server) checkSchedule(c runnel.Schedule) (string, error) {
	return c.Name, s.withCallback(c.CallbackURL, c.Validate)
}
