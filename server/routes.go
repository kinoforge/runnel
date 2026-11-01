package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	healthTimeout = 2 * time.Second
)

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
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
