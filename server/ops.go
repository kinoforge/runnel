package server

import (
	"context"
	"net/http"
	"time"

	"github.com/kinoforge/runnel"
)

// Drain makes /readyz answer 503, so load balancers stop sending requests
// before the server shuts down. Requests still get answers.
func (s *Server) Drain() { s.draining.Store(true) }

func (s *Server) storeAnswers(r *http.Request) bool {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.HealthTimeout)
	defer cancel()
	_, err := s.store.Configs(ctx, "", runnel.ConfigNamespace)
	return err == nil
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if !s.storeAnswers(r) {
		s.problem(w, http.StatusServiceUnavailable, "unavailable", "store unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		s.problem(w, http.StatusServiceUnavailable, "unavailable", "shutting down")
		return
	}
	s.healthz(w, r)
}

// backup streams a copy of the whole store as a tar file. A failure after
// the status is sent cuts the connection, so the client knows the copy is
// incomplete.
func (s *Server) backup(w http.ResponseWriter, r *http.Request, _ principal) error {
	b, ok := s.store.(runnel.Backuper)
	if !ok {
		s.problem(w, http.StatusNotImplemented, "not implemented", "this store has no online backup; use the database's own backup tools")
		return nil
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", `attachment; filename="runnel-`+s.clock.Now().UTC().Format("20060102T150405Z")+`.tar"`)
	if err := b.Backup(r.Context(), w); err != nil {
		s.log.Error("backup failed", "err", err)
		panic(http.ErrAbortHandler)
	}
	return nil
}
