package server

import (
	"net/http"

	"github.com/kinoforge/runnel"
)

func (s *Server) carryTrace(r *http.Request, j *runnel.Job) {
	parent := r.Header.Get(runnel.TraceParent)
	if parent == "" || j.Meta[runnel.TraceParent] != "" {
		return
	}
	add := map[string]string{runnel.TraceParent: parent}
	if state := r.Header.Get(runnel.TraceState); state != "" {
		add[runnel.TraceState] = state
	}
	if len(j.Meta)+len(add) > s.cfg.Limits.MaxMeta || runnel.ValidMeta(add, s.cfg.Limits) != nil {
		return
	}
	runnel.Meta(add)(j)
}
