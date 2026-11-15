package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/kinoforge/runnel"
)

const claimPollEvery = 250 * time.Millisecond

func (s *Server) claim(w http.ResponseWriter, r *http.Request, p principal) error {
	var body struct {
		Worker string          `json:"worker"`
		Queues []string        `json:"queues"`
		Limit  int             `json:"limit"`
		Lease  runnel.Duration `json:"lease"`
		Wait   runnel.Duration `json:"wait"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	worker, err := workerID(p, body.Worker)
	if err != nil {
		return err
	}
	jobs, err := s.claimJobs(r.Context(), claimArgs{ns: r.PathValue("ns"), worker: worker, queues: body.Queues, limit: body.Limit, lease: body.Lease.D(), wait: body.Wait.D()})
	if err != nil {
		return err
	}
	if jobs == nil {
		jobs = []*runnel.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "now": s.clock.Now()})
	return nil
}

type claimArgs struct {
	ns, worker  string
	queues      []string
	limit       int
	lease, wait time.Duration
}

func (s *Server) claimJobs(ctx context.Context, a claimArgs) ([]*runnel.Job, error) {
	if len(a.queues) == 0 || len(a.queues) > s.cfg.MaxClaimQueues {
		return nil, fmt.Errorf("%w: claim 1 to %d queues", runnel.ErrInvalid, s.cfg.MaxClaimQueues)
	}
	for _, q := range a.queues {
		if !runnel.ValidName(q) {
			return nil, fmt.Errorf("%w: bad queue %q", runnel.ErrInvalid, q)
		}
	}
	limit := min(max(a.limit, 1), s.cfg.MaxClaim)
	lease := s.clampLease(a.lease)
	if a.lease == 0 {
		lease = s.cfg.DefaultLease
	}
	deadline := time.Now().Add(min(max(a.wait, 0), s.cfg.MaxClaimWait))
	h := s.hub(a.ns)
	for {
		wake := h.wait()
		jobs, err := s.store.Claim(ctx, a.ns, runnel.ClaimRequest{Queues: a.queues, Worker: a.worker, Limit: limit, Lease: lease, Now: s.clock.Now()})
		if err != nil {
			return nil, err
		}
		if len(jobs) > 0 || !time.Now().Before(deadline) {
			for _, j := range jobs {
				s.obs.Started(j)
			}
			return jobs, nil
		}
		t := time.NewTimer(min(claimPollEvery, time.Until(deadline)))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-wake:
		case <-t.C:
		}
		t.Stop()
	}
}

func (s *Server) extend(w http.ResponseWriter, r *http.Request, p principal) error {
	var body struct {
		Worker string          `json:"worker"`
		IDs    []string        `json:"ids"`
		Lease  runnel.Duration `json:"lease"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	worker, err := workerID(p, body.Worker)
	if err != nil {
		return err
	}
	lost, err := s.extendJobs(r.Context(), r.PathValue("ns"), worker, body.IDs, body.Lease.D())
	if err != nil {
		return err
	}
	if lost == nil {
		lost = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"lost": lost, "now": s.clock.Now()})
	return nil
}

func (s *Server) finish(w http.ResponseWriter, r *http.Request, p principal) error {
	var body struct {
		Worker  string            `json:"worker"`
		Version int64             `json:"version"`
		Outcome runnel.Outcome    `json:"outcome"`
		Payload json.RawMessage   `json:"payload,omitempty"`
		Meta    map[string]string `json:"meta,omitempty"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	worker, err := workerID(p, body.Worker)
	if err != nil {
		return err
	}
	f := finishArgs{ns: r.PathValue("ns"), worker: worker, id: r.PathValue("id"), version: body.Version, outcome: body.Outcome, payload: body.Payload, meta: body.Meta}
	if err := s.finishJob(r.Context(), f); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	return nil
}

func (s *Server) extendJobs(ctx context.Context, ns, worker string, ids []string, lease time.Duration) ([]string, error) {
	if len(ids) > s.cfg.MaxExtend {
		return nil, fmt.Errorf("%w: at most %d ids", runnel.ErrInvalid, s.cfg.MaxExtend)
	}
	for _, id := range ids {
		if !runnel.ValidID(id) {
			return nil, fmt.Errorf("%w: bad id", runnel.ErrInvalid)
		}
	}
	return s.store.Extend(ctx, ns, worker, ids, s.clock.Now().Add(s.clampLease(lease)))
}

type finishArgs struct {
	ns, worker, id string
	version        int64
	outcome        runnel.Outcome
	payload        json.RawMessage
	meta           map[string]string
}

func (s *Server) finishJob(ctx context.Context, f finishArgs) error {
	if err := f.outcome.Validate(); err != nil {
		return err
	}
	j, err := s.client(f.ns).Get(ctx, f.id)
	if err != nil {
		return err
	}
	if j.State != runnel.StateRunning || j.Worker != f.worker || j.Version != f.version {
		return runnel.ErrConflict
	}
	if f.payload != nil {
		if len(f.payload) > s.cfg.Limits.MaxPayload || !json.Valid(f.payload) {
			return fmt.Errorf("%w: payload must be valid JSON under the size limit", runnel.ErrInvalid)
		}
		j.Payload = f.payload
	}
	if f.meta != nil {
		if err := runnel.ValidMeta(f.meta, s.cfg.Limits); err != nil {
			return err
		}
		j.Meta = f.meta
	}
	if err := s.source(f.ns).Finish(ctx, f.worker, j, f.outcome); err != nil {
		return err
	}
	s.obs.Finished(j, f.outcome, 0)
	return nil
}

func (s *Server) dependencies(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if len(body.IDs) > s.cfg.Limits.MaxDependsOn {
		return fmt.Errorf("%w: at most %d ids", runnel.ErrInvalid, s.cfg.Limits.MaxDependsOn)
	}
	jobs, err := s.store.Get(r.Context(), r.PathValue("ns"), body.IDs)
	if err != nil {
		return err
	}
	if jobs == nil {
		jobs = []*runnel.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
	return nil
}

func (s *Server) clampLease(d time.Duration) time.Duration {
	return min(max(d, s.cfg.MinLease), s.cfg.MaxLease)
}
