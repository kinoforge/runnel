package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	maxClaimQueues   = 64
	maxClaimLimit    = 100
	minLease         = 5 * time.Second
	maxLease         = 10 * time.Minute
	defaultLease     = 30 * time.Second
	claimPollEvery   = 250 * time.Millisecond
	maxExtendIDs     = 1000
	maxDependencyIDs = 256
)

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
	if len(body.Queues) == 0 || len(body.Queues) > maxClaimQueues {
		return fmt.Errorf("%w: claim 1 to %d queues", runnel.ErrInvalid, maxClaimQueues)
	}
	for _, q := range body.Queues {
		if !runnel.ValidName(q) {
			return fmt.Errorf("%w: bad queue %q", runnel.ErrInvalid, q)
		}
	}
	body.Limit = min(max(body.Limit, 1), maxClaimLimit)
	lease := clampLease(body.Lease.D())
	if body.Lease == 0 {
		lease = defaultLease
	}
	deadline := time.Now().Add(min(max(body.Wait.D(), 0), s.cfg.MaxClaimWait))
	ns := r.PathValue("ns")
	h := s.hub(ns)
	for {
		wake := h.wait()
		jobs, err := s.store.Claim(r.Context(), ns, runnel.ClaimRequest{Queues: body.Queues, Worker: worker, Limit: body.Limit, Lease: lease, Now: s.clock.Now()})
		if err != nil {
			return err
		}
		if len(jobs) > 0 || !time.Now().Before(deadline) {
			for _, j := range jobs {
				s.obs.Started(j)
			}
			if jobs == nil {
				jobs = []*runnel.Job{}
			}
			writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
			return nil
		}
		t := time.NewTimer(min(claimPollEvery, time.Until(deadline)))
		select {
		case <-r.Context().Done():
			t.Stop()
			return r.Context().Err()
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
	if len(body.IDs) > maxExtendIDs {
		return fmt.Errorf("%w: at most %d ids", runnel.ErrInvalid, maxExtendIDs)
	}
	for _, id := range body.IDs {
		if !runnel.ValidID(id) {
			return fmt.Errorf("%w: bad id", runnel.ErrInvalid)
		}
	}
	lease := clampLease(body.Lease.D())
	lost, err := s.store.Extend(r.Context(), r.PathValue("ns"), worker, body.IDs, s.clock.Now().Add(lease))
	if err != nil {
		return err
	}
	if lost == nil {
		lost = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"lost": lost})
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
	if err := body.Outcome.Validate(); err != nil {
		return err
	}
	ns := r.PathValue("ns")
	j, err := s.client(ns).Get(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	if j.State != runnel.StateRunning || j.Worker != worker || j.Version != body.Version {
		return runnel.ErrConflict
	}
	if body.Payload != nil {
		if len(body.Payload) > s.cfg.Limits.MaxPayload || !json.Valid(body.Payload) {
			return fmt.Errorf("%w: payload must be valid JSON under the size limit", runnel.ErrInvalid)
		}
		j.Payload = body.Payload
	}
	if body.Meta != nil {
		if err := runnel.ValidMeta(body.Meta, s.cfg.Limits); err != nil {
			return err
		}
		j.Meta = body.Meta
	}
	if err := s.source(ns).Finish(r.Context(), worker, j, body.Outcome); err != nil {
		return err
	}
	s.obs.Finished(j, body.Outcome, 0)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	return nil
}

func (s *Server) dependencies(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if len(body.IDs) > maxDependencyIDs {
		return fmt.Errorf("%w: at most %d ids", runnel.ErrInvalid, maxDependencyIDs)
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

func clampLease(d time.Duration) time.Duration { return min(max(d, minLease), maxLease) }
