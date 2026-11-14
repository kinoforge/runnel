package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	maxDelayDays   = 365
	maxEnqueueJobs = 1000
)

// JobRequest is the JSON body that enqueues a job.
type JobRequest struct {
	Queue        string            `json:"queue"`
	Kind         string            `json:"kind"`
	Payload      json.RawMessage   `json:"payload,omitempty"`
	Priority     int               `json:"priority,omitempty"`
	Group        string            `json:"group,omitempty"`
	Pool         string            `json:"pool,omitempty"`
	RunAt        time.Time         `json:"run_at,omitzero"`
	Delay        runnel.Duration   `json:"delay,omitempty"`
	MaxAttempts  int               `json:"max_attempts,omitempty"`
	UniqueKey    string            `json:"unique_key,omitempty"`
	UniqueFor    runnel.Duration   `json:"unique_for,omitempty"`
	StartTimeout runnel.Duration   `json:"start_timeout,omitempty"`
	Deadline     time.Time         `json:"deadline,omitzero"`
	TTL          runnel.Duration   `json:"ttl,omitempty"`
	Timeout      runnel.Duration   `json:"timeout,omitempty"`
	Backoff      runnel.Backoff    `json:"backoff,omitzero"`
	DependsOn    []string          `json:"depends_on,omitempty"`
	CallbackURL  string            `json:"callback_url,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
}

func (s *Server) toJob(req JobRequest) (*runnel.Job, error) {
	if req.CallbackURL != "" {
		if err := s.checkCallback(req.CallbackURL); err != nil {
			return nil, err
		}
	}
	if req.Delay < 0 || req.Delay.D() > maxDelayDays*24*time.Hour || req.TTL < 0 || req.TTL.D() > maxDelayDays*24*time.Hour {
		return nil, fmt.Errorf("%w: delay and ttl must be between 0 and %d days", runnel.ErrInvalid, maxDelayDays)
	}
	if req.TTL > 0 && !req.Deadline.IsZero() {
		return nil, fmt.Errorf("%w: set deadline or ttl, not both", runnel.ErrInvalid)
	}
	if req.Queue == runnel.CallbackQueue || req.Kind == runnel.CallbackKind {
		return nil, fmt.Errorf("%w: %s is reserved", runnel.ErrInvalid, runnel.CallbackQueue)
	}
	j := &runnel.Job{Queue: req.Queue, Kind: req.Kind, Payload: req.Payload, Priority: req.Priority, Group: req.Group,
		Pool: req.Pool, RunAt: req.RunAt, MaxAttempts: req.MaxAttempts, UniqueKey: req.UniqueKey, UniqueFor: req.UniqueFor,
		StartTimeout: req.StartTimeout, Deadline: req.Deadline, Timeout: req.Timeout, Backoff: req.Backoff,
		DependsOn: req.DependsOn, CallbackURL: req.CallbackURL, Meta: req.Meta}
	if req.Delay > 0 {
		runnel.In(req.Delay.D())(j)
	}
	if req.TTL > 0 {
		runnel.TTL(req.TTL.D())(j)
	}
	return j, nil
}

type insertView struct {
	ID        string `json:"id,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Error     string `json:"error,omitempty"`
}

func viewInserted(in runnel.Inserted) insertView {
	v := insertView{ID: in.ID, Duplicate: in.Duplicate}
	if in.Err != nil {
		v.ID, v.Error = "", in.Err.Error()
	}
	return v
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request, _ principal) error {
	var raw json.RawMessage
	if err := decode(r, &raw); err != nil {
		return err
	}
	var batch struct {
		Jobs []JobRequest `json:"jobs"`
	}
	single := true
	if probe := struct {
		Jobs json.RawMessage `json:"jobs"`
	}{}; json.Unmarshal(raw, &probe) == nil && len(probe.Jobs) > 0 {
		single = false
		if err := strictUnmarshal(raw, &batch); err != nil {
			return err
		}
	} else {
		var one JobRequest
		if err := strictUnmarshal(raw, &one); err != nil {
			return err
		}
		batch.Jobs = []JobRequest{one}
	}
	if len(batch.Jobs) == 0 || len(batch.Jobs) > maxEnqueueJobs {
		return fmt.Errorf("%w: send 1 to %d jobs", runnel.ErrInvalid, maxEnqueueJobs)
	}
	jobs := make([]*runnel.Job, len(batch.Jobs))
	for i, req := range batch.Jobs {
		j, err := s.toJob(req)
		if err != nil {
			return fmt.Errorf("job %d: %w", i, err)
		}
		s.carryTrace(r, j)
		jobs[i] = j
	}
	ns := r.PathValue("ns")
	res, err := s.client(ns).EnqueueJobs(r.Context(), jobs)
	if err != nil {
		return err
	}
	if single {
		if res[0].Err != nil {
			return res[0].Err
		}
		w.Header().Set("Location", jobPath(ns, res[0].ID))
		if wait := s.enqueueWait(r, ns, jobs[0].Queue); wait > 0 {
			return s.answerWhenDone(w, r, ns, res[0], wait)
		}
		status := http.StatusCreated
		if res[0].Duplicate {
			status = http.StatusOK
		}
		writeJSON(w, status, viewInserted(res[0]))
		return nil
	}
	out := make([]insertView, len(res))
	for i, in := range res {
		out[i] = viewInserted(in)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": out})
	return nil
}

func jobPath(ns, id string) string { return "/v1/ns/" + ns + "/jobs/" + id }

func (s *Server) answerWhenDone(w http.ResponseWriter, r *http.Request, ns string, in runnel.Inserted, wait time.Duration) error {
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	j, err := s.client(ns).Wait(ctx, in.ID)
	if r.Context().Err() != nil {
		return r.Context().Err()
	}
	w.Header().Set("Preference-Applied", "wait="+strconv.Itoa(int(wait/time.Second)))
	if err == nil {
		writeJSON(w, http.StatusOK, j)
		return nil
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	w.Header().Set("Retry-After", s.retryAfter())
	writeJSON(w, http.StatusAccepted, viewInserted(in))
	return nil
}

func strictUnmarshal(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: body: %w", runnel.ErrInvalid, err)
	}
	return nil
}

func (s *Server) enqueueFlow(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		Steps []struct {
			Name  string   `json:"name"`
			After []string `json:"after,omitempty"`
			JobRequest
		} `json:"steps"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	f := runnel.NewFlow()
	for _, st := range body.Steps {
		j, err := s.toJob(st.JobRequest)
		if err != nil {
			return fmt.Errorf("step %q: %w", st.Name, err)
		}
		s.carryTrace(r, j)
		f.Add(st.Name, j, st.After...)
	}
	res, err := s.client(r.PathValue("ns")).EnqueueFlow(r.Context(), f)
	if err != nil {
		return err
	}
	out := make(map[string]insertView, len(res))
	for name, in := range res {
		out[name] = viewInserted(in)
	}
	writeJSON(w, http.StatusOK, map[string]any{"steps": out})
	return nil
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, _ principal) error {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	jobs, next, err := s.client(r.PathValue("ns")).List(r.Context(), runnel.Query{Queue: q.Get("queue"), Kind: q.Get("kind"),
		Group: q.Get("group"), State: runnel.State(q.Get("state")), Cursor: q.Get("cursor"), Limit: limit})
	if err != nil {
		return err
	}
	if jobs == nil {
		jobs = []*runnel.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": jobs, "next": next})
	return nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, _ principal) error {
	c := s.client(r.PathValue("ns"))
	j, err := c.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	if wait, _ := s.preferWait(r); wait > 0 && !j.State.Final() {
		ctx, cancel := context.WithTimeout(r.Context(), wait)
		defer cancel()
		latest, err := c.Wait(ctx, j.ID)
		if r.Context().Err() != nil {
			return r.Context().Err()
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if latest != nil {
			j = latest
		}
		w.Header().Set("Preference-Applied", "wait="+strconv.Itoa(int(wait/time.Second)))
	}
	writeJSON(w, http.StatusOK, j)
	return nil
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request, _ principal) error {
	j, err := s.client(r.PathValue("ns")).Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, j)
	return nil
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request, _ principal) error {
	j, err := s.client(r.PathValue("ns")).Retry(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, j)
	return nil
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request, _ principal) error {
	st, err := s.client(r.PathValue("ns")).Stats(r.Context())
	if err != nil {
		return err
	}
	if st == nil {
		st = []runnel.QueueStats{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": st})
	return nil
}

func (s *Server) events(w http.ResponseWriter, r *http.Request, _ principal) error {
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	evs, err := s.client(r.PathValue("ns")).Events(r.Context(), after, limit)
	if err != nil {
		return err
	}
	if evs == nil {
		evs = []runnel.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": evs})
	return nil
}
