package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/kinoforge/runnel"
)

type namespaceView struct {
	Name      string              `json:"name"`
	HasSecret bool                `json:"has_webhook_secret"`
	Keys      []keyView           `json:"keys"`
	CreatedAt time.Time           `json:"created_at"`
	Stats     []runnel.QueueStats `json:"stats,omitempty"`
}

type keyView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	Roles     []string  `json:"roles"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

func viewNamespace(n runnel.Namespace, keys []runnel.APIKey) namespaceView {
	v := namespaceView{Name: n.Name, HasSecret: n.WebhookSecret != "", CreatedAt: n.CreatedAt, Keys: []keyView{}}
	for _, k := range keys {
		v.Keys = append(v.Keys, keyView{ID: k.ID, Name: k.Name, Roles: k.Roles, CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt})
	}
	return v
}

func keysByNamespace(all map[string]runnel.APIKey) map[string][]runnel.APIKey {
	out := map[string][]runnel.APIKey{}
	for _, k := range all {
		out[k.Namespace] = append(out[k.Namespace], k)
	}
	for _, list := range out {
		slices.SortFunc(list, func(a, b runnel.APIKey) int { return a.CreatedAt.Compare(b.CreatedAt) })
	}
	return out
}

func (s *Server) namespace(ctx context.Context, name string) (runnel.Namespace, error) {
	all, err := runnel.Namespaces(ctx, s.store)
	if err != nil {
		return runnel.Namespace{}, err
	}
	n, ok := all[name]
	if !ok {
		return runnel.Namespace{}, runnel.ErrNotFound
	}
	return n, nil
}

func (s *Server) listNamespaces(w http.ResponseWriter, r *http.Request, _ principal) error {
	all, err := runnel.Namespaces(r.Context(), s.store)
	if err != nil {
		return err
	}
	keys, err := runnel.Keys(r.Context(), s.store)
	if err != nil {
		return err
	}
	byNS := keysByNamespace(keys)
	out := make([]namespaceView, 0, len(all))
	for _, n := range all {
		out = append(out, viewNamespace(n, byNS[n.Name]))
	}
	slices.SortFunc(out, func(a, b namespaceView) int {
		if a.Name < b.Name {
			return -1
		}
		return 1
	})
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *Server) putNamespace(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		WebhookSecret *string `json:"webhook_secret"`
	}
	if r.ContentLength != 0 {
		if err := decode(r, &body); err != nil {
			return err
		}
	}
	name := r.PathValue("name")
	n, err := s.namespace(r.Context(), name)
	status := http.StatusOK
	if errors.Is(err, runnel.ErrNotFound) {
		n, status = runnel.Namespace{Name: name}, http.StatusCreated
	} else if err != nil {
		return err
	}
	if body.WebhookSecret != nil {
		n.WebhookSecret = *body.WebhookSecret
	}
	if err := runnel.PutNamespace(r.Context(), s.store, n); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	keys, err := runnel.NamespaceKeys(r.Context(), s.store, name)
	if err != nil {
		return err
	}
	writeJSON(w, status, viewNamespace(n, keys))
	return nil
}

func (s *Server) getNamespace(w http.ResponseWriter, r *http.Request, _ principal) error {
	n, err := s.namespace(r.Context(), r.PathValue("name"))
	if err != nil {
		return err
	}
	keys, err := runnel.NamespaceKeys(r.Context(), s.store, n.Name)
	if err != nil {
		return err
	}
	v := viewNamespace(n, keys)
	if v.Stats, err = s.store.Stats(r.Context(), n.Name); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, v)
	return nil
}

func (s *Server) deleteNamespace(w http.ResponseWriter, r *http.Request, _ principal) error {
	if err := runnel.DeleteNamespace(r.Context(), s.store, r.PathValue("name")); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		Name      string    `json:"name"`
		Roles     []string  `json:"roles"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	ns := r.PathValue("name")
	if _, err := s.namespace(r.Context(), ns); err != nil {
		return err
	}
	existing, err := runnel.NamespaceKeys(r.Context(), s.store, ns)
	if err != nil {
		return err
	}
	if len(existing) >= 100 {
		return fmt.Errorf("%w: at most 100 keys per namespace", runnel.ErrInvalid)
	}
	token, key, err := runnel.NewAPIKey(ns, body.Name, body.Roles, body.ExpiresAt)
	if err != nil {
		return err
	}
	if err := runnel.PutKey(r.Context(), s.store, key); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	writeJSON(w, http.StatusCreated, map[string]any{"id": key.ID, "key": token, "roles": key.Roles, "expires_at": key.ExpiresAt})
	return nil
}

func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request, _ principal) error {
	if err := runnel.DeleteKey(r.Context(), s.store, r.PathValue("name"), r.PathValue("id")); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// JobRequest is the JSON body that enqueues a job.
type JobRequest struct {
	Queue       string            `json:"queue"`
	Kind        string            `json:"kind"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Tenant      string            `json:"tenant,omitempty"`
	Pool        string            `json:"pool,omitempty"`
	RunAt       time.Time         `json:"run_at,omitzero"`
	Delay       runnel.Duration   `json:"delay,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	UniqueKey   string            `json:"unique_key,omitempty"`
	UniqueFor   runnel.Duration   `json:"unique_for,omitempty"`
	StartWithin runnel.Duration   `json:"start_within,omitempty"`
	Timeout     runnel.Duration   `json:"timeout,omitempty"`
	Backoff     runnel.Backoff    `json:"backoff,omitzero"`
	Fallbacks   []runnel.Target   `json:"fallbacks,omitempty"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Webhook     string            `json:"webhook,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

func (s *Server) toJob(req JobRequest) (*runnel.Job, error) {
	if req.Webhook != "" {
		if err := s.checkWebhook(req.Webhook); err != nil {
			return nil, err
		}
	}
	if req.Delay < 0 || req.Delay.D() > 365*24*time.Hour {
		return nil, fmt.Errorf("%w: delay must be between 0 and 365 days", runnel.ErrInvalid)
	}
	if req.Queue == runnel.WebhookQueue || req.Kind == runnel.WebhookKind {
		return nil, fmt.Errorf("%w: %s is reserved", runnel.ErrInvalid, runnel.WebhookQueue)
	}
	j := &runnel.Job{Queue: req.Queue, Kind: req.Kind, Payload: req.Payload, Priority: req.Priority, Tenant: req.Tenant,
		Pool: req.Pool, RunAt: req.RunAt, MaxAttempts: req.MaxAttempts, UniqueKey: req.UniqueKey, UniqueFor: req.UniqueFor,
		StartWithin: req.StartWithin, Timeout: req.Timeout, Backoff: req.Backoff, Fallbacks: req.Fallbacks,
		DependsOn: req.DependsOn, Webhook: req.Webhook, Meta: req.Meta}
	if req.Delay > 0 {
		runnel.In(req.Delay.D())(j)
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
	if len(batch.Jobs) == 0 || len(batch.Jobs) > 1000 {
		return fmt.Errorf("%w: send 1 to 1000 jobs", runnel.ErrInvalid)
	}
	jobs := make([]*runnel.Job, len(batch.Jobs))
	for i, req := range batch.Jobs {
		j, err := s.toJob(req)
		if err != nil {
			return fmt.Errorf("job %d: %w", i, err)
		}
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
		if wait := s.preferWait(r); wait > 0 {
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
		return fmt.Errorf("%w: body: %v", runnel.ErrInvalid, err)
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
		Tenant: q.Get("tenant"), State: runnel.State(q.Get("state")), Cursor: q.Get("cursor"), Limit: limit})
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
	if wait := s.preferWait(r); wait > 0 && !j.State.Final() {
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

type hub struct {
	mu sync.Mutex
	ch chan struct{}
}

func (h *hub) wait() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ch
}

func (h *hub) fire() {
	h.mu.Lock()
	close(h.ch)
	h.ch = make(chan struct{})
	h.mu.Unlock()
}

func (s *Server) hub(ns string) *hub {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hubs[ns]
	if h != nil {
		return h
	}
	h = &hub{ch: make(chan struct{})}
	s.hubs[ns] = h
	if wk, ok := s.store.(runnel.Waker); ok {
		go func() {
			ch, err := wk.Subscribe(context.Background(), ns)
			if err != nil {
				return
			}
			for range ch {
				h.fire()
			}
		}()
	}
	return h
}

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
	if len(body.Queues) == 0 || len(body.Queues) > 64 {
		return fmt.Errorf("%w: claim 1 to 64 queues", runnel.ErrInvalid)
	}
	for _, q := range body.Queues {
		if !runnel.ValidName(q) {
			return fmt.Errorf("%w: bad queue %q", runnel.ErrInvalid, q)
		}
	}
	body.Limit = min(max(body.Limit, 1), 100)
	lease := min(max(body.Lease.D(), 5*time.Second), 10*time.Minute)
	if body.Lease == 0 {
		lease = 30 * time.Second
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
		t := time.NewTimer(min(250*time.Millisecond, time.Until(deadline)))
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
	if len(body.IDs) > 1000 {
		return fmt.Errorf("%w: at most 1000 ids", runnel.ErrInvalid)
	}
	for _, id := range body.IDs {
		if !runnel.ValidID(id) {
			return fmt.Errorf("%w: bad id", runnel.ErrInvalid)
		}
	}
	lease := min(max(body.Lease.D(), 5*time.Second), 10*time.Minute)
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
	if len(body.IDs) > 256 {
		return fmt.Errorf("%w: at most 256 ids", runnel.ErrInvalid)
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
