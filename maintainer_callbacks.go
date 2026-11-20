package runnel

import (
	"context"
	"fmt"
	"time"
)

const defaultCallbackRoom = 64 << 10

func (m *Maintainer) ensureHookWorker(ctx context.Context, ns string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.hooks[ns]; ok {
		return
	}
	hctx, cancel := context.WithCancel(ctx)
	m.hooks[ns] = cancel
	w := NewWorker(m.store, ns, WorkerConfig{Queues: []string{m.cfg.Callbacks.Queue}, Concurrency: m.cfg.Callbacks.Concurrency, Logger: m.log})
	w.Handle(CallbackKind, NewCallbackHandler(m.store, m.cfg.Callbacks))
	m.wg.Go(func() {
		if err := w.Run(hctx); err != nil {
			m.log.Error("runnel: callback worker stopped", "namespace", ns, "err", err)
		}
	})
}

func (m *Maintainer) stopHooks() {
	m.mu.Lock()
	for ns, cancel := range m.hooks {
		cancel()
		delete(m.hooks, ns)
	}
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *Maintainer) hookLimits() Limits {
	l := DefaultLimits
	l.MaxPayload = m.cfg.Callbacks.MaxPayload
	return l
}

func (m *Maintainer) hookClient(ns string) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.hookCli[ns]
	if c == nil {
		c = NewClient(m.store, ns).WithLimits(m.hookLimits())
		m.hookCli[ns] = c
	}
	return c
}

func (m *Maintainer) hookFor(ns string, j *Job, e Event) (*Job, error) {
	event := "job." + string(e.State)
	hook, err := NewCallbackJob(j, event, e.Seq, m.cfg.Callbacks)
	if err != nil {
		return nil, err
	}
	if len(hook.Payload) > m.cfg.Callbacks.MaxPayload && j.Result != nil {
		trimmed := j.Clone()
		trimmed.Result = nil
		if hook, err = NewCallbackJob(trimmed, event, e.Seq, m.cfg.Callbacks); err != nil {
			return nil, err
		}
		m.log.Warn("runnel: callback sent without the job's result, which is too large", "namespace", ns, "job", j.ID, "event", e.Seq)
	}
	if err := hook.validate(m.hookLimits()); err != nil {
		m.log.Error("runnel: callback dropped, its delivery is invalid", "namespace", ns, "job", j.ID, "event", e.Seq, "err", err)
		return nil, nil
	}
	return hook, nil
}

func (m *Maintainer) callbacks(ctx context.Context, ns string, now time.Time) error {
	cur, err := m.store.Cursor(ctx, ns, "callbacks")
	if err != nil {
		return err
	}
	events, err := m.store.Events(ctx, ns, cur, m.cfg.Batch)
	if err != nil || len(events) == 0 {
		return err
	}
	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.JobID)
	}
	found, err := m.store.Get(ctx, ns, ids)
	if err != nil {
		return err
	}
	byID := make(map[string]*Job, len(found))
	for _, j := range found {
		byID[j.ID] = j
	}
	var hooks []*Job
	var seqs []int64
	for _, e := range events {
		j := byID[e.JobID]
		if j == nil || j.CallbackURL == "" || j.Queue == m.cfg.Callbacks.Queue {
			continue
		}
		hook, err := m.hookFor(ns, j, e)
		if err != nil {
			return err
		}
		if hook != nil {
			hooks, seqs = append(hooks, hook), append(seqs, e.Seq)
		}
	}
	upTo, failed := events[len(events)-1].Seq, error(nil)
	if len(hooks) > 0 {
		res, err := m.hookClient(ns).EnqueueJobs(ctx, hooks)
		if err != nil {
			return err
		}
		for i, r := range res {
			if r.Err != nil {
				upTo, failed = seqs[i]-1, r.Err
				break
			}
		}
	}
	if upTo > cur {
		if _, err := m.store.SwapCursor(ctx, ns, "callbacks", cur, upTo); err != nil {
			return err
		}
	}
	if failed != nil {
		return fmt.Errorf("runnel: callback delivery not queued: %w", failed)
	}
	return nil
}
