package runnel

import (
	"context"
	"fmt"
	"slices"
)

// Do enqueues j and waits for it to finish, as Client.EnqueueAndWait does.
// When a slot of this worker and the job's limits allow, the first attempt
// runs at once on the calling goroutine instead of going through the claim
// loop. The job is an ordinary job: a failed attempt is retried by the
// workers like any other, and Do keeps waiting until it is final or ctx ends.
// The worker must be running, read from a store and serve the job's queue.
func (w *Worker) Do(ctx context.Context, j *Job) (*Job, error) {
	ss, ok := w.src.(*storeSource)
	if !ok {
		return nil, invalid("direct start needs a worker that reads from a store")
	}
	if _, ok := w.mux.handlers[j.Kind]; !ok {
		return nil, fmt.Errorf("%w %q", ErrNoHandler, j.Kind)
	}
	if !slices.Contains(w.cfg.Queues, j.Queue) {
		return nil, invalid("worker does not serve queue %q", j.Queue)
	}
	w.mu.Lock()
	base, wg, freed := w.base, w.wg, w.freed
	if wg != nil {
		wg.Add(1)
	}
	w.mu.Unlock()
	if base == nil {
		return nil, invalid("worker is not running")
	}
	c := ss.client()
	id, err := w.startDirect(ctx, ss, c, j, base, freed)
	wg.Done()
	if err != nil {
		return nil, err
	}
	return c.Wait(ctx, id)
}

func (w *Worker) startDirect(ctx context.Context, ss *storeSource, c *Client, j *Job, base context.Context, freed chan struct{}) (string, error) {
	res, err := c.EnqueueJobs(ctx, []*Job{j})
	if err != nil {
		return "", err
	}
	if res[0].Err != nil {
		return "", res[0].Err
	}
	if !res[0].Duplicate && base.Err() == nil && w.reserve(1) == 1 {
		got, err := ss.store.Claim(ctx, ss.ns, ClaimRequest{Queues: []string{j.Queue}, IDs: []string{res[0].ID}, Worker: w.cfg.ID,
			Limit: 1, Lease: w.cfg.Lease, Now: ss.clock.Now()})
		var r *inflight
		var jctx context.Context
		if err == nil && len(got) == 1 {
			r, jctx = w.track(base, got[0])
		}
		w.unreserve(1)
		if err != nil {
			return "", err
		}
		if r != nil {
			w.runOne(base, jctx, r, freed)
		}
	}
	return res[0].ID, nil
}
