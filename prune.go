package runnel

import (
	"context"
	"time"
)

const maxPruneRounds = 20

func (m *Maintainer) prune(ctx context.Context, ns string, now time.Time) error {
	if err := m.pruneEvents(ctx, ns, now); err != nil {
		return err
	}
	stats, err := m.store.Stats(ctx, ns)
	if err != nil {
		return err
	}
	queues, err := LoadTyped[QueueConfig](ctx, m.store, ns, ConfigQueue)
	if err != nil {
		return err
	}
	for _, st := range stats {
		q := queues[st.Queue]
		plan := map[State]time.Duration{
			StateCompleted: pick(q.Retention.D(), m.cfg.Retention.Completed),
			StateCancelled: pick(q.Retention.D(), m.cfg.Retention.Cancelled),
			StateDead:      pick(q.DeadRetention.D(), m.cfg.Retention.Dead),
			StateExpired:   pick(q.DeadRetention.D(), m.cfg.Retention.Dead),
		}
		for state, keep := range plan {
			if keep <= 0 || st.Counts[state] == 0 {
				continue
			}
			for range maxPruneRounds {
				n, err := m.store.Prune(ctx, ns, PruneRequest{Queue: st.Queue, States: []State{state}, Before: now.Add(-keep),
					Limit: m.cfg.Batch, Archive: m.cfg.Archive})
				if err != nil {
					return err
				}
				if n < m.cfg.Batch {
					break
				}
			}
		}
	}
	return nil
}

func (m *Maintainer) pruneEvents(ctx context.Context, ns string, now time.Time) error {
	keep := EventPruneRequest{Before: now.Add(-m.cfg.Retention.Events), Limit: m.cfg.Batch}
	if m.cfg.Callbacks.Enabled {
		cur, err := m.store.Cursor(ctx, ns, "callbacks")
		if err != nil {
			return err
		}
		if cur == 0 {
			keep.Before = time.Time{}
		}
		keep.UpTo = cur
	}
	if _, err := m.drainEvents(ctx, ns, keep); err != nil {
		return err
	}
	if !m.cfg.Callbacks.Enabled {
		return nil
	}
	lost, err := m.drainEvents(ctx, ns, EventPruneRequest{Before: now.Add(-m.cfg.Retention.Undelivered), Limit: m.cfg.Batch})
	if lost > 0 {
		m.log.Warn("runnel: dropped undelivered callback events past retention", "namespace", ns, "events", lost,
			"retention", m.cfg.Retention.Undelivered)
	}
	return err
}

func (m *Maintainer) drainEvents(ctx context.Context, ns string, req EventPruneRequest) (int, error) {
	total := 0
	for range maxPruneRounds {
		n, err := m.store.PruneEvents(ctx, ns, req)
		total += n
		if err != nil || n < req.Limit {
			return total, err
		}
	}
	return total, nil
}
