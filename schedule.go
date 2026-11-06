package runnel

import (
	"context"
	"encoding/json"
	"maps"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	maxCatchUp    = 1000
	cronUniqueFor = 24 * time.Hour
)

// Schedule enqueues a job on a cron spec. Each firing is enqueued once, even
// with many maintainers running.
type Schedule struct {
	Name        string            `json:"name"`
	Spec        string            `json:"spec"`
	Timezone    string            `json:"timezone,omitempty"`
	Paused      bool              `json:"paused,omitempty"`
	Queue       string            `json:"queue"`
	Kind        string            `json:"kind"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Group       string            `json:"group,omitempty"`
	Pool        string            `json:"pool,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Timeout     Duration          `json:"timeout,omitempty"`
	Fallbacks   []Target          `json:"fallbacks,omitempty"`
	CallbackURL string            `json:"callback_url,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// Validate checks the spec, timezone and the job the schedule makes.
func (s Schedule) Validate() error {
	if err := checkName("schedule", s.Name, false); err != nil {
		return err
	}
	if _, err := parseSpec(s.Spec, s.Timezone); err != nil {
		return invalid("schedule spec: %v", err)
	}
	probe := &Job{Queue: s.Queue, Kind: s.Kind, Payload: s.Payload, Priority: s.Priority, Group: s.Group,
		Pool: s.Pool, MaxAttempts: max(s.MaxAttempts, 1), Timeout: s.Timeout, Fallbacks: s.Fallbacks, CallbackURL: s.CallbackURL, Meta: s.Meta}
	return probe.validate(DefaultLimits)
}

func parseSpec(spec, tz string) (cron.Schedule, error) {
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, err
		}
		spec = "CRON_TZ=" + tz + " " + spec
	}
	return specParser.Parse(spec)
}

func (m *Maintainer) spec(s Schedule) (cron.Schedule, error) {
	key := s.Timezone + "\x00" + s.Spec
	m.mu.Lock()
	defer m.mu.Unlock()
	if sp, ok := m.specs[key]; ok {
		return sp, nil
	}
	sp, err := parseSpec(s.Spec, s.Timezone)
	if err != nil {
		return nil, err
	}
	m.specs[key] = sp
	return sp, nil
}

func (m *Maintainer) schedules(ctx context.Context, ns string, now time.Time) error {
	scheds, err := LoadTyped[Schedule](ctx, m.store, ns, ConfigSchedule)
	if err != nil {
		return err
	}
	for name, s := range scheds {
		if s.Paused {
			continue
		}
		sp, err := m.spec(s)
		if err != nil {
			m.log.Error("runnel: bad schedule", "namespace", ns, "schedule", name, "err", err)
			continue
		}
		cursor := "cron:" + name
		last, err := m.store.Cursor(ctx, ns, cursor)
		if err != nil {
			return err
		}
		if last == 0 {
			if _, err := m.store.SwapCursor(ctx, ns, cursor, 0, Millis(now)); err != nil {
				return err
			}
			continue
		}
		fire := sp.Next(FromMillis(last))
		if fire.IsZero() || fire.After(now) {
			continue
		}
		for range maxCatchUp {
			n := sp.Next(fire)
			if n.IsZero() || n.After(now) {
				break
			}
			fire = n
		}
		won, err := m.store.SwapCursor(ctx, ns, cursor, last, Millis(fire))
		if err != nil {
			return err
		}
		if !won {
			continue
		}
		j := &Job{Queue: s.Queue, Kind: s.Kind, Payload: s.Payload, Priority: s.Priority, Group: s.Group, Pool: s.Pool,
			MaxAttempts: s.MaxAttempts, Timeout: s.Timeout, Fallbacks: s.Fallbacks, CallbackURL: s.CallbackURL,
			UniqueKey: "cron:" + name + ":" + strconv.FormatInt(fire.Unix(), 10), UniqueFor: Duration(cronUniqueFor)}
		j.Meta = map[string]string{"schedule": name, "fire_at": fire.UTC().Format(time.RFC3339)}
		maps.Copy(j.Meta, s.Meta)
		if _, err := m.client(ns).EnqueueJobs(ctx, []*Job{j}); err != nil {
			m.log.Error("runnel: scheduled job rejected", "namespace", ns, "schedule", name, "err", err)
		}
	}
	return nil
}
