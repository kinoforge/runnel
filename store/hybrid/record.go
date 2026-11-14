package hybrid

import (
	"time"

	"github.com/google/uuid"
	"github.com/kinoforge/runnel"
)

const idLen = 36

var idDigits = [16]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}

type uid [16]byte

// Only the canonical spelling is accepted, so an id always reads back as it was written.
func parseID(s string) (uid, bool) {
	var u uid
	if len(s) != idLen || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, false
	}
	for k, i := range idDigits {
		hi, ok1 := unhex(s[i])
		lo, ok2 := unhex(s[i+1])
		if !ok1 || !ok2 {
			return uid{}, false
		}
		u[k] = hi<<4 | lo
	}
	return u, true
}

func unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

func (u uid) String() string { return uuid.UUID(u).String() }

type sym uint32

type syms struct {
	ids   map[string]sym
	names []string
}

func newSyms() syms { return syms{ids: map[string]sym{}, names: []string{""}} }

func (t *syms) id(s string) sym {
	if s == "" {
		return 0
	}
	if v, ok := t.ids[s]; ok {
		return v
	}
	v := sym(len(t.names))
	t.names = append(t.names, s)
	t.ids[s] = v
	return v
}

func (t *syms) reuse(v sym, s string) sym {
	if t.names[v] == s {
		return v
	}
	return t.id(s)
}

func (t *syms) find(s string) (sym, bool) {
	if s == "" {
		return 0, true
	}
	v, ok := t.ids[s]
	return v, ok
}

func (t *syms) name(v sym) string { return t.names[v] }

const (
	stBlocked uint8 = iota + 1
	stWaiting
	stRunning
	stPolling
	stCompleted
	stDead
	stCancelled
	stExpired
	stCount
)

var stateNames = [stCount]runnel.State{"", runnel.StateBlocked, runnel.StateWaiting, runnel.StateRunning,
	runnel.StatePolling, runnel.StateCompleted, runnel.StateDead, runnel.StateCancelled, runnel.StateExpired}

func stateCode(s runnel.State) uint8 {
	for i, n := range stateNames {
		if i > 0 && n == s {
			return uint8(i)
		}
	}
	return 0
}

func final(st uint8) bool { return st >= stCompleted }

const (
	flagUnique uint8 = 1 << iota
	flagDeps
)

type ref struct {
	file uint32
	n    uint32
	off  int64
}

// No pointers, so the garbage collector never scans job records. Keep it that way.
type job struct {
	id                                  uid
	queue, kind, group, pool, worker    sym
	prio, attempt, maxAttempts, pending int32
	state, flags                        uint8
	runAt, waitingSince, startBy, lease int64
	created, updated, started, finished int64
	version, window, deadline           int64
	body                                ref
}

func ms(t time.Time) int64             { return runnel.Millis(t) }
func fromMs(v int64) time.Time         { return runnel.FromMillis(v) }
func (h *job) final() bool             { return final(h.state) }
func (h *job) is(st uint8) bool        { return h.state == st }
func (h *job) stateName() runnel.State { return stateNames[h.state] }

func (t *syms) hot(j *runnel.Job, like *job) job {
	var l job
	if like != nil {
		l = *like
	}
	h := job{queue: t.reuse(l.queue, j.Queue), kind: t.reuse(l.kind, j.Kind), group: t.reuse(l.group, j.Group),
		pool: t.reuse(l.pool, j.Pool), worker: t.reuse(l.worker, j.Worker),
		prio: int32(j.Priority), attempt: int32(j.Attempt), maxAttempts: int32(j.MaxAttempts), pending: int32(j.Pending),
		runAt: ms(j.RunAt), waitingSince: ms(j.WaitingSince), startBy: ms(j.StartBy), lease: ms(j.LeaseUntil),
		created: ms(j.CreatedAt), updated: ms(j.UpdatedAt), started: ms(j.StartedAt), finished: ms(j.FinishedAt),
		version: j.Version, window: j.StartTimeout.D().Milliseconds(), deadline: ms(j.Deadline), state: stateCode(j.State)}
	h.id, _ = parseID(j.ID)
	if j.UniqueKey != "" {
		h.flags |= flagUnique
	}
	if len(j.DependsOn) > 0 {
		h.flags |= flagDeps
	}
	return h
}

func (t *syms) full(ns string, h *job, body []byte) (*runnel.Job, error) {
	d := t.draft(h)
	d.body = body
	return d.job(ns)
}

type draft struct {
	h     job
	names [5]string
	body  []byte
}

func (t *syms) draft(h *job) draft { return draft{h: *h, names: t.names5(h)} }

func (t *syms) names5(h *job) [5]string {
	return [5]string{t.name(h.queue), t.name(h.kind), t.name(h.group), t.name(h.pool), t.name(h.worker)}
}

func (d *draft) job(ns string) (*runnel.Job, error) {
	h := &d.h
	j := &runnel.Job{ID: h.id.String(), Namespace: ns, Queue: d.names[0], Kind: d.names[1], Group: d.names[2],
		Pool: d.names[3], Worker: d.names[4], State: h.stateName(),
		Priority: int(h.prio), Attempt: int(h.attempt), MaxAttempts: int(h.maxAttempts), Pending: int(h.pending),
		RunAt: fromMs(h.runAt), WaitingSince: fromMs(h.waitingSince), StartBy: fromMs(h.startBy), Deadline: fromMs(h.deadline), LeaseUntil: fromMs(h.lease),
		CreatedAt: fromMs(h.created), UpdatedAt: fromMs(h.updated), StartedAt: fromMs(h.started), FinishedAt: fromMs(h.finished),
		Version: h.version}
	return j, decodeBody(d.body, j)
}
