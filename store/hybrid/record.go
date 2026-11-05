package hybrid

import (
	"time"

	"github.com/google/uuid"
	"github.com/kinoforge/runnel"
)

const idLen = 36

// uid is a job id in its 16 raw bytes, so ids hold no pointers.
type uid [16]byte

// parseID reads a job id in canonical form. Other spellings of the same UUID
// are refused, so an id always reads back as it was written.
func parseID(s string) (uid, bool) {
	if len(s) != idLen {
		return uid{}, false
	}
	u, err := uuid.Parse(s)
	if err != nil || u.String() != s {
		return uid{}, false
	}
	return uid(u), true
}

func (u uid) String() string { return uuid.UUID(u).String() }

// sym stands for a string the namespace has seen, such as a queue or tenant
// name. Zero is the empty string.
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

// find looks s up without adding it.
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
	stCount
)

var stateNames = [stCount]runnel.State{"", runnel.StateBlocked, runnel.StateWaiting, runnel.StateRunning,
	runnel.StatePolling, runnel.StateCompleted, runnel.StateDead, runnel.StateCancelled}

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

// ref says where a job's body lies in the log or a snapshot.
type ref struct {
	file uint32
	n    uint32
	off  int64
}

// job is what stays in memory for each job: the fields claims, limits and
// indexes read, with no pointers, so the garbage collector never scans it. The
// rest of the job, its body, sits in the cache or on disk.
type job struct {
	id                                    uid
	queue, kind, tenant, resource, worker sym
	prio, attempt, maxAttempts, pending   int32
	runAt, waitingSince, startBy, lease   int64
	created, updated, started, finished   int64
	version, window                       int64
	body                                  ref
	state, flags                          uint8
}

func ms(t time.Time) int64             { return runnel.Millis(t) }
func fromMs(v int64) time.Time         { return runnel.FromMillis(v) }
func (h *job) final() bool             { return final(h.state) }
func (h *job) is(st uint8) bool        { return h.state == st }
func (h *job) stateName() runnel.State { return stateNames[h.state] }

// hot takes the in-memory fields of j, interning its names.
func (t *syms) hot(j *runnel.Job) job {
	h := job{queue: t.id(j.Queue), kind: t.id(j.Kind), tenant: t.id(j.Tenant), resource: t.id(j.Resource), worker: t.id(j.Worker),
		prio: int32(j.Priority), attempt: int32(j.Attempt), maxAttempts: int32(j.MaxAttempts), pending: int32(j.Pending),
		runAt: ms(j.RunAt), waitingSince: ms(j.WaitingSince), startBy: ms(j.StartBy), lease: ms(j.LeaseUntil),
		created: ms(j.CreatedAt), updated: ms(j.UpdatedAt), started: ms(j.StartedAt), finished: ms(j.FinishedAt),
		version: j.Version, window: j.StartWindow().Milliseconds(), state: stateCode(j.State)}
	h.id, _ = parseID(j.ID)
	if j.UniqueKey != "" {
		h.flags |= flagUnique
	}
	if len(j.DependsOn) > 0 {
		h.flags |= flagDeps
	}
	return h
}

// full rebuilds the whole job from its in-memory fields and its body.
func (t *syms) full(ns string, h *job, body []byte) (*runnel.Job, error) {
	j := &runnel.Job{ID: h.id.String(), Namespace: ns, Queue: t.name(h.queue), Kind: t.name(h.kind),
		Tenant: t.name(h.tenant), Resource: t.name(h.resource), Worker: t.name(h.worker), State: h.stateName(),
		Priority: int(h.prio), Attempt: int(h.attempt), MaxAttempts: int(h.maxAttempts), Pending: int(h.pending),
		RunAt: fromMs(h.runAt), WaitingSince: fromMs(h.waitingSince), StartBy: fromMs(h.startBy), LeaseUntil: fromMs(h.lease),
		CreatedAt: fromMs(h.created), UpdatedAt: fromMs(h.updated), StartedAt: fromMs(h.started), FinishedAt: fromMs(h.finished),
		Version: h.version}
	return j, decodeBody(body, j)
}
