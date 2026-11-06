package runnel

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var (
	// ErrNotFound means the job, config or key does not exist in the namespace.
	ErrNotFound = errors.New("runnel: not found")
	// ErrConflict means the job changed between reading and writing it, or the
	// worker no longer holds it.
	ErrConflict = errors.New("runnel: job changed concurrently")
	// ErrGroupFull means the group already has the queue's group_max_queued jobs
	// queued.
	ErrGroupFull = errors.New("runnel: group has too many queued jobs")
	// ErrQueueFull means the queue already has max_queued jobs queued.
	ErrQueueFull = errors.New("runnel: queue has too many queued jobs")
	// ErrDependency means a job the new job depends on is missing, dead or
	// cancelled.
	ErrDependency = errors.New("runnel: dependency missing or failed")
	// ErrFinal means the job already finished and cannot be cancelled.
	ErrFinal = errors.New("runnel: job already finished")
	// ErrNotFinal means only dead or cancelled jobs can be retried.
	ErrNotFinal = errors.New("runnel: job is not dead or cancelled")
	// ErrInvalid wraps every validation error.
	ErrInvalid = errors.New("runnel: invalid input")
	// ErrNoHandler means no handler is registered for the job's kind.
	ErrNoHandler = errors.New("runnel: no handler for kind")
)

// Inserted is the result of enqueueing one job. Duplicate is set when a unique
// key matched a job already held, whose ID is returned. Err is set when this job
// was refused while others in the same batch went in.
type Inserted struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Err       error  `json:"-"`
}

// ClaimRequest asks a store for up to Limit ready jobs from Queues, leased to
// Worker until Now plus Lease. IDs, when set, limits the claim to those jobs.
type ClaimRequest struct {
	Queues []string
	IDs    []string
	Worker string
	Limit  int
	Lease  time.Duration
	Now    time.Time
}

// Change is a job to write back. The write happens only if the stored job still
// has Job.Version, still is in state From when From is set, and its lease ends
// before LeaseBefore when that is set.
type Change struct {
	Job         *Job
	From        State
	LeaseBefore time.Time
}

// DueKind selects which overdue jobs Store.Due returns.
type DueKind int

const (
	// DueLease selects running jobs whose lease has ended.
	DueLease DueKind = iota + 1
	// DueStart selects waiting jobs past their start timeout.
	DueStart
	// DueDeadline selects unfinished jobs past their deadline.
	DueDeadline
)

// Query filters a job listing. Results are newest first; Cursor continues after
// the last job of the previous page.
type Query struct {
	Queue  string
	Kind   string
	Group  string
	State  State
	Cursor string
	Limit  int
}

// PruneRequest deletes up to Limit finished jobs in States that finished before
// Before, optionally copying them to the archive first.
type PruneRequest struct {
	Queue   string
	States  []State
	Before  time.Time
	Limit   int
	Archive bool
}

// EventPruneRequest deletes up to Limit events older than Before. When UpTo is
// set, only events with a sequence number up to it are deleted.
type EventPruneRequest struct {
	UpTo   int64
	Before time.Time
	Limit  int
}

// Event records a job reaching a final state. Seq orders events within a
// namespace.
type Event struct {
	Seq       int64     `json:"seq"`
	Namespace string    `json:"namespace"`
	JobID     string    `json:"job_id"`
	Queue     string    `json:"queue"`
	Kind      string    `json:"kind"`
	State     State     `json:"state"`
	At        time.Time `json:"at"`
}

// ConfigKind names a kind of stored configuration.
type ConfigKind string

const (
	// ConfigQueue holds QueueConfig values, one per queue.
	ConfigQueue ConfigKind = "queue"
	// ConfigPool holds PoolConfig values.
	ConfigPool ConfigKind = "pool"
	// ConfigSchedule holds Schedule values.
	ConfigSchedule ConfigKind = "schedule"
	// ConfigNamespace holds Namespace values, stored outside any namespace.
	ConfigNamespace ConfigKind = "namespace"
	// ConfigKey holds APIKey values, stored outside any namespace.
	ConfigKey ConfigKind = "key"
)

type QueueStats struct {
	Queue  string          `json:"queue"`
	Counts map[State]int64 `json:"counts"`
}

// Store keeps jobs and their configuration. Every implementation passes the
// conformance suite in store/storetest. Methods take the namespace explicitly;
// namespaces never see each other's data.
type Store interface {
	// Migrate creates or upgrades the store's schema. It is safe to run from many
	// processes at once.
	Migrate(ctx context.Context) error
	Close() error
	// Insert adds prepared jobs atomically, applying unique keys, dependencies and
	// waiting caps, and returns one result per job.
	Insert(ctx context.Context, ns string, jobs []*Job, now time.Time) ([]Inserted, error)
	// Claim leases ready jobs to a worker, honouring pauses, priorities,
	// max running, group and pool limits and rate limits. Polling jobs due for a
	// poll come first.
	Claim(ctx context.Context, ns string, req ClaimRequest) ([]*Job, error)
	// Extend moves the lease of the worker's running jobs to until and returns the
	// ids it no longer holds.
	Extend(ctx context.Context, ns, worker string, ids []string, until time.Time) ([]string, error)
	// Apply writes changes, each guarded by its version, and reports which were
	// written. Jobs that reach a final state record an event, release their unique
	// key and settle their dependants.
	Apply(ctx context.Context, ns string, changes []Change, now time.Time) ([]bool, error)
	// Get returns the jobs found, in the order of ids.
	Get(ctx context.Context, ns string, ids []string) ([]*Job, error)
	// List returns a page of jobs matching q and the cursor of the next page.
	List(ctx context.Context, ns string, q Query) ([]*Job, string, error)
	// Due returns up to limit jobs whose lease or start deadline has passed at now.
	Due(ctx context.Context, ns string, kind DueKind, now time.Time, limit int) ([]*Job, error)
	// Prune deletes old finished jobs and returns how many.
	Prune(ctx context.Context, ns string, req PruneRequest) (int, error)
	// PruneEvents deletes old events and returns how many.
	PruneEvents(ctx context.Context, ns string, req EventPruneRequest) (int, error)
	// Events returns up to limit events after the sequence number after.
	Events(ctx context.Context, ns string, after int64, limit int) ([]Event, error)
	// Cursor reads a named position, such as the last callback event delivered.
	Cursor(ctx context.Context, ns, name string) (int64, error)
	// SwapCursor sets a named position to next if it still is old, and reports
	// whether it did.
	SwapCursor(ctx context.Context, ns, name string, old, next int64) (bool, error)
	PutConfig(ctx context.Context, ns string, kind ConfigKind, name string, value []byte) error
	// Configs returns every value of a configuration kind by name.
	Configs(ctx context.Context, ns string, kind ConfigKind) (map[string][]byte, error)
	DeleteConfig(ctx context.Context, ns string, kind ConfigKind, name string) error
	Stats(ctx context.Context, ns string) ([]QueueStats, error)
}

// TxInserter is implemented by SQL stores that can insert jobs inside a
// transaction the caller owns.
type TxInserter interface {
	InsertTx(ctx context.Context, tx *sql.Tx, ns string, jobs []*Job, now time.Time) ([]Inserted, error)
	ConfigsTx(ctx context.Context, tx *sql.Tx, ns string, kind ConfigKind) (map[string][]byte, error)
}

// Waker is implemented by stores that signal when jobs may be ready, so idle
// workers claim at once instead of polling.
type Waker interface {
	Subscribe(ctx context.Context, ns string) (<-chan struct{}, error)
}

// EndWatcher is implemented by stores that announce jobs reaching a final state,
// so waiters return at once.
type EndWatcher interface {
	Ended(ctx context.Context, ns string) (<-chan string, error)
}
