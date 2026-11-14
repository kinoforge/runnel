package storetest

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
)

// Opener returns the store under test. It may return the same store every time.
type Opener func(t *testing.T) runnel.Store

type env struct {
	t   *testing.T
	s   runnel.Store
	ns  string
	c   *runnel.Client
	ctx context.Context
}

func setup(t *testing.T, open Opener) *env {
	t.Helper()
	s := open(t)
	ns := fmt.Sprintf("t%d", rand.Int64N(1<<40))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return &env{t: t, s: s, ns: ns, c: runnel.NewClient(s, ns), ctx: ctx}
}

func (e *env) now() time.Time { return runnel.ClockOf(e.s).Now() }

func (e *env) add(queue string, opts ...runnel.Option) runnel.Inserted {
	e.t.Helper()
	res, err := e.c.Enqueue(e.ctx, queue, "k", map[string]any{"n": 1}, opts...)
	if err != nil {
		e.t.Fatalf("enqueue: %v", err)
	}
	return res
}

func (e *env) claim(queues []string, limit int) []*runnel.Job {
	e.t.Helper()
	jobs, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: queues, Worker: "w1", Limit: limit, Lease: 30 * time.Second, Now: e.now()})
	if err != nil {
		e.t.Fatalf("claim: %v", err)
	}
	return jobs
}

func (e *env) finish(j *runnel.Job, o runnel.Outcome) *runnel.Job {
	e.t.Helper()
	n := runnel.Decide(j, o, e.now())
	ok, err := e.s.Apply(e.ctx, e.ns, []runnel.Change{{Job: n}}, e.now())
	if err != nil || !ok[0] {
		e.t.Fatalf("apply: %v %v", ok, err)
	}
	return n
}

func (e *env) get(id string) *runnel.Job {
	e.t.Helper()
	j, err := e.c.Get(e.ctx, id)
	if err != nil {
		e.t.Fatalf("get %s: %v", id, err)
	}
	return j
}

func ids(jobs []*runnel.Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

// Run checks a store against the behaviour every Runnel store must have. Each
// case uses its own random namespace.
func Run(t *testing.T, open Opener) {
	tests := map[string]func(*testing.T, Opener){
		"InsertGetRoundTrip":         testRoundTrip,
		"ClaimOrder":                 testClaimOrder,
		"ScheduledNotEarly":          testScheduled,
		"UniqueDedupe":               testUnique,
		"UniqueWindow":               testUniqueWindow,
		"GroupMaxQueued":             testGroupMaxQueued,
		"QueueMaxRunning":            testMaxRunning,
		"GroupAndPoolMaxRunning":     testGroupAndPoolMaxRunning,
		"RateLimit":                  testRate,
		"PoolRateLimit":              testPoolRate,
		"PoolPause":                  testPoolPause,
		"PausedQueue":                testPaused,
		"ExtendAndLost":              testExtend,
		"VersionGuard":               testVersionGuard,
		"LeaseGuard":                 testLeaseGuard,
		"WorkflowCascadeDone":        testCascadeDone,
		"WorkflowCascadeFail":        testCascadeFail,
		"DependencyRejects":          testDependencyRejects,
		"PollingBypassesLimits":      testPolling,
		"DueStart":                   testDueStart,
		"Deadline":                   testDeadline,
		"PruneArchive":               testPrune,
		"EventsCursors":              testEvents,
		"ConfigsCRUD":                testConfigs,
		"StatsAndList":               testStatsList,
		"ConcurrentClaimsExclusive":  testConcurrentClaims,
		"WorkerEndToEnd":             testWorkerEndToEnd,
		"WorkerRetryDead":            testWorkerRetryDead,
		"WorkerBusyKeepsWait":        testWorkerBusy,
		"WorkerAsyncPoll":            testWorkerAsync,
		"WorkerFlowResults":          testWorkerFlow,
		"WorkerCancelRunning":        testWorkerCancel,
		"MaintainerReapAndStart":     testMaintainer,
		"MaintainerCron":             testCron,
		"StoreTime":                  testStoreTime,
		"EventsOutliveJobPrune":      testEventsOutlivePrune,
		"MaintainerKeepsUndelivered": testMaintainerEvents,
		"QueueMaxQueued":             testMaxQueued,
		"ClaimByID":                  testClaimByID,
		"WaitForResult":              testWait,
		"WorkerDirectStart":          testDirect,
		"FairClaimPastCappedGroup":   testFair,
		"FairClaimPastFullPool":      testFairPool,
		"EnqueueInsideTransaction":   testEnqueueTx,
		"NamespacesIsolated":         testNamespaces,
		"MigrateAgain":               testMigrateAgain,
		"GroupRateLimit":             testGroupRate,
		"WorkerConcurrency":          testWorkerConcurrency,
		"WorkerGather":               testWorkerGather,
		"CallbackDeliveryJobs":       testCallbackJobs,
		"StorageReport":              testStorage,
		"ConfigSwap":                 testSwapConfig,
		"ConfigUpdate":               testUpdateConfig,
	}
	for _, n := range slices.Sorted(maps.Keys(tests)) {
		t.Run(n, func(t *testing.T) { tests[n](t, open) })
	}
}
