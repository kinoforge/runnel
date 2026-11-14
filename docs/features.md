# Features

This page walks through what Runnel does and how to use it. The exact rules, each with an id and the tests that check it, are in [SPEC.md](SPEC.md).

## The words

| Word | What it means |
|---|---|
| **Job** | one piece of work: a `kind` (which handler runs it), a JSON payload and some options |
| **Queue** | where jobs wait. It has its own limits: how many run at once (`max_running`), how many per group (`group_max_running`), how fast they start (`rate`), and how many may wait (`max_queued`) |
| **Pool** | a number of `slots` shared by every job that names it, whatever its queue. Think of one provider account or one GPU machine |
| **Group** | any id you give a job, like a customer, an API key or a project. Each group has its own caps, so one group cannot crowd out the rest |
| **Worker** | your process, in Go or any language over HTTP. It has slots of its own, claims the next jobs the limits allow and runs one per slot |
| **Lease** | a worker's hold on a job, kept alive by heartbeats. If the worker dies, the job goes back to the queue |
| **Namespace** | one separate set of queues, jobs, settings and keys. Usually one per service |

## What a handler can answer

When a handler finishes, what it returns decides what happens next.

| Handler returns | What happens |
|---|---|
| `nil` | the job is completed, with the result from `j.SetResult` |
| any error | it is retried with backoff. When attempts run out, it is dead |
| `runnel.Fail(err)` | no more retries: dead |
| `runnel.Busy(d)` | the pool is full. Try again after `d`, and it does not count as an attempt |
| `runnel.Pending(handle, d)` | an outside task is running. Check it with `handle` after `d` |
| `runnel.RetryIn(d, err)` | retry after exactly `d` |

## Everything it can do

| | |
|---|---|
| Priority | from -1000 to 1000. Higher runs first, then oldest first |
| Fair claiming | a group at its `group_max_running`, or a pool with no free slot, is skipped, and the jobs behind it still run |
| Limits | running jobs per queue, per group, per pool, and per group inside a pool. Start rates per queue, per group and per pool. All of them hold across every worker |
| Pool pause | `paused` or `paused_until` on a pool. Its jobs wait while a provider recovers, and every other pool keeps going |
| Queue caps | `max_queued` and `group_max_queued` turn new jobs away with `429` and `Retry-After`, so a queue cannot grow forever |
| Retries | exponential backoff with jitter, per job or per queue. Every attempt's error is kept |
| Start timeouts | `StartTimeout(d)`: a job that waited `d` without starting ends `expired`, with an event and its callback, and the caller decides what to do next |
| Deadlines | `Deadline(t)`, `TTL(d)` or a queue's `ttl`: a job not done by then ends `expired`, whatever it was doing. A running handler's context ends with `ErrDeadline` |
| Leases | workers send heartbeats for their jobs. A crashed worker's jobs go back to the queue. Cancelling a running job stops its handler |
| Waiting for results | `Prefer: wait=N` on enqueue and on read, or a queue's default `wait`. In Go: `Client.Wait` and `EnqueueAndWait`. Postgres and Redis wake waiters the moment a job ends |
| Direct start | `Worker.Do` runs a job on the calling goroutine when a slot is free. It is still a real job, with retries |
| Transactional enqueue | `Client.EnqueueTx` on SQL stores: the job exists only if your own transaction commits |
| Unique jobs | by key, while the job runs or for a time window |
| Workflows | chains and DAGs. Steps read their parents' results, and a failure cancels everything after it |
| Cron | cron specs, `@every`, time zones. Run as many maintainers as you like and each firing still happens once |
| Callbacks | signed with HMAC-SHA256, retried, never redirected, never sent to private addresses |
| Namespaces | each service gets its own queues, pools, schedules, callback secret and API keys with roles |
| One clock | leases and schedules are measured on the store's clock, so servers whose clocks drift still agree |
| Operations | pause and resume, retention with an optional archive, `/healthz` and `/readyz`, backups of the hybrid store, Prometheus metrics, OpenTelemetry traces, an OpenAPI document and `runnel bench`. See the [runbook](RUNBOOK.md) |

## An example: collecting data from suppliers

A service collects sales data for its customers. Supplier A allows 5 calls at a time, and supplier B is the backup. A collection can take minutes. No customer may hold more than 2 slots, and nobody should wait more than a minute for A. If A cannot help, trying B is the service's own choice: Runnel ends the job and the service enqueues the next one.

```go
store, _ := postgres.Open("postgres://localhost/app")
store.Migrate(ctx)

c := runnel.NewClient(store, "collector")
c.SetQueue(ctx, runnel.QueueConfig{Name: "collect", GroupMaxRunning: 2, MaxQueued: 10000})
c.SetPool(ctx, runnel.PoolConfig{Name: "supplier-a", Slots: 5, Rate: 2})
c.SetPool(ctx, runnel.PoolConfig{Name: "supplier-b", Slots: 20})

c.Enqueue(ctx, "collect", "sales.collect", Request{Shop: 42},
	runnel.Group("shop-42"),
	runnel.Pool("supplier-a"),
	runnel.StartTimeout(time.Minute),
	runnel.Unique("sales:42:2026-11-01", 24*time.Hour),
	runnel.Callback("https://app.example.com/hooks/runnel"))

w := runnel.NewWorker(store, "collector", runnel.WorkerConfig{Queues: []string{"collect"}, Concurrency: 50})
w.HandleFunc("sales.collect", func(ctx context.Context, j *runnel.Job) error {
	var r Request
	if err := j.Bind(&r); err != nil {
		return runnel.Fail(err)
	}
	data, err := supplierA.Fetch(ctx, r.Shop)
	switch {
	case errors.Is(err, supplier.ErrRateLimited):
		return runnel.Busy(30 * time.Second)
	case errors.Is(err, supplier.ErrUnsupported):
		_, err := c.Enqueue(ctx, "collect", "sales.collect.b", r, runnel.Pool("supplier-b"), runnel.Meta(map[string]string{"parent": j.ID}))
		return err
	case err != nil:
		return err
	}
	return j.SetResult(data)
})
w.HandleFunc("sales.collect.b", collectFromB)
go w.Run(ctx)
go runnel.NewMaintainer(store, runnel.MaintainerConfig{}).Run(ctx)
```

If supplier A goes down for a while, pause its pool until it should be back. Its jobs wait, and B keeps working:

```go
c.SetPool(ctx, runnel.PoolConfig{Name: "supplier-a", Slots: 5, Rate: 2, PausedUntil: time.Now().Add(10 * time.Minute)})
```

## Work that runs somewhere else

Some work starts in an outside system and is checked later, like an export or a video render:

```go
w.Handle("report.export", runnel.Async(
	func(ctx context.Context, j *runnel.Job) error {
		task, err := exporter.Start(ctx, j.Payload)
		if err != nil {
			return err
		}
		return runnel.Pending(task.ID, 10*time.Second)
	},
	func(ctx context.Context, j *runnel.Job) error {
		st, err := exporter.Status(ctx, j.Handle)
		switch {
		case err != nil:
			return err
		case st.Running:
			return runnel.Pending("", 10*time.Second)
		}
		return j.SetResult(st.URL)
	}))
```

While a job waits like this it keeps its slot, so the limits always match what is really running.

## Using it over HTTP

```
docker compose -f deploy/compose.yml up -d postgres
export RUNNEL_STORE='postgres://runnel:runnel@localhost:55432/runnel?sslmode=disable'
export RUNNEL_ADMIN_TOKEN=$(openssl rand -hex 24)

runnel ns create collector --callback-secret "$(openssl rand -hex 16)"
runnel key create collector --roles producer,reader --name api
runnel key create collector --roles worker --name workers
runnel queue set collector collect --json '{"group_max_running":2,"max_queued":10000}'
runnel serve
```

Settings come from built-in defaults, then a YAML file (`--config FILE` or `RUNNEL_CONFIG`), then `RUNNEL_*` variables, then flags. Every setting and its default is in [`deploy/runnel.example.yaml`](../deploy/runnel.example.yaml). A setting's variable is named after its path, so `server.max_body` is `RUNNEL_SERVER_MAX_BODY`.

Enqueue from any language and wait up to 30 seconds for the answer:

```
curl -i localhost:8700/v1/ns/collector/jobs \
  -H "Authorization: Bearer $PRODUCER_KEY" -H "Prefer: wait=30" \
  -d '{"queue":"collect","kind":"sales.collect","payload":{"shop":42},"group":"shop-42"}'
```

You get `200` with the finished job, or `202` with a `Location` and `Retry-After` if it is still running. `GET` that location with `Prefer: wait=30` to wait again. Without `Prefer` the answer is `201` at once, unless the queue sets a default `wait`.

A worker in any language repeats three calls:

```
POST /v1/ns/collector/claim             {"worker":"py-1","queues":["collect"],"limit":10,"lease":"30s","wait":"20s"}
POST /v1/ns/collector/extend            {"worker":"py-1","ids":["..."],"lease":"30s"}
POST /v1/ns/collector/jobs/{id}/finish  {"worker":"py-1","version":3,"outcome":{"kind":"done","result":{"rows":120}}}
```

`claim` waits for work, so idle workers cost nothing. Go services can use `remote.NewWorker` and `remote.New(...).EnqueueAndWait` instead of the raw calls. The whole API is in the OpenAPI 3.1 document at [`server/openapi.yaml`](../server/openapi.yaml), also served at `/openapi.yaml`.

Errors are RFC 9457 problem documents. Request bodies have a size cap and are parsed strictly. API keys are shown once and stored only as SHA-256 hashes. A worker key can only finish jobs it claimed itself.

## Callbacks

When a job finishes, Runnel posts its summary to the job's callback URL:

```
Runnel-Event: job.completed
Runnel-Delivery: 1842
Runnel-Signature: t=1761900000,v1=<hex hmac_sha256(secret, "<t>.<body>")>
```

To check it, compute the HMAC of `"<t>.<raw body>"` with the namespace secret, compare it in constant time, and reject old values of `t`. In Go, Runnel does it for you. Give it the old and the new secret while you rotate:

```go
cb, err := runnel.CallbackReader{Secrets: []string{newSecret, oldSecret}}.Read(r)
if err != nil {
	http.Error(w, err.Error(), http.StatusUnauthorized)
	return
}
```

A delivery can arrive twice, so dedupe on `cb.Delivery`. If the job's meta holds a `traceparent`, the callback carries it too, and the receiver's span joins the same trace.

## Transactional enqueue

On SQL stores you can enqueue inside your own transaction:

```go
tx, _ := db.BeginTx(ctx, nil)
tx.ExecContext(ctx, "UPDATE orders SET status = 'paid' WHERE id = $1", id)
c.EnqueueTx(ctx, tx, receiptJob)
tx.Commit()
```

If the transaction rolls back, the job never existed. If it commits, the job is there. On Postgres the signal that wakes workers is part of the same transaction.

## How it works inside

- **SQL stores.** Claims on queues without limits lock rows with `SKIP LOCKED`, so workers never wait on each other. Claims under limits take turns per namespace, so the counts stay exact, and they read small rows in rounds that skip groups and pools at their limit. Finishing a job needs no namespace lock: one transaction updates the row under its version, records an event, frees its unique key and settles the jobs that depend on it.
- **Durability.** Enqueues and results reach the disk before Runnel answers. Claims do not wait for the disk: after a crash the last claims may be forgotten, and those jobs run again, which at least once delivery allows anyway.
- **Redis family.** The same rules run as Lua scripts over hashes and sorted sets.
- **Hybrid store.** The same rules run on in-memory B-trees over fixed-size job records, which Go's garbage collector skips. Each call writes what it changed as one checksummed record to a write-ahead log. Snapshots are written while calls go on, and a restart loads the newest one and replays the log after it.
- **Guarantees.** Delivery is at least once, so make handlers safe to run twice: use `j.ID` or a unique key. A job runs on one worker at a time while its lease lives. Every write is checked against a version, so a stale worker never overwrites a newer state.
