# Runnel

[![ci](https://github.com/kinoforge/runnel/actions/workflows/ci.yml/badge.svg)](https://github.com/kinoforge/runnel/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/kinoforge/runnel.svg)](https://pkg.go.dev/github.com/kinoforge/runnel)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Runnel is a job queue for services that do slow or unreliable work on behalf of many customers: calling partner APIs, collecting data, sending messages, rendering files. It keeps jobs in the database you already run, and it is fair. One busy customer, or one full supplier, never holds up everyone else.

Embed it in a Go service as a library, or run `runnel serve` and use it over HTTP from any language. Every service gets its own isolated queue system, called a namespace.

```
go get github.com/kinoforge/runnel
```

Runnel is developed by [Kinoforge](https://kinoforge.dev) and released under Apache 2.0. It is at version 0.0.1: the model is settled, but the API may still change before 1.0.

## The model

| Term | What it is |
|---|---|
| **Job** | one piece of work: a `kind` (which handler runs it), a JSON payload, options |
| **Queue** | where jobs wait, with its own limits: how many run at once (`max_running`), per group (`group_max_running`), how fast they start (`rate`), how many may wait (`max_queued`) |
| **Pool** | a number of `slots` shared by every job that names it, whatever its queue, such as one provider account or one GPU box |
| **Group** | any id you give a job (a customer, an API key, a project) with its own caps, so one group cannot crowd out the others |
| **Worker** | your process, in Go or any language over HTTP, with slots of its own; it claims the next jobs the limits allow and runs one per slot |
| **Lease** | a worker's claim on a job, kept alive by heartbeats; when the worker dies, the job goes back |
| **Namespace** | one isolated set of queues, jobs, settings and keys, usually one per service |

A caller gets a result in one of three ways: it holds the request open (`Prefer: wait`), comes back later (`GET` the job), or receives a callback. With direct start, `Worker.Do` runs the job on the caller itself when a slot is free.

When a handler cannot finish a job, what it returns says what happens next:

| Handler returns | Effect |
|---|---|
| `nil` | completed, with the result from `j.SetResult` |
| any error | retry with backoff; when attempts run out, the next fallback, then dead |
| `runnel.Fail(err)` | stop retrying: next fallback, or dead |
| `runnel.Fallback(err)` | move to the next fallback now |
| `runnel.Busy(d)` | the pool is full: try again after `d` without using an attempt |
| `runnel.Pending(handle, d)` | an outside task is running: poll it with `handle` after `d` |
| `runnel.RetryIn(d, err)` | retry after exactly `d` |

## Example: collecting data from suppliers

A service collects sales data for its customers. Supplier A is preferred but allows 5 calls at a time; supplier B is the backup. A collection can take minutes. No customer may hold more than 2 slots, and nobody should wait more than a minute for A before trying B.

```go
store, _ := postgres.Open("postgres://localhost/app")
store.Migrate(ctx)

c := runnel.NewClient(store, "collector")
c.SetQueue(ctx, runnel.QueueConfig{Name: "collect", GroupMaxRunning: 2, MaxQueued: 10000})
c.SetPool(ctx, runnel.PoolConfig{Name: "supplier-a", MaxRunning: 5})
c.SetPool(ctx, runnel.PoolConfig{Name: "supplier-b", MaxRunning: 20})

c.Enqueue(ctx, "collect", "sales.collect", Request{Shop: 42},
	runnel.Group("shop-42"),
	runnel.Pool("supplier-a"),
	runnel.StartTimeout(time.Minute),
	runnel.Fallbacks(runnel.Target{Pool: "supplier-b", Kind: "sales.collect.b"}),
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
		return runnel.Fallback(err)
	case err != nil:
		return err
	}
	return j.SetResult(data)
})
w.HandleFunc("sales.collect.b", collectFromB)
go w.Run(ctx)
go runnel.NewMaintainer(store, runnel.MaintainerConfig{}).Run(ctx)
```

Work that runs in an outside system and is checked later:

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

While a job polls it keeps its slot, so limits stay true to what is really running.

## Features

| | |
|---|---|
| Priority | -1000 to 1000. Higher runs first, then oldest first. |
| Fair claiming | A group at its `group_max_running`, or a pool with no free `slots`, is skipped, and the jobs behind it run. |
| Limits | Max running per queue, per group, per pool and per group in a pool. Token-bucket rates per queue and per group. All are enforced across every worker. |
| Queued caps | `max_queued` and `group_max_queued` refuse new jobs with `429` and `Retry-After` instead of letting a queue grow without end. |
| Retries | Exponential backoff with jitter, per job or per queue. Every attempt's error is kept. |
| Fallbacks | An ordered list of other queues, kinds or pools. |
| Start deadlines | `StartTimeout(d)`: a job that waited `d` without starting moves to its next fallback. Busy requeues keep the time already waited. |
| Leases | Workers heartbeat their jobs. A crashed worker's jobs return to the queue. Cancelling a running job stops its handler. |
| Waiting for results | `Prefer: wait=N` on enqueue and on read, `Client.Wait`, `EnqueueAndWait`. Postgres and Redis wake waiters the moment a job ends. |
| Direct start | `Worker.Do` runs a job on the calling goroutine when a slot is free. It is still a real job, with retries and fallbacks. |
| Transactional enqueue | `Client.EnqueueTx` on SQL stores: the job exists only if your own transaction commits. |
| Unique jobs | By key, while the job runs or for a time window. |
| Workflows | Chains and DAGs. Steps read their parents' results; a failure cancels everything downstream. |
| Cron | Cron specs, `@every`, time zones. Any number of maintainers, and each firing happens once. |
| Callbacks | HMAC-SHA256 signed, retried, never redirected, never sent to private addresses. |
| Namespaces | Each service gets its own queues, pools, schedules, callback secret and API keys with roles. |
| One clock | Leases and schedules are measured on the store's clock, so servers whose clocks drift apart still agree. |
| Operations | Pause and resume, retention with optional archive, Prometheus metrics, OpenAPI document, `runnel bench`. |

## Stores

| | Postgres | MySQL 8 | SQLite | Redis, Valkey, Dragonfly | Hybrid |
|---|---|---|---|---|---|
| Every feature above | yes | yes | yes | yes, except transactional enqueue | yes, except transactional enqueue |
| Workers wake at once | LISTEN/NOTIFY | polling (default 1s) | polling | pub/sub | in process |
| Waiters wake at once | LISTEN/NOTIFY | polling | polling | pub/sub | in process |
| Claims without limits | `SKIP LOCKED`, no lock | `SKIP LOCKED`, no lock | one writer | one Lua script | in memory, per namespace lock |
| Good for | production | production | tests, tools, one machine | high throughput | highest throughput on one machine, no database |

The hybrid store (`hybrid:/var/lib/runnel`) keeps a small record of each active job in memory, about 290 bytes whatever the payload, and logs each change to disk. A second after a job finishes, it moves to an embedded store on disk (Pebble, inside the same binary and data directory), so keeping finished jobs for days costs disk, not memory. Waiting jobs past a quarter of the memory limit wait there too, in claim order, and come back in batches as workers reach them, so a backlog is bounded by disk, not memory. Payloads, results, meta and errors are written once and read back from disk through the OS page cache. `runnel` keeps Go's heap under 70% of the container's memory limit and leaves the rest to the page cache. Every size and timing works out of the box from the memory limit; the `memory` and `hybrid` sections of the config file (see `deploy/runnel.example.yaml`) override them.

| `?sync=` | Answers after | A crash of the process loses | A crash of the machine loses |
|---|---|---|---|
| `each` (default) | the disk flush | nothing | nothing |
| `periodic` | the write to the OS | nothing | the last 100 ms |
| `off` | memory | the last 100 ms | the last 100 ms |

`memory:` keeps nothing on disk. One process owns the directory, so run it behind `runnel serve` and point workers at the server.

All stores pass the same conformance suite in `store/storetest`, under the race detector.

On Redis, a claim examines at most 2,000 waiting jobs. If one group at its limit has more jobs than that ahead of everyone else, other groups wait until its jobs drain below that depth. The SQL stores skip capped groups inside the query and have no such bound.

## Server

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

Settings come from built-in defaults, then a YAML file (`--config FILE` or `RUNNEL_CONFIG`), then `RUNNEL_*` variables, then flags. Every key and its default is in [`deploy/runnel.example.yaml`](deploy/runnel.example.yaml); a key's variable is named after its path, so `server.max_body` is `RUNNEL_SERVER_MAX_BODY`. For local development, copy `.env.example` to `.env`.

Enqueue from any language, and wait up to 30 seconds for the answer:

```
curl -i localhost:8700/v1/ns/collector/jobs \
  -H "Authorization: Bearer $PRODUCER_KEY" -H "Prefer: wait=30" \
  -d '{"queue":"collect","kind":"sales.collect","payload":{"shop":42},"group":"shop-42"}'
```

The answer is `200` with the finished job, or `202` with a `Location` and `Retry-After` if it is still running. `GET` the location with `Prefer: wait=30` to wait again. Without `Prefer` the answer is `201` at once.

A worker in any language repeats three calls:

```
POST /v1/ns/collector/claim             {"worker":"py-1","queues":["collect"],"limit":10,"lease":"30s","wait":"20s"}
POST /v1/ns/collector/extend            {"worker":"py-1","ids":["..."],"lease":"30s"}
POST /v1/ns/collector/jobs/{id}/finish  {"worker":"py-1","version":3,"outcome":{"kind":"done","result":{"rows":120}}}
```

`claim` long-polls, so idle workers cost nothing. The outcome kinds are the ones in the table above. Go services can use `remote.NewWorker` and `remote.New(...).EnqueueAndWait` instead of the raw calls. The full API is described by the OpenAPI 3.1 document at [`server/openapi.yaml`](server/openapi.yaml), also served at `/openapi.yaml`.

Errors are RFC 9457 problem documents. Bodies are size-capped and parsed strictly. API keys are shown once and stored as SHA-256 hashes; a worker key can only finish jobs it claimed itself.

### Callbacks

When a job finishes, Runnel posts its summary to the job's callback URL:

```
Runnel-Event: job.completed
Runnel-Delivery: 1842
Runnel-Signature: t=1761900000,v1=<hex hmac_sha256(secret, "<t>.<body>")>
```

Compute the HMAC of `"<t>.<raw body>"` with the namespace secret, compare it in constant time, and reject old values of `t`.

## Transactional enqueue

```go
tx, _ := db.BeginTx(ctx, nil)
tx.ExecContext(ctx, "UPDATE orders SET status = 'paid' WHERE id = $1", id)
c.EnqueueTx(ctx, tx, receiptJob)
tx.Commit()
```

If the transaction rolls back, the job never existed. If it commits, the job is there; on Postgres the notification that wakes workers is part of the same transaction.

## How it works

- **SQL stores.** Claims of queues without limits lock rows with `SKIP LOCKED`, so workers never wait on each other. Claims under limits serialize per namespace, so counts stay exact, and read light rows in rounds that skip groups and pools at their limit. Finishing a job needs no namespace lock: it updates the row under its version, records an event, releases its unique key and settles its dependants in one transaction. Postgres notifications are sent after commit and batched, so they never serialize commits. Each worker sends the results that pile up while a commit is in flight as one transaction, which adds no delay when it is idle.
- **Durability.** Enqueues and results are committed to disk before they are acknowledged. On Postgres, claims commit without waiting for the disk: after a crash the last claims may be forgotten, and those jobs run again, which at-least-once delivery allows anyway.
- **Redis family.** The same rules run as Lua scripts over hashes and sorted sets. A namespace's keys share a hash tag, so it works on Redis Cluster.
- **Hybrid store.** The same rules run on in-memory B-trees over fixed-size job records with no pointers, which the garbage collector skips. Each call logs what it changed as one checksummed record in a write-ahead log (`store/hybrid/wal`); job bodies are read back from the log and snapshots through the OS page cache. Snapshots are written while calls go on. Restart loads the newest snapshot and replays the log after it, dropping a record cut short by a crash. Claims return without waiting for the disk, like on Postgres: the finish that follows makes the claim durable too.
- **Guarantees.** Delivery is at least once, so make handlers idempotent: use `j.ID` or a unique key. A job runs on one worker at a time while its lease lives. Every write is guarded by a version, so a stale worker, a reaper or a canceller never overwrites a newer state.

## Benchmarks

`runnel bench` enqueues 20,000 jobs in batches of 100, then 4 workers with 25 slots each run them with an empty handler. "Fair" runs 10,000 jobs spread over 50 groups with `group_max_running: 2`, the limited path. Latency is one job from enqueue to finished result, through `Worker.Do` (direct start) or `EnqueueAndWait`.

| Store | Enqueue | Run | Run, fair | Direct start p50 | Enqueue and wait p50 |
|---|---|---|---|---|---|
| Postgres 17 | 3,300/s | 1,160/s | 675/s | 58 ms | 25 ms |
| MySQL 8.4 | 1,070/s | 270/s | 240/s | 81 ms | 75 ms |
| SQLite (WAL) | 16,200/s | 2,700/s | 1,390/s | 0.8 ms | 51 ms |
| Redis 7.4 | 17,000/s | 6,060/s | 3,110/s | 1.1 ms | 1.1 ms |
| Valkey 8.1 | 16,800/s | 5,640/s | 2,900/s | 1.0 ms | 1.2 ms |
| Dragonfly 1.32 | 8,500/s | 2,290/s | 1,520/s | 2.0 ms | 1.6 ms |

Measured on a 2014 desktop (Intel i5-4460, 4 cores, 16 GB, consumer SSD), with the benchmark and every database in Docker on the same machine, all with default durability. The SQL numbers are bound by how fast the disk flushes a commit, about 8 ms here. Server hardware flushes far faster, so expect much more there, and run `make bench` on your own. Numbers move by 20% between runs. MySQL ran with `--skip-log-bin`; with the binary log on, every commit pays a second flush. SQLite and MySQL have no wake-up signal, so waiting for a result there polls.

The hybrid store was measured on a different machine, a laptop with a SATA SSD that takes about 70 ms to flush, so compare it with SQLite on the same machine rather than with the table above:

| Store (laptop) | Enqueue | Run | Direct start p50 |
|---|---|---|---|
| SQLite (WAL) | 5,350/s | 635/s | 3.2 ms |
| Redis 7.4 | 17,900/s | 5,950/s | 0.9 ms |
| Hybrid, `sync=each` | 1,850/s | 560/s | 67 ms |
| Hybrid, `sync=each`, 4 x 200 slots | | 3,000 to 4,800/s | |
| Hybrid, `sync=periodic` | 215,000/s | 110,000 to 130,000/s | 0.03 ms |
| Hybrid, `sync=off` | 240,000/s | 127,000 to 130,000/s | 0.03 ms |
| Memory only | 300,000/s | 160,000 to 166,000/s | 0.02 ms |

The store alone (`go test -bench Cycle ./store/hybrid`) claims and finishes 190,000 jobs/s in memory and 115,000 to 135,000 with `sync=periodic`. With `sync=each` every enqueue and result waits for a flush, so throughput grows with the number of jobs in flight, not with CPU. In a Docker container limited to 500 MB and 4 CPUs, `runnel bench` runs 500,000 jobs at 72,000/s with `sync=periodic`, all enqueued before the first runs. Larger backlogs wait on disk: limited to 500 MB, 2 CPUs and a 7200 rpm hard disk, 2,000,000 jobs enqueue at 22,000/s and run at 9,000/s, and 1,000,000 jobs of 1 KB at 7,500/s and 4,000/s, with memory flat. Jobs brought back from disk run at the pace the disk allows.

## Development

```
make up          # postgres, mysql, redis, valkey and dragonfly on local ports
make test-all    # every backend, with the race detector
make test        # SQLite, server and engine only
make lint vuln fuzz
make bench       # RUNNEL_STORE=... make bench
RUNNEL_RULES=C04,W make test-all   # only the tests of rule C04 and area W
```

[SPEC.md](docs/SPEC.md) lists every rule Runnel follows, each checked by tests. [DESIGN.md](docs/DESIGN.md) explains the decisions and the known limits.

See [CONTRIBUTING.md](.github/CONTRIBUTING.md). Security reports go through [SECURITY.md](.github/SECURITY.md).

## Roadmap

- SDKs for Python and TypeScript, generated from the OpenAPI document with a hand-written worker loop.
- A dashboard built into `runnel serve`.
- Worker labels, so jobs can ask for workers with certain abilities (a GPU, a region).
- Per-group ready sets on Redis, removing the 2,000-job examination bound.

## License

Apache 2.0. Copyright 2026 The Runnel Authors. Free for any use, including commercial.
