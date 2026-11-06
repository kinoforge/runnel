# Runnel design

The decisions behind Runnel, why they were made, and the limits we accept. What Runnel does, rule by rule, is in [SPEC.md](SPEC.md); this document explains why.

## What Runnel is for

Runnel runs background jobs for services that do slow or unreliable work for many customers: calling partner APIs, collecting data, rendering files, sending messages. Three things matter most there, so they are built in rather than left to the user:

- **Fairness.** One busy customer, or one full partner account, must not hold up everyone else.
- **Back-pressure.** When a queue is full, callers hear it at once (429) instead of a backlog growing without bound.
- **Alternatives.** When one provider fails or is too slow to start, the job moves to the next one.

What Runnel is not:

- **Not a message broker or stream.** Kafka, RabbitMQ and NATS were removed before 0.0.1. If they come back, it will be as separate connectors that feed jobs in or events out, never as part of the core.
- **Not a workflow engine.** Flows are dependency graphs between ordinary jobs, with no durable code execution and no saved step state.
- **Never a runner of received code.** Payloads are data, and handlers are code you deploy. Nothing a caller sends is executed.

## The model

Producers enqueue jobs into queues. Workers claim jobs under a lease, run the handler for the job's kind and report an outcome. Everything is stored in one store. A maintainer handles the work nobody else owns.

```
producer --enqueue--> queue --claim, lease--> worker --outcome--> store
```

A job's life:

```
blocked    --dependencies complete-->     waiting
waiting    --claimed-->                   running
running    --nil-->                       completed
running    --error, Busy-->               waiting, later
running    --Pending-->                   polling --poll due--> running
running    --Fail, no attempts left-->    dead
waiting    --start timeout passed-->      expired
not final  --deadline passed-->           expired
not final  --Cancel-->                    cancelled
```

The words are plain on purpose: job, queue, group, pool, worker, lease, namespace. SPEC.md defines each one.

## One store, no broker

Jobs, settings, keys, events and cursors all live in one store: Postgres, MySQL, SQLite, Redis, Valkey or Dragonfly.

- **Why.** Teams already run one of these and already back it up. With jobs in the application's own database, `EnqueueTx` writes a job in the same transaction as the business row, so neither exists without the other. With no second system, there is no write to two places that can half fail.
- **One suite.** Every backend passes the same conformance suite (`store/storetest`). New behaviour is written there first, so all backends prove it.
- **Optional extras.** A store only has to implement `Store`. Extra interfaces make it better: `Waker` wakes idle workers, `EndWatcher` wakes waiters, `TimeSource` gives a shared clock, `TxInserter` allows enqueueing in the caller's transaction.

## Namespaces

A namespace is the unit of isolation: its own queues, jobs, settings, events and keys. Every store method takes the namespace explicitly, so data from one cannot reach another by accident.

- Namespaces and keys are stored outside any namespace. Each key is its own row. An earlier version kept a namespace's keys inside its record, and two key creations at once lost one of them to a read-modify-write race.
- Deleting a namespace revokes its keys at once. Its jobs stay until pruned, because deleting a large backlog in one request would be slow and risky.

## Delivery guarantees

- **At least once.** A job is leased to one worker at a time, and the worker heartbeats every third of the lease. When the worker dies, the lease ends and the maintainer puts the job back. Exactly-once is impossible once a handler has effects outside the store, so handlers should be idempotent. The job id and unique keys are there for that.
- **Versions.** Every write of a job checks the version it was read at. A stale worker, a reaper and a canceller can race freely; the loser changes nothing.
- **Cancelling a running job is cooperative.** The worker learns about it at its next heartbeat and cancels the handler's context with `ErrLost`, then drops the result.

## Limits

There are two kinds, applied at different moments:

- **At enqueue: queued caps** (`max_queued`, `group_max_queued`). These are back-pressure: the caller gets `ErrQueueFull` or `ErrGroupFull` (HTTP 429 with `Retry-After`) and can slow down, shed load or tell its own user.
- **At claim: how much runs** (`max_running`, `group_max_running`, pool `slots` and `group_slots`, `rate` and `group_rate`). These are checked when a job starts, because a slot frees when a job ends.

Details:

- **Active means running or polling.** A polling job still holds its slot, because the outside task it started still uses the pool, such as a render running at a provider. For the same reason, polls skip the limits: they already hold their slot.
- **Pools** are limits shared across queues, such as one partner account several queues call.
- **Rate limits are token buckets kept in the store,** so every worker on every host shares them.
- **`Busy` is not a failure.** "The provider is at capacity" should not use up the attempts a job has for real errors. `Busy` puts the job back without using an attempt and without resetting how long it has waited.
- **No fallbacks, an `expired` end instead.** What to try after a failure (another provider, model or account) is the caller's logic, so a worker enqueues the next job itself, linking it through `meta`. The one case a worker cannot see is a job that never started, as no worker holds it: past its `start_timeout` it ends `expired`, with an event and its callback, and the caller decides what next. An earlier version moved jobs through a list of fallback targets inside Runnel; it tied business routing to the queue.

## Fair claiming

Jobs are claimed by priority, then time. Suppose the first 400 jobs in line belong to a group already at its cap. A simple claim reads the first rows, finds them all blocked, and returns nothing, so the other groups wait behind a backlog that cannot run.

- **SQL stores** scan light rows (id, queue, group, pool, priority, start time) and admit them in order. Each group, pool or queue found full is excluded from the next scan, for up to 4 rounds of `max(need*4, 32)` rows each. A capped group costs one round, not a read of its whole backlog.
- **Redis** does the same in a Lua script that pages through the ready set, doubling the page up to 500 and stopping after 2000 jobs examined. **Known limit:** a capped group with more than 2000 jobs ahead of the others delays them until its own slots free. Per-group ready sets would remove the bound, and they are on the roadmap.

## SQL stores

- **Claims with no limits** lock rows with `FOR UPDATE SKIP LOCKED`, so workers never wait on each other.
- **Claims under any limit** take a per-namespace lock, so counting active jobs and admitting new ones is exact. Locking per group or per pool would be finer, but harder to get right and prone to deadlocks. **Known limit:** limited claims in one namespace run one at a time (about 675 jobs/s on Postgres on the reference box). Split hot workloads into namespaces.
- **Inserts** take the namespace lock only when a queued cap applies, because the count and the insert must agree.
- **Finishing, cancelling and retrying** take no namespace lock. Each is a row update under the version, applied in id order to avoid deadlocks, and retried on serialization failures and deadlocks.
- **Unique keys and dependencies** have their own tables, so checking them needs no scan of jobs.
- **Settings are cached briefly:** 1s in the store, 5s in each client, and API keys 5s on the server (1s for a key it has not seen). A change reaches every process within about 5 seconds.

## Postgres: durability and speed

Early benchmarks ran 57 jobs/s on Postgres. Three changes brought that to 1,200 to 1,600 on a 2014 desktop.

1. **Notifications after commit.** `NOTIFY` inside a transaction takes a database-wide lock at commit, which put every commit in one line. Wake-ups and end notices are now sent after commit, batched by a notifier. `EnqueueTx` is the exception: it notifies inside the caller's transaction, so the wake-up arrives only if the transaction commits.
2. **Grouped finishes.** Results that pile up while a commit is in flight go out together as one transaction of up to 500. When a batch fails, its results are applied one at a time. An idle worker sends at once, so this adds no delay.
3. **Claims commit without waiting for the disk** (`synchronous_commit` off for claims only). After a database crash, the last claims may be forgotten and those jobs run again, which at-least-once delivery already allows. Enqueues and results always wait for the disk before they are acknowledged.

## MySQL and SQLite

Both use the same SQL store through a dialect. Neither has a wake-up signal, so idle workers poll (every second by default) and `Wait` reads with a backoff of up to 1s. MySQL is bound by how fast the disk flushes commits; with the binary log on, each commit flushes twice. SQLite runs in WAL mode and suits development and single-host services.

## Hybrid store

Every database round trip costs more than the work a claim does, so the hybrid store keeps the jobs in the process and only writes a log.

- **Memory.** Each namespace has its own lock, fixed-size job records in blocks of 4096 that never move and are released once empty, and B-trees keyed by 16-byte ids: ready jobs per queue in claim order, scheduled and polling jobs by time, leases, start deadlines, finished jobs and ids by state. Queue, kind, group, pool and worker names are stored once per namespace and referred to by number. Records and index keys hold no pointers, so the garbage collector never scans them; this took the collector from 30% of CPU to 6%.
- **Calls.** Calls that arrive while another holds a namespace queue up, and one of them runs the whole queue under one hold of the lock (flat combining), instead of the lock passing from goroutine to goroutine. Bodies are encoded before the lock is taken, and claimed jobs are rebuilt after it is released.
- **Bodies.** Payload, result, meta, errors and the other fields claims never look at form a job's body, in a compact binary form. A body lives in the record that wrote it, in the log or a snapshot, and the job keeps where. A body stays in memory only until its record is written; after that it is read from the file, through the OS page cache, with one read for each run of neighbouring bodies. Keeping bodies in a cache of our own measured no faster and cost memory the page cache can use instead. Claims and lease extensions log only the small record, not the body again.
- **Log.** A call applies its changes to memory and appends them as one checksummed record. `store/hybrid/wal` knows nothing about jobs. The first caller to wait writes everything queued so far, its record and those of the callers behind it, so one write and one fsync serve them all (group commit), with no hand-off to another goroutine. The periodic fsync runs beside writers instead of holding them up. Recovery replays the same records, so it runs no job logic.
- **Snapshots.** When the log passes 64 MiB and the size of the last snapshot, the store rotates to a new log file and writes a snapshot while calls go on, copying one block of job records at a time under the namespace lock and their bodies in disk order. What the snapshot misses is in the new log file, and replaying it over the snapshot gives the same state: every op sets its value outright, and replay skips kids and events it already holds. The log is synced before the snapshot is renamed into place; then jobs whose body did not change are pointed at their copy, and the older log files and snapshot are deleted.
- **Finished jobs.** A second after a job finishes, it moves with its event to a cold store on disk (`store/hybrid/cold`, built on Pebble, the storage engine of CockroachDB) and leaves memory, so retention costs disk, not memory. The cold store keeps each job by id, with indexes by state for listing and by finish time for pruning, and per-queue counts written in the same batch as the jobs. Reads, listings, counts, retries, duplicate ids and dependencies look in memory first, then there; ids are time-ordered, so a new job's id, larger than any in the cold store, needs no lookup. Pebble's own log is off: the store syncs its log before moving jobs, and flushes Pebble before a snapshot replaces the older log files, so a replay restores whatever Pebble lost.
- **Backlog.** Memory holds the front of each queue; waiting jobs past a quarter of the memory limit (about 300 bytes each) move to the cold store, last in claim order first: future ones keyed by run time, ready ones by priority, run time and id, each entry holding the whole record so jobs come back in order without a lookup each. Each queue keeps its head there, the first job in claim order. A claim takes from memory only the jobs ahead of that head; when it reaches the head, it brings the next batch back, 8192 jobs or 4 MiB (logged like an insert, then deleted from the cold store) and goes on, so claim order holds across memory and disk. A claim first moves the future jobs due by its time to the ready order. Jobs others depend on, and jobs still being deleted from the cold store, stay in memory. Start deadlines and group counts of the jobs on disk stay in memory, so expiry and `group_max_queued` still apply. Inserts help move jobs out while memory holds twice the budget, so a producer faster than the disk slows down instead of filling memory.
- **Failure.** If writing the log fails, memory holds changes the disk does not, so the store refuses every later call until it is reopened from disk.
- **Why not an embedded key-value store for everything.** Active jobs change several times a second each; a KV store would add a second copy of every change and a second log. The job model already says what to index, and the log of changes is all that must reach the disk. Finished jobs change no more, so they suit one.

## Time

Leases, start times, retries and schedules compare times written by different hosts. With skewed clocks, a fast host could take back jobs from live workers, and a slow one could start jobs late.

- Every process measures time on the store's clock: `clock_timestamp()` on Postgres, `UTC_TIMESTAMP(6)` on MySQL, `TIME` on Redis.
- Each process samples the store three times every 30 seconds and keeps the offset from the fastest round trip, so reading the time costs no round trip.
- `In(d)` is turned into a time at enqueue, on that clock.
- Stores without a clock use local time.

## Waiting for a result

A caller waits in one of three ways:

1. **Come back later:** the enqueue answer has a `Location`; read the job when you like.
2. **Wait:** `Client.Wait` or `EnqueueAndWait` in Go. Over HTTP, `Prefer: wait=N` (RFC 7240) on the enqueue or on a read holds the request until the job is final, then answers 200; when N runs out, it answers 202 with `Location` and `Retry-After`. A standard header on the same endpoint means clients that do not send it get a plain enqueue, and no second API is needed.
3. **Be called:** a callback.

**Direct start** (`Worker.Do`) is for a caller that is also a worker: when a slot is free, the first attempt runs on the caller at once, skipping the claim loop. The job is still ordinary: if that attempt fails, the other workers retry it.

Stores that announce finished jobs (Postgres `LISTEN`, Redis pub/sub) wake waiters at once. One listener per client serves all its waiters and stops after a minute idle.

## Events and callbacks

- **Events are an outbox.** Every final state writes an event in the same transaction as the job. No final state is lost, and nothing is written to two places.
- **Deliveries are jobs.** The maintainer turns events into delivery jobs on the reserved `runnel.callbacks` queue, so deliveries get retries, backoff (5s to 30m, 10 attempts) and a visible dead state for free. Each delivery is unique per event for 48h, so a maintainer that crashes between making the job and moving its cursor does not post twice.
- **Signed:** HMAC-SHA256 over the timestamp and the body. Receivers check it and refuse old timestamps.
- **No requests into your network:** the delivery client checks the resolved address on every connection, which defeats DNS rebinding. It never follows redirects and uses no proxy. Over the API, callbacks must use https.
- **Events wait for delivery.** Events are kept 24h, but with callbacks on they are not deleted before their delivery job exists, for up to 7 days. Past that they are dropped and logged. An earlier version deleted events with their jobs, so one queue with a short retention wiped the whole namespace's callback backlog.

## Schedules

- Cron specs with minute resolution, `@every` and descriptors, in a time zone.
- **Any number of maintainers is safe.** Each schedule has a cursor that moves by compare-and-swap, and only the maintainer that moves it enqueues. Each firing's job also carries a unique key for 24h, as a second guard.
- **No surprises after a deploy or an outage.** A new schedule starts counting from now, without firing at once. After downtime, only the latest missed firing runs, not one per missed slot.

## The maintainer

The maintainer is a loop with no state of its own; run as many as you like. Each tick (every second) it takes back jobs whose leases ended, moves jobs past their start timeout, fires schedules and makes callback deliveries. Every minute it prunes old jobs and events in batches, optionally archiving jobs first. Versions and cursors make overlapping maintainers harmless.

## HTTP server

- **Same model, any language.** The server is built on the same client and worker code as the library. A remote worker speaks a small lease protocol: claim with a long-poll of up to 30s, extend, then finish with the version it claimed.
- **Two kinds of credential.** The admin token manages namespaces and keys only. A server started without one keeps those routes closed rather than open. Namespace keys carry roles (`reader`, `producer`, `worker`, `admin`). A key is `rnk_<id>_<secret>`: the id finds it, and the 256-bit random secret is stored as SHA-256. A slow hash such as bcrypt protects guessable passwords; a random secret this long needs none.
- **Never trust input.** Bodies are decoded strictly and capped at 4 MiB, every field is validated, the callback queue is reserved, and nothing received is executed. A remote worker's name is prefixed with its key id, so one key can never touch another key's leases.
- **Errors** are RFC 9457 problem documents, and the Go remote client maps them back to the same errors as the library.
- **The OpenAPI document** is embedded and served, and a test keeps it equal to the routes. SDKs for other languages will be generated from it.

## How we work

- **Spec first.** A behaviour change starts as a rule in SPEC.md. Tests name the rules they check with `spec.Rule(t, "C04")`, and `internal/spec` fails when the two disagree. `RUNNEL_RULES=C04` runs one rule's tests on every backend.
- **Store behaviour goes in the conformance suite,** never in one backend's tests.
- **Code:** exported identifiers have doc comments, and function bodies carry none. Apache 2.0, copyright The Runnel Authors.
- **Releases** pin the Go toolchain, so `govulncheck` stays clean, and are built by goreleaser with SBOMs and keyless cosign signatures.
- **Schema:** until the first release, the v1 schema is edited in place. After it, migrations only add.

## Known limits

- On Redis, a claim examines at most 2000 jobs (see Fair claiming).
- The hybrid store keeps about 290 bytes in memory per running job and per waiting job up to a quarter of the memory limit; finished jobs leave memory after a second, and the rest of a backlog waits on disk. A restart brings the jobs written since the last snapshot back into memory, and they move out again. Jobs waiting on disk run at the pace the disk returns them, and one process owns a directory; other processes go through the server. A claim examines at most 4096 jobs refused by a limit.
- After a crash, the last claims on the hybrid store may run again; with `sync=periodic` a machine crash loses up to 100ms of changes, and with `sync=off` any crash does.
- Limited claims in one namespace run one at a time on SQL stores.
- After a Postgres crash, the last claims may run again.
- MySQL and SQLite have no wake-up signal, so workers and waiters poll.
- Cancelling a running job takes effect at its next heartbeat.
- Settings reach every process within about 5 seconds.

## Later

- SDKs for Python and TypeScript, generated from the OpenAPI document with a hand-written worker loop.
- A dashboard in `runnel serve`.
- Worker labels, so a job can ask for workers with an ability such as a GPU or a region.
- Per-group ready sets on Redis, removing the 2000-job bound.
- Partial indexes on Postgres for the hottest scans.
- Connectors for brokers, outside the core, if users need them.
