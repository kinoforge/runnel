# Runnel behaviour spec

Every rule Runnel follows, as someone using the library or the HTTP API sees it. Each rule has an ID, and at least one test checks it by calling `spec.Rule(t, "C04")`. The tests in `internal/spec` fail when a rule has no test, or when a test names a rule that is not here.

Run only the tests of some rules or areas:

```
RUNNEL_RULES=C04 go test ./...
RUNNEL_RULES=C,W make test-all
```

Store rules run against every backend through `store/storetest`. The reasons behind the rules are in [DESIGN.md](DESIGN.md).

## Words

- **Job**: one piece of work. Its kind picks the handler that runs it, its payload is the JSON input, its result the JSON output.
- **Queue**: where jobs wait. Its settings limit how many of its jobs run at once, per group, and how fast they start.
- **Group**: whoever a job is for, such as a customer. Per-group limits keep one group from crowding out the rest.
- **Pool**: a limit shared by the jobs of every queue that name it, such as the calls a partner API allows at once.
- **Worker**: a process, or goroutines in one, that claims jobs and runs their handlers.
- **Lease**: a claim on a job that lasts while its worker heartbeats. When it ends, the job goes back.
- **Namespace**: one isolated set of queues, jobs, settings and keys, usually one per service.
- **Store**: the database or Redis that holds all of it. Producers and workers share nothing else.
- **Maintainer**: the loop that takes back the jobs of dead workers, expires jobs that waited too long to start, fires schedules, makes callback deliveries and prunes.

A job is `blocked` (waiting for other jobs), `waiting`, `running`, `polling` (an outside task it started is running), or final: `completed`, `dead` or `cancelled`. Running and polling jobs are active: they hold a slot.

## Jobs (J)

- **J01** A job reads back as it was enqueued: queue, kind, payload, priority, group, pool, max attempts, timeout, start timeout, callback and meta. A new job is `waiting`, or `blocked` (F01). Its id is a UUID v7; reading an unknown id is `ErrNotFound`.
- **J02** Input is checked before anything is written; bad input is `ErrInvalid`. Queue, kind and pool names are 1 to 128 letters, digits, dots, dashes or underscores, starting with a letter or digit. Priority is -1000 to 1000. The payload is JSON of at most 1 MiB. Max attempts is 1 to 1000. Durations are not negative. Dependencies are job ids. A unique key has no `|`. A callback is an absolute http or https URL without credentials.
- **J03** Options a job leaves unset come from its queue's settings: max attempts, backoff, timeout, start timeout and callback. With neither set, a job has 5 attempts.
- **J04** One call enqueues 1 to 1000 jobs. Each job gets its own answer: its id; or, as a duplicate, the id of the job holding its unique key; or its own error (`ErrDependency`, `ErrQueueFull`, `ErrGroupFull`) while the others go in.
- **J05** A job given a start time (`At`, `In`, `run_at`, `delay`) is not claimed before it. `In` is measured on the store's clock.
- **J06** While a job with a unique key is not final, enqueueing the same key adds nothing and returns that job's id, marked duplicate. Once the job is final, the key is free.
- **J07** A unique key with a window stays taken for the window after enqueue, even when the job finishes sooner, and is free after it.
- **J08** A queue with `max_queued` refuses a job with `ErrQueueFull` once that many of its jobs are waiting or blocked; with `group_max_queued`, it refuses with `ErrGroupFull` once the job's group has that many. Running jobs do not count, and one queue's cap does not touch another queue.
- **J09** `EnqueueTx` writes jobs inside the caller's SQL transaction: they exist only if it commits, and the queue settings are read through it.
- **J10** Durations in JSON are strings such as `"30s"` or numbers of seconds, and are written back as strings.

## Flows and dependencies (F)

- **F01** A job enqueued `After` other jobs is `blocked` until all of them complete, then `waiting`. A dependency that already completed counts as done.
- **F02** A dependency that is missing, dead or cancelled refuses the job with `ErrDependency`.
- **F03** When a job ends dead or cancelled, every blocked job that depends on it is cancelled, and so on down the chain.
- **F04** A flow enqueues all its steps in one call. A step that waits on an unknown step, a cycle, or a flow without steps or with more than 1000 is `ErrInvalid`.
- **F05** A flow step whose unique key is taken becomes the job holding it, and the steps after it wait on that job.
- **F06** A step's handler reads the jobs it depended on, with their results, through `Dependencies(ctx)`.

## Claiming (C)

- **C01** A claim leases up to its limit of ready jobs, `waiting` with their start time passed, from the queues it names. Each becomes `running` under the claiming worker with a lease end, and its attempt count goes up by one.
- **C02** Jobs are claimed highest priority first, then earliest start time, then oldest, across all the queues of the claim.
- **C03** A job is held by one claim at a time: concurrent claims never return the same job.
- **C04** A paused queue starts no jobs; unpausing it lets them start.
- **C05** `max_running` caps the active jobs of a queue across all workers. A slot frees when a job stops being active.
- **C06** `group_max_running` caps the active jobs of one group in a queue. A pool's `slots` cap the active jobs naming it over every queue, and its `group_slots` cap them per group.
- **C07** `rate` and `burst` cap how many jobs of a queue start per second, as a token bucket. `group_rate` and `group_burst` do the same per group.
- **C08** A limit that holds back the first jobs in line does not hold back the jobs after them: a group at its cap or a pool with no free slot is skipped and the claim goes on to others. On Redis this holds within the first 2000 jobs a claim examines.
- **C09** Polling jobs due for a poll are claimed before new jobs. They keep their attempt count and are not held back by limits, because they already hold their slot.
- **C10** A claim can be limited to given job ids. It still obeys every limit and touches no other job.
- **C11** A pool's `rate` and `burst` cap how many jobs naming it start per second over every queue, as a token bucket.
- **C12** A paused pool, or one whose `paused_until` has not passed, starts no jobs; its running and polling jobs go on, and the claim goes on to jobs of other pools. Once `paused_until` passes the pool starts jobs again without a change.

## Leases (L)

- **L01** A worker heartbeats every third of its lease. A heartbeat extends the jobs the worker still holds and reports the ones it lost: cancelled, or no longer leased to it.
- **L02** When a running job's lease ends without a heartbeat, the maintainer takes it back as a retry with the error recorded: the attempt counts (O02).
- **L03** The maintainer never takes back a job whose lease is still live.
- **L04** Every write of a job is checked against the version it was read at. A writer holding an older version, such as a worker that lost its lease, changes nothing.
- **L05** A worker that loses a job cancels the handler's context with `ErrLost`, so `Cancelled(ctx)` is true, and drops its result.

## Outcomes (O)

What a handler returns decides what happens to the job next.

- **O01** `nil` completes the job, with the result set by `SetResult`.
- **O02** Any other error retries after the backoff while attempts remain. Then the job is `dead`.
- **O03** The wait before retrying attempt n is `base * factor^(n-1)`, capped at `max`, then spread by up to `jitter` either way. Unset fields mean 1s, 10m, 2 and 0.2.
- **O04** `Fail(err)` stops retrying: the job is `dead`.
- **O06** `Busy(d)` puts the job back to `waiting` for `d` (15s when unset). It uses no attempt, records no error and keeps how long the job has been waiting.
- **O07** `Pending(handle, d)` parks the job as `polling` and calls the kind's poller with the handle after `d` (5s when unset). The attempt stays the same through every poll. `Pending` without a handle is a failure.
- **O08** `RetryIn(d, err)` retries after exactly `d`.
- **O09** These errors keep their meaning when wrapped.
- **O10** Each attempt that does not complete adds an entry to the job's errors: attempt, queue, kind, time and message. The last 20 are kept.
- **O11** Whatever the outcome, a job that stops being active drops its worker and lease, and a final job has a finish time.

## Workers (R)

- **R01** A worker runs at most its concurrency of jobs at once (10 by default), and claims only as many as it has free slots.
- **R02** A worker runs the handler registered for the job's kind. A job of a kind with no handler fails at once with `ErrNoHandler`.
- **R03** A panic in a handler is an error: the attempt is retried.
- **R04** Middleware wraps every handler, the first listed outermost.
- **R05** An attempt that runs past the job's timeout has its context cancelled with `ErrTimeout`, and is retried.
- **R06** When its context ends, a worker stops claiming and waits up to its shutdown timeout for running jobs. Then it cancels them with `ErrShutdown` and gives them back untouched: no attempt used, no error recorded.
- **R07** A worker needs at least one queue and one handler; otherwise `Run` returns `ErrInvalid`.
- **R08** A busy worker claims again once a quarter of its slots are free, or after 2ms, so claims take jobs in batches. A free slot never waits longer than that for a claim.
- **R09** With `tracing.Middleware`, each attempt is a consumer span named after its queue, a child of the trace in the job's `traceparent` meta, with an error status when it fails or retries. `tracing.Job` puts the caller's trace in a job, and `tracing.Handler` makes each API request a server span named after its route.
- **R10** `SetConcurrency` changes a running worker's concurrency. A larger one claims more jobs at once; a smaller one lets running jobs finish and starts no new one until fewer than the new number run. It must be at least 1, or it is `ErrInvalid` and nothing changes.

## Managing jobs (M)

- **M01** `Cancel` ends a job that is not final as `cancelled`; a running job learns it at its next heartbeat (L05). Cancelling a cancelled job changes nothing. Cancelling a completed or dead job is `ErrFinal`.
- **M02** `Retry` runs a dead, cancelled or expired job again from its first attempt, keeping its error history. Any other state is `ErrNotFinal`. A job cancelled before it ran, because a dependency did not complete, runs again once every dependency is completed; until then `Retry` is `ErrDependency`.
- **M03** A listing is newest first, filtered by queue, kind, group and state, in pages joined by a cursor: 50 jobs by default, at most 500.
- **M04** Stats count a namespace's jobs per queue and state.
- **M05** Every job that reaches a final state records one event. Events are numbered in order within a namespace and read from after a number.
- **M06** A named cursor moves only from the value the caller expects it to hold, so of two maintainers racing, one wins.

## Waiting for results (W)

- **W01** `Wait` returns the job once it is final. Stores that announce finished jobs (Postgres and the Redis family) wake it at once; on others it reads the job again with a backoff of up to 1s.
- **W02** When the caller's context ends first, `Wait` returns the job as last read and the context's error. Waiting on an unknown job is `ErrNotFound`.
- **W03** `EnqueueAndWait` enqueues a job and waits for it like `Wait`. A duplicate waits for the job holding its key.
- **W04** `Worker.Do` enqueues a job and, when the worker has a free slot and the job's limits allow, runs its first attempt at once on the caller's goroutine. Otherwise the job takes the normal path. Either way `Do` returns when the job is final, like `EnqueueAndWait`. Waiting does not hold up the worker's shutdown.
- **W05** `Worker.Do` needs a running worker that reads from a store, serves the job's queue and has a handler for its kind.

## Maintenance (T)

- **T01** A waiting job that has not started within its start timeout ends `expired`, with an error recorded. Like every final state, this is an event and a callback, and the jobs that depend on it are cancelled. A retried job, whether a handler asked for the retry or an operator did, waits from the retry again, with its start timeout counted anew.
- **T02** A schedule enqueues a job on its cron spec: five fields, or `@every 10m`, `@daily` and the like, in a time zone. Each firing is enqueued once, even with many maintainers running, and the job carries `schedule` and `fire_at` meta. With `overlap: skip`, a firing is dropped while the schedule's previous job has not finished; a dropped firing, or one refused by a full queue or group, is not an error.
- **T03** The maintainer deletes completed and cancelled jobs after 24h and dead jobs after 7 days, unless the queue sets its own retention, and can archive them first. Only final jobs can be pruned.
- **T04** Events outlive the jobs they describe and are deleted after their own retention, 24h by default.
- **T05** With callbacks on, an event is not deleted before its delivery job is made, unless it has waited longer than the undelivered retention (7 days). Those are deleted and logged. Jobs that finished since the oldest such event are not pruned, so their delivery can still be made.
- **T06** A job with a deadline, set as a time or as a `ttl` after enqueue, ends `expired` once the deadline passes without it completing, whether blocked, waiting, running or polling. A running job's lease is taken, so its worker's next heartbeat fails, and the attempt's context ends with `ErrDeadline`. A retry or poll that would come after the deadline expires the job at once. A job's `start_timeout` and `timeout` must fit before its deadline; a queue default that does not fit is left out. A retried expired job keeps a deadline that has not passed.

## Callbacks (H)

- **H01** A job with a callback gets one delivery for its final state. The maintainer turns the event into a delivery job on the reserved `runnel.callbacks` queue, unique per event, so the event is posted once even when it is seen twice. A delivery job that cannot be queued, such as on a full queue, is made again on the next round, with the events after it.
- **H02** A delivery POSTs `{"event", "delivery", "job"}` with the headers `Runnel-Event`, `Runnel-Delivery` and, when the namespace has a secret, `Runnel-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`.
- **H03** A 2xx answer completes the delivery. 408, 429 and 5xx retry, after `Retry-After` seconds when given (up to an hour). Any other answer, a redirect included, fails it. A delivery has 10 attempts.
- **H04** Deliveries connect only to public addresses, checked after DNS on every connection, unless private ones are allowed.
- **H05** `VerifySignature` and `CallbackReader` accept a delivery signed with any of the given secrets, so secrets can rotate, and refuse a missing, malformed or wrong signature and one signed more than 5 minutes (or the given age) from now, with `ErrSignature`. `CallbackReader` refuses a body over 16 MiB (or its `MaxBody`).
- **H06** A job whose meta holds `traceparent` or `tracestate` sends them as headers with its callback, so a trace continues to the receiver.
- **H07** A delivery is signed with the secret of the namespace it runs in. One whose job names another namespace fails without being sent.

## Clock (K)

- **K01** Leases, start times, deadlines, retries and schedules are measured on the store's clock (database or Redis time) when it has one, so hosts with skewed clocks agree. Each process samples it about every 30 seconds and keeps the offset of the fastest of three reads. A remote worker measures its jobs' deadlines on the server's clock, which claim and heartbeat answers carry as `now`.

## Stores (X)

- **X01** Namespaces never see each other's jobs, settings, events or cursors.
- **X02** `Migrate` can run again on a migrated store.
- **X03** Queue, pool and schedule settings are stored per namespace, replaced, swapped (X09) and deleted by name. Deleting a setting or namespace that does not exist is `ErrNotFound`. Bad settings are `ErrInvalid`: bad names, negative limits, rates over 1e6 per second, more than 1000 attempts.
- **X04** The hybrid store restarts with every change it acknowledged, read from its snapshot and log. A record cut short by a crash is dropped, and the store opens again after it.
- **X05** Only one process at a time opens a hybrid store's directory.
- **X06** A hybrid store's directory has a `FORMAT` file naming the version of its files. A store refuses a directory of another version, or one that holds data without the file, with `ErrFormat`.
- **X07** A hybrid store's backup, taken while calls go on, restores into an empty directory and opens with every job acknowledged before the backup began. Restore refuses a non-empty directory, entries outside it and a tar without `FORMAT`.
- **X08** A store that reports its storage gives its kind and a size of zero or more bytes.
- **X09** `SwapConfig` writes a setting only if it still holds the value the caller read, or, with no value read, only if it does not exist yet. Of two writers racing, one wins, and the other learns it lost.
- **X10** `UpdateQueue` and `UpdatePool` read a setting, let the caller change it and swap it in (X09). When another writer got there first they read again and call the change again, so no writer's fields are lost. A change that leaves the setting as it was writes nothing, and a change that breaks the setting is `ErrInvalid`.

## Access (A)

- **A01** Every `/v1` route needs credentials: `Authorization: Bearer <token>`, Basic auth (A07) or a client certificate (A09). Missing or unknown credentials are 401 with a `WWW-Authenticate` header naming Basic and Bearer. `/healthz`, `/readyz`, `/metrics` and `/openapi.yaml` need none.
- **A02** Only the admin token reaches the `/v1/namespaces`, `/v1/backup` and `/v1/system` routes; a key there is 403. A server started without an admin token closes them, unless auth is off (A08) or a client certificate gets in (A09).
- **A03** A key acts in its own namespace only (403 elsewhere) and only with its roles. `reader` reads jobs, events, stats and settings. `producer` enqueues and cancels. `worker` claims, heartbeats, finishes and reads dependencies. `admin` does all of it, changes settings and retries jobs.
- **A04** A key is `rnk_<12 hex id>_<64 hex secret>` and is shown once, when it is made. Only the SHA-256 of the secret is stored, and it is compared in constant time. An expired key is 401. A namespace holds at most 100 keys.
- **A05** Keys made at the same time are all kept. Deleting a key or its namespace revokes it at once on the server that did it, and making the namespace again does not bring old keys back.
- **A06** No answer contains a callback secret or a key hash.
- **A07** Basic auth works like a bearer token. The admin user (`admin` unless `server.admin_user` names another) with the admin token as password acts as admin. A key's id as user and its secret as password acts as that key. Anything else is 401.
- **A08** With `server.auth: none` every request acts as admin, with or without credentials, and the server logs a warning at start. The setting refuses to start next to an admin token or a client CA.
- **A09** With `server.tls.client_ca`, a client certificate signed by that CA acts as admin. With `client_names`, only a certificate whose common name or DNS name is listed does. A certificate from another CA fails the handshake, and with `require_client_cert` so does a client without one.

## HTTP API (S)

- **S01** Request bodies are strict: one JSON value, no unknown fields, no trailing data, at most 4 MiB (413 above). Errors are `application/problem+json` (RFC 9457).
- **S02** Errors map to status codes: invalid input 400; an unknown job, setting, key or namespace 404; a conflict, a finished job or a job that is not finished 409; a setting that changed since it was read 412; a patch of another media type 415; a failed dependency 422; a full queue or group 429; a store low on disk space 507. The remote client turns them back into the same Go errors.
- **S03** Over the API a callback, whether on a job, a queue or a schedule, must use https unless the server allows http, and the reserved `runnel.callbacks` queue and `runnel.callback` kind are refused.
- **S04** Enqueueing one job answers 201 with `Location: /v1/ns/{ns}/jobs/{id}`, or 200 for a duplicate. A batch, `{"jobs": [...]}`, answers 200 with one result per job.
- **S05** `Prefer: wait=N` on an enqueue holds the answer for up to N seconds: 200 with the finished job, or 202 with `Location` and `Retry-After` while it is still going. `Preference-Applied` echoes the wait used. N is capped at 60 by default; a bad or non-positive N means no wait. Without a Prefer header the queue's `wait` is used.
- **S06** A `GET` of a job with `Prefer: wait=N` answers when the job is final or N seconds pass, with its latest state.
- **S07** A full queue or group answers 429 with `Retry-After` and the title `queue full` or `group full`.
- **S08** A remote worker's name is scoped to its key, so two keys never share leases. A finish from another worker, or for an older version of the job, is 409.
- **S09** Setting a queue, pool or schedule needs the name in the body to match the path.
- **S10** `/openapi.yaml` documents exactly the routes the server serves.
- **S11** `/healthz` answers 200 while the store answers and 503 when it does not. `/metrics` serves Prometheus metrics.
- **S12** The remote client sets a queue or a pool (`SetQueue`, `SetPool`) with an admin key; a key without the admin role is 403.
- **S13** `/readyz` answers like `/healthz` until the server starts shutting down, then 503.
- **S14** `GET /v1/backup` streams the store's backup as a tar file, or answers 501 when the store has none.
- **S15** An enqueue with a `traceparent` header, and `tracestate` with it, puts them in the meta of each job that has none and has room for them.
- **S16** The server takes its caps from its settings: jobs per enqueue, how far ahead a job may run or live, queues and jobs per claim, the default lease and its range, and jobs per heartbeat.
- **S18** The remote client and the CLI connect with one URL: `http(s)://[user:password@]host[:port][/path]`, with optional `ca`, `cert` and `key` files for https. User and password go out as Basic auth. Unknown options, a bad scheme or TLS options on plain http are refused, and no error or printed endpoint shows the password.
- **S17** `GET /v1/system` needs the admin token and reports the version, start time, Go memory and the store's storage, or `null` when the store reports none.
- **S19** Reading one queue, pool or schedule answers with an `ETag`, and so does setting one. `PUT` and `PATCH` with `If-Match` write only if the setting still has one of the listed tags (`*`: if it exists), compared strongly. With `If-None-Match: *` they write only if it does not exist. Otherwise they answer 412 with the title `precondition failed`. Without these headers a `PUT` replaces the setting as before.
- **S20** `PATCH` on a queue, pool or schedule takes an RFC 7396 merge patch as `application/merge-patch+json` or `application/json`, any other type is 415. Named fields are set, `null` clears a field and the rest stay. A missing setting is created from the patch. The name comes from the path, and a different name, an unknown field or a setting that fails its checks is 400. It needs the admin role.
- **S21** The remote client's `UpdateQueue` and `UpdatePool` work like X10 over the API, with `If-Match` on the tag they read and `If-None-Match: *` for a new setting, and start again on 412. A 412 is `ErrStale`.

## gRPC and Connect API (G)

`runnel serve` serves the services in `proto/runnel/v1` on its HTTP port, over gRPC, gRPC-Web and Connect, with HTTP/2 over TLS or plain. The HTTP API is unchanged.

- **G01** JobService, WorkerService, ConfigService and AdminService take the same credentials as the HTTP API and need the same roles. Payloads and results travel as the job's raw JSON bytes, unchanged. `Enqueue` with one job answers its refusal as an error, like the HTTP API; a batch answers each job's result.
- **G02** Errors map to codes: invalid input `InvalidArgument`; an unknown job, setting, key or namespace `NotFound`; a conflict, a finished or unfinished job or a failed dependency `FailedPrecondition`; a stale precondition `Aborted`; a full queue or group and a store low on disk `ResourceExhausted` with `Retry-After`; no credentials `Unauthenticated`; a missing role `PermissionDenied`. The `Runnel-Error` metadata names the Runnel error.
- **G03** A `Work` stream starts with `start`. The worker sends credit for the jobs it can take, and the server sends at most that many, claimed under the lease named at start. The stream answers `extend` and `finish` in it, a refused finish with its error. Every answer carries `now`, the server's clock. When the stream ends, jobs claimed but not yet sent are given back untouched.
- **G04** Queue, pool and schedule settings share ETags with the HTTP API. `Patch` changes only the fields in `update_mask` and never the name; `if_match` and `if_none_match` refuse a write over a changed or existing setting with `Aborted`.

## Stories (U)

Each story walks one whole use of Runnel through its public API, from enqueue to the end, and checks every step on the way. Library stories run on every backend; HTTP stories run through `runnel serve`'s handler.

- **U01** A shop sends a welcome email with a callback. The worker completes the job, the maintainer makes one delivery, and the shop's receiver gets one POST it can verify with the namespace secret, carrying `job.completed` and the result. A job that fails for good sends `job.dead` the same way.
- **U02** One customer floods a queue while another adds a few jobs. With `group_max_running`, the flooding customer never runs more than its cap, and the other customer's jobs start while the flood still waits.
- **U03** Two queues share a provider's pool. Their jobs together never run more than the pool's slots. An operator pauses the pool: running jobs finish, none start, and other pools keep working. The app sets the pool's slots again while it is paused, and the pause stays. Once resumed, every job runs.
- **U04** A provider fails twice and then answers: the job completes on its third attempt with both errors recorded. A job that fails for good is dead after one attempt; once the cause is fixed, an operator retries it and it completes, keeping its error history.
- **U05** A render runs on a provider that answers later. The job is `polling` between checks, keeps its first attempt through every poll and completes with the result. A poll that fails for good ends the job dead.
- **U06** A worker crashes holding a job. Once its lease ends, the maintainer takes the job back and another worker completes it on its second attempt, with the lost attempt recorded.
- **U07** A worker is stopped during a deploy while a job runs. The job goes back to `waiting` with no attempt used and no error, and the next worker completes it on its first attempt.
- **U08** A fetch, resize and publish pipeline runs in order, and each step reads the results of the steps before it. When resize fails for good, publish is cancelled, and every step's final state is an event.
- **U09** A customer cancels a running export. Its handler learns it at the next heartbeat, the job ends `cancelled`, and the job waiting on it is cancelled too. Retried, the export runs again and completes.
- **U10** A job that cannot start in time, because its queue is paused, ends `expired` with an event. A job whose deadline passes while it runs ends `expired`, and its handler's context ends with `ErrDeadline`.
- **U11** A client retries an enqueue that it is not sure went through. The unique key makes it one job, marked duplicate the second time. Once the job is done, the key is free; with a window, it stays taken until the window ends.
- **U20** A service in another language uses only HTTP. The admin makes a namespace with a callback secret and keys for a producer and a worker, and sets an https callback on the queue. An enqueue with `Prefer: wait` answers 200 with the result that a remote worker set, and a signed callback arrives.
- **U21** An app and an operator share a pool over HTTP. The app sets slots with `UpdatePool`, the operator pauses it with `PATCH` and `If-Match`, and the app sets slots again: the pool stays paused. A stale tag is 412 and a reader key is 403. Once resumed, the jobs run.
- **U22** A key works from the moment it is made. Deleted, it is refused at once. A namespace deleted and made again does not bring its old keys back.
- **U23** A queue with `max_queued: 2` refuses a third waiting job with 429, `Retry-After` and the title `queue full`. Once a worker claims a job, there is room again.

## Failure drills (D)

Each drill runs the `runnel` binary with a hybrid store in a temporary folder, behind a proxy the drill controls, and does to it what goes wrong in production. Drills build with the `drill` tag and run with `make drill`, in a container capped at 1 CPU and 2 GB.

- **D01** Killed with SIGKILL while producers enqueue and workers finish jobs, then started again on the same folder, the server keeps every enqueue it acknowledged. Every job then completes, with one event.
- **D02** While the network between the clients and the server is cut, no enqueue is acknowledged, and once the cut-off workers' leases end, none of their jobs is left running. When the network comes back, every job completes, with one event.
- **D03** When the disk holding a hybrid store has less free space than `min_free_bytes` (64 MiB by default), new jobs are refused with `ErrNoSpace`, 507 over HTTP, and nothing else stops: `/healthz` answers 200, and workers claim and finish the jobs already taken. Once space is freed, enqueues are taken again without a restart, and the store still has every job it acknowledged.
- **D04** Killed again and again at random moments during writes, the server opens each time with every job it acknowledged, unchanged.
- **D05** A backup taken over `GET /v1/backup` while producers enqueue restores with `runnel restore` into an empty folder, and the restored server has every job acknowledged before the backup began. Restoring into a folder that is not empty fails.
- **D06** When the disk fills anyway, the store stops: `/healthz` answers 503 and no write is acknowledged. Once space is freed and the server restarts, it has every job it acknowledged and takes writes again.
- **D07** A hybrid store killed with SIGKILL right after it cancels waiting jobs kept on disk and retries a finished job kept on disk opens with them as acknowledged: the cancelled jobs stay cancelled and never run, the retried job waits, and every other job runs once.
