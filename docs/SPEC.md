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
- **J03** Options a job leaves unset come from its queue's settings: max attempts, backoff, timeout and start timeout. With neither set, a job has 5 attempts.
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

## Managing jobs (M)

- **M01** `Cancel` ends a job that is not final as `cancelled`; a running job learns it at its next heartbeat (L05). Cancelling a cancelled job changes nothing. Cancelling a completed or dead job is `ErrFinal`.
- **M02** `Retry` runs a dead or cancelled job again from its first attempt, keeping its error history. Any other state is `ErrNotFinal`.
- **M03** A listing is newest first, filtered by queue, kind, group and state, in pages joined by a cursor: 50 jobs by default, at most 500.
- **M04** Stats count a namespace's jobs per queue and state.
- **M05** Every job that reaches a final state records one event. Events are numbered in order within a namespace and read from after a number.
- **M06** A named cursor moves only from the value the caller expects it to hold, so of two maintainers racing, one wins.

## Waiting for results (W)

- **W01** `Wait` returns the job once it is final. Stores that announce finished jobs (Postgres and the Redis family) wake it at once; on others it reads the job again with a backoff of up to 1s.
- **W02** When the caller's context ends first, `Wait` returns the job as last read and the context's error. Waiting on an unknown job is `ErrNotFound`.
- **W03** `EnqueueAndWait` enqueues a job and waits for it like `Wait`. A duplicate waits for the job holding its key.
- **W04** `Worker.Do` enqueues a job and, when the worker has a free slot and the job's limits allow, runs its first attempt at once on the caller's goroutine. Otherwise the job takes the normal path. Either way `Do` returns when the job is final, like `EnqueueAndWait`.
- **W05** `Worker.Do` needs a running worker that reads from a store, serves the job's queue and has a handler for its kind.

## Maintenance (T)

- **T01** A waiting job that has not started within its start timeout ends `expired`, with an error recorded. Like every final state, this is an event and a callback, and the jobs that depend on it are cancelled. A retried job waits from the retry again.
- **T02** A schedule enqueues a job on its cron spec: five fields, or `@every 10m`, `@daily` and the like, in a time zone. Each firing is enqueued once, even with many maintainers running, and the job carries `schedule` and `fire_at` meta.
- **T03** The maintainer deletes completed and cancelled jobs after 24h and dead jobs after 7 days, unless the queue sets its own retention, and can archive them first. Only final jobs can be pruned.
- **T04** Events outlive the jobs they describe and are deleted after their own retention, 24h by default.
- **T05** With callbacks on, an event is not deleted before its delivery job is made, unless it has waited longer than the undelivered retention (7 days). Those are deleted and logged.
- **T06** A job with a deadline, set as a time or as a `ttl` after enqueue, ends `expired` once the deadline passes without it completing, whether blocked, waiting, running or polling. A running job's lease is taken, so its worker's next heartbeat fails, and the attempt's context ends with `ErrDeadline`. A retry or poll that would come after the deadline expires the job at once. A job's `start_timeout` and `timeout` must fit before its deadline; a queue default that does not fit is left out. A retried expired job keeps a deadline that has not passed.

## Callbacks (H)

- **H01** A job with a callback gets one delivery for its final state. The maintainer turns the event into a delivery job on the reserved `runnel.callbacks` queue, unique per event, so the event is posted once even when it is seen twice.
- **H02** A delivery POSTs `{"event", "delivery", "job"}` with the headers `Runnel-Event`, `Runnel-Delivery` and, when the namespace has a secret, `Runnel-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`.
- **H03** A 2xx answer completes the delivery. 408, 429 and 5xx retry, after `Retry-After` seconds when given (up to an hour). Any other answer, a redirect included, fails it. A delivery has 10 attempts.
- **H04** Deliveries connect only to public addresses, checked after DNS on every connection, unless private ones are allowed.

## Clock (K)

- **K01** Leases, start times, retries and schedules are measured on the store's clock (database or Redis time) when it has one, so hosts with skewed clocks agree. Each process samples it about every 30 seconds and keeps the offset of the fastest of three reads.

## Stores (X)

- **X01** Namespaces never see each other's jobs, settings, events or cursors.
- **X02** `Migrate` can run again on a migrated store.
- **X03** Queue, pool and schedule settings are stored per namespace, replaced and deleted by name. Bad settings are `ErrInvalid`: bad names, negative limits, rates over 1e6 per second, more than 1000 attempts.
- **X04** The hybrid store restarts with every change it acknowledged, read from its snapshot and log. A record cut short by a crash is dropped, and the store opens again after it.
- **X05** Only one process at a time opens a hybrid store's directory.

## Access (A)

- **A01** Every `/v1` route needs `Authorization: Bearer <token>`; a missing or unknown token is 401. `/healthz`, `/metrics` and `/openapi.yaml` need none.
- **A02** Only the admin token reaches the `/v1/namespaces` routes; a key there is 403. A server started without an admin token closes them.
- **A03** A key acts in its own namespace only (403 elsewhere) and only with its roles. `reader` reads jobs, events, stats and settings. `producer` enqueues and cancels. `worker` claims, heartbeats, finishes and reads dependencies. `admin` does all of it, changes settings and retries jobs.
- **A04** A key is `rnk_<12 hex id>_<64 hex secret>` and is shown once, when it is made. Only the SHA-256 of the secret is stored, and it is compared in constant time. An expired key is 401. A namespace holds at most 100 keys.
- **A05** Keys made at the same time are all kept. Deleting a key or its namespace revokes it at once on the server that did it, and making the namespace again does not bring old keys back.
- **A06** No answer contains a callback secret or a key hash.

## HTTP API (S)

- **S01** Request bodies are strict: one JSON value, no unknown fields, no trailing data, at most 4 MiB (413 above). Errors are `application/problem+json` (RFC 9457).
- **S02** Errors map to status codes: invalid input 400; an unknown job, setting, key or namespace 404; a conflict, a finished job or a job that is not finished 409; a failed dependency 422; a full queue or group 429. The remote client turns them back into the same Go errors.
- **S03** Over the API a callback must use https unless the server allows http, and the reserved `runnel.callbacks` queue and `runnel.callback` kind are refused.
- **S04** Enqueueing one job answers 201 with `Location: /v1/ns/{ns}/jobs/{id}`, or 200 for a duplicate. A batch, `{"jobs": [...]}`, answers 200 with one result per job.
- **S05** `Prefer: wait=N` on an enqueue holds the answer for up to N seconds: 200 with the finished job, or 202 with `Location` and `Retry-After` while it is still going. `Preference-Applied` echoes the wait used. N is capped at 60 by default; a bad or non-positive N means no wait. Without a Prefer header the queue's `wait` is used.
- **S06** A `GET` of a job with `Prefer: wait=N` answers when the job is final or N seconds pass, with its latest state.
- **S07** A full queue or group answers 429 with `Retry-After` and the title `queue full` or `group full`.
- **S08** A remote worker's name is scoped to its key, so two keys never share leases. A finish from another worker, or for an older version of the job, is 409.
- **S09** Setting a queue, pool or schedule needs the name in the body to match the path.
- **S10** `/openapi.yaml` documents exactly the routes the server serves.
- **S11** `/healthz` answers 200 while the store answers and 503 when it does not. `/metrics` serves Prometheus metrics.
