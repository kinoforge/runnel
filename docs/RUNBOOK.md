# Runbook

How to run Runnel in production, what to watch, and what to do when something goes wrong. The rules behind each behaviour are in [SPEC.md](SPEC.md); every setting and its default is in [`deploy/runnel.example.yaml`](../deploy/runnel.example.yaml).

## Deploy

One `runnel serve` process per store. Workers and producers reach it over HTTP; they never open the store themselves.

| Store | Run it as | Data lives in | Back it up with |
|---|---|---|---|
| Hybrid | one process, one volume | the data directory | `runnel backup` |
| Postgres, MySQL | any number of `runnel serve` processes | the database | the database's own tools (`pg_dump`, PITR, snapshots) |
| Redis, Valkey, Dragonfly | any number of processes | Redis | RDB or AOF, as for any Redis |

The hybrid store is locked to one process. Do not run two replicas on one volume: the second exits with `wal: directory is in use by another process`. Give it its own volume, never a network file system shared with another writer.

Settings for production:

```yaml
server:
  admin_token: ""          # set RUNNEL_ADMIN_TOKEN, 24 characters or more
  shutdown_timeout: 15s
  drain_delay: 5s          # longer than your load balancer's probe interval
log:
  format: json
tracing:
  enabled: true            # with OTEL_EXPORTER_OTLP_ENDPOINT set
```

Memory: set a container memory limit. Runnel keeps Go's heap under 70% of it (`memory.heap_percent`) and leaves the rest to the OS page cache, which holds job bodies. A waiting job takes about 290 bytes of memory; past a quarter of the limit, waiting jobs move to disk. Finished jobs leave memory about a second after they end.

Disk: the log grows until a snapshot replaces it, then shrinks. Finished jobs stay on disk until their retention ends (24 hours for completed and cancelled jobs, 7 days for dead and expired ones). Size the volume for your retention, plus room for one snapshot.

## Probes

| Path | Answers 200 when | Use it for |
|---|---|---|
| `/healthz` | the store answers | liveness |
| `/readyz` | the store answers and the server is not shutting down | readiness |

On `SIGTERM` the server answers `/readyz` with 503 for `drain_delay`, then stops taking new connections and waits up to `shutdown_timeout` for requests in flight, long-polls included (claims wait up to `max_claim_wait`, 30 seconds by default).

Kubernetes:

```yaml
livenessProbe:  {httpGet: {path: /healthz, port: 8700}, periodSeconds: 10, failureThreshold: 3}
readinessProbe: {httpGet: {path: /readyz,  port: 8700}, periodSeconds: 2}
terminationGracePeriodSeconds: 30   # more than drain_delay + shutdown_timeout
```

A hybrid store replays its snapshot and log before the server listens, so a restart after a large backlog takes a moment before the probes pass. Give the liveness probe an `initialDelaySeconds` that covers it.

## Metrics

`/metrics` serves Prometheus metrics:

| Metric | Labels | Meaning |
|---|---|---|
| `runnel_queue_jobs` | namespace, queue, state | jobs in each state now |
| `runnel_jobs_started_total` | namespace, queue, kind | attempts started by workers in this process |
| `runnel_jobs_finished_total` | namespace, queue, kind, outcome | attempts finished, by outcome |
| `runnel_job_duration_seconds` | namespace, queue, kind | attempt run time |
| `runnel_http_requests_total` | route, code | API requests |

Remote workers report their attempts through the API, so `runnel_jobs_*` counts them on the server.

Alerts to start with:

| Alert | Expression | Means |
|---|---|---|
| Backlog growing | `deriv(runnel_queue_jobs{state="waiting"}[15m]) > 0` for 30m | workers or a pool cannot keep up |
| Jobs expiring | `delta(runnel_queue_jobs{state="expired"}[10m]) > 0` | jobs wait past their `start_timeout` or deadline |
| Dead jobs | `delta(runnel_queue_jobs{state="dead"}[10m]) > 0` | jobs failed for good |
| API errors | `sum(rate(runnel_http_requests_total{code=~"5.."}[5m])) > 0` | the store is failing; see below |
| Down | `up == 0` or `/readyz` failing | the process is gone or the store does not answer |

## Tracing

With `tracing.enabled`, every API request is a server span named after its route, sent over OTLP/HTTP. The standard variables configure it: `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME` (default `runnel`), `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`.

A job carries its trace in its meta, as `traceparent` and `tracestate`. An enqueue with a `traceparent` header puts it there; from Go, use `tracing.Job(ctx)`. Workers wrapped in `tracing.Middleware` record each attempt as a child span, and the callback carries the same headers, so one trace runs from the producer to the callback receiver.

## Backup and restore

Hybrid store, while it runs:

```
runnel backup --server http://runnel:8700 -o /backups/runnel-$(date +%F-%H%M).tar
```

The admin token comes from `RUNNEL_ADMIN_TOKEN`. The copy is consistent: it holds every change acknowledged before the backup began, taken while calls go on. A copy cut short leaves no file (the CLI writes `FILE.part` and renames it at the end). With the server stopped, `runnel backup --store hybrid:/var/lib/runnel -o FILE` reads the directory directly.

Restore into an empty directory, with the server stopped:

```
runnel restore /backups/runnel-2026-11-14-0300.tar --store hybrid:/var/lib/runnel
runnel serve --store hybrid:/var/lib/runnel
```

`restore` opens the store once to check that it replays, and prints how many namespaces it found. Jobs that were running at backup time are running in the copy; their leases end and they run again, as after any crash.

Run a backup every hour or more often, keep them off the machine, and restore one into a scratch directory every week. A backup you have never restored is not a backup.

## Upgrades

1. Read the changelog for the new version.
2. Take a backup.
3. Replace the binary or image and restart. Workers and producers retry while it is down; jobs keep their state.

A hybrid data directory holds a `FORMAT` file. A build that cannot read it refuses to start with `hybrid: unsupported data format` and changes nothing; roll back to the version that wrote it. The SQL stores upgrade their schema on start (`runnel migrate` does it alone).

## When something goes wrong

**`/healthz` answers 503, or calls fail with `hybrid: store stopped after a disk error`.** A write to disk failed (full disk, I/O error). The store refuses every call rather than answer with data the disk does not hold. Fix the disk, then restart: it replays what it had acknowledged.

**The disk is filling up.** Check retention: `queue.retention` and `queue.dead_retention`, or the `maintainer.retention` defaults. Make sure a maintainer runs (`--no-maintain` turns it off). A snapshot needs room for a copy of the live jobs while it is written.

**A provider or downstream system is down or rate limiting.** Pause its pool until it should recover, and change nothing else:

```
runnel pool set NS acct-1 --json '{"name":"acct-1","slots":20,"paused_until":"2026-11-14T15:30:00Z"}'
```

Jobs naming the pool wait; running and polling ones go on; other pools keep working. Jobs whose `start_timeout` passes meanwhile end `expired` and the producer is told. To cap a provider's calls per second instead, set the pool's `rate` and `burst`.

**A queue is backing up.** Look at `runnel stats NS`. Waiting jobs with free workers usually mean a limit holds them: a pool's `slots`, a group's `group_max_running`, a `rate`, or a paused queue or pool. Check them with `runnel queue list NS` and `runnel pool list NS`. To refuse new work early instead of queueing it, set `max_queued` or `group_max_queued`: producers get 429 with `Retry-After`.

**Jobs stay `running` after a worker died.** They come back when their lease ends (the lease the worker asked for, 30 seconds by default) and run again. Handlers must be safe to run twice.

**Callbacks fail.** Deliveries retry 10 times with backoff on 408, 429 and 5xx; other answers fail at once. Find them with `runnel jobs NS --queue runnel.callbacks --state dead`, fix the receiver and `runnel retry NS ID`. A receiver on a private address needs `callbacks.allow_private`.

**Rotating the callback secret.** Receivers verify with both secrets first (`runnel.VerifySignature(header, body, 0, now, newSecret, oldSecret)`), then set the new one with `runnel ns create NS --callback-secret NEW`, then drop the old one from receivers once deliveries signed with it are done (a delivery retries for a little over an hour).

**Rotating an API key.** Create a new key, deploy it, then `runnel key delete NS ID`. A running server stops accepting the deleted key within 5 seconds.
