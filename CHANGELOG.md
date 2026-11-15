# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, minor versions may change the API.

## [Unreleased]

### Added

- User stories (`U` rules in SPEC.md) that walk whole uses of Runnel on every backend and over HTTP, boundary tests for each rule, and failure drills (`make drill`): a kill under load, a network cut, a full disk, kills during writes and a restore from backup.

### Fixed

- A remote worker measures its jobs' deadlines on the server's clock, which claim and heartbeat answers now carry as `now`. Before, it used its own clock, so a worker whose clock was off stopped handlers early or late.
- A worker stopped while a job runs now writes the job back before `Run` returns. Before, the write used the cancelled shutdown context and failed, so the job stayed `running` until its lease ended and then lost an attempt.
- A running job's deadline is measured on the store's clock, like leases. Before, the worker used its own clock, so with the store's clock behind, the handler stopped early and the job ended `dead` instead of `expired`; with it ahead, the handler ran past the deadline. A job stopped by its deadline now always ends `expired`.
- A heartbeat answered after a job finished and was claimed again by the same worker, as polls are, no longer cancels the new run. Before, the new run's result was dropped and the job stayed `running` until its lease ended.
- A job retried after a failed attempt counts its start timeout again from when it is due, as T01 says. Before, the retry cleared the start deadline, so a retried job held back by a full pool could wait forever.
- On Redis, Valkey and Dragonfly, jobs of the same priority are claimed by start time to the millisecond, as on the other stores. Before, start times counted only to the second, so jobs due in the same second ran in id order. `Migrate` re-scores the waiting jobs of a store made by an older version once; upgrade every server before relying on the new order.

## [0.0.3-beta.3] - 2026-11-14

### Added

- A queue's `callback_url` is the callback of every job in it that sets none, so producers no longer add it to each job. Over the API it must use https unless the server allows http, like a job's.
- `Worker.SetConcurrency` changes a running worker's number of slots, and `Worker.Concurrency` reads it. Growing claims more jobs at once; shrinking lets running jobs finish first. The app decides when, for example from its own admin page.

### Changed

- The README says what Runnel is and who decides what, and has no question headings.

## [0.0.3-beta.2] - 2026-11-14

### Added

- `PATCH` on queues, pools and schedules takes an RFC 7396 merge patch: it changes only the fields named, `null` clears one, and a missing setting is created.
- Reading or setting one queue, pool or schedule answers with an `ETag`. `PUT` and `PATCH` honour `If-Match` and `If-None-Match: *`, and answer 412 when the setting changed.
- `UpdateQueue` and `UpdatePool` on `runnel.Client` and `remote.Client` change a setting from a fresh read and try again when another writer got there first. `runnel.ErrStale` is the 412.
- `Store.SwapConfig` writes a setting only if it still holds the value read. Every store has it.

### Fixed

- Two writers of one pool no longer undo each other: an app that sets slots keeps a pause set from elsewhere, when both use `UpdatePool` or `PATCH`.
- Hybrid store: a job that got a dependant while it was being moved to disk lost the link, so the dependant stayed blocked after the job ended. It now stays in memory instead.

## [0.0.3-beta.1] - 2026-11-14

### Added

- `GET /v1/system` (admin token) reports the version, start time, Go memory and the store's storage: bytes on disk for the hybrid store, the database size for Postgres, MySQL and SQLite, and used memory for Redis.
- Basic auth next to bearer tokens: user `admin` (`server.admin_user`) with the admin token as password, or a key's id and secret.
- `server.auth: none` turns auth off for private networks, with a warning at start.
- Built-in TLS: `server.tls.cert` and `key`, and client certificates with `client_ca`, `require_client_cert` and `client_names`.
- `remote.Open` and `remote.ParseURL` connect with one URL, such as `https://admin:PASSWORD@host:8700?ca=ca.pem`, with optional `ca`, `cert` and `key`.

### Changed

- `runnel backup --server` reads `RUNNEL_URL` instead of `RUNNEL_SERVER`, and takes credentials and TLS files from the URL.

## [0.0.2-alpha] - 2026-11-14

### Added

- Settings for what was fixed before. Server: `max_batch`, `max_delay`, `max_claim_queues`, `max_claim`, `default_lease`, `min_lease`, `max_lease`, `max_extend`, `key_cache_ttl`, `health_timeout`. Callbacks: `max_attempts`, `backoff_base`, `backoff_max`, `max_retry_after`, `unique_for`, `secret_ttl`. Limits: `max_meta_key`, `max_meta_value`. Tracing: `flush_timeout`.
- SQL connection pool in the store URL: `max_conns`, `max_idle_conns`, `conn_max_lifetime`, `conn_max_idle_time`, for databases that allow only a few connections.
- `remote.Client.WithWaits` sets how long the client waits for results and claims.
- `make test-box` runs tests in a container with caps on CPU, memory, processes and disk operations.
- Docs split into features, stores and benchmarks pages, with a shorter README.

### Changed

- `runnel.CallbackReader` replaces `ReadCallback`, with a settable body limit.
- `NewCallbackJob` takes a `CallbackConfig` instead of a queue name.
- Go 1.27.4 and the latest modules, tools and GitHub Actions.
- Releases are signed as cosign bundles. Verify with `cosign verify-blob --bundle checksums.txt.sigstore.json`.
- A heartbeat may extend 10,000 jobs by default instead of 1,000, so remote workers with more than 1,000 slots keep their leases.

### Fixed

- The Docker image is built with Go 1.27.4, which fixes the `net/http` crash (GO-2026-6859). The 0.0.1-alpha image was built with 1.26.10.

## [0.0.1-alpha] - 2026-11-14

First release, for trying Runnel out. The model is settled; the API may still change before 1.0.

### Added

- Job queue library and HTTP server, on Postgres, MySQL 8, SQLite, Redis, Valkey, Dragonfly and the hybrid store, all checked by one shared conformance suite.
- Hybrid store (`hybrid:DIR`, or `memory:` for no disk): a small record per job in memory, payloads and results on disk read through the OS page cache, every change in a write-ahead log, snapshots written without pausing calls. Finished jobs, and waiting jobs past a quarter of the memory limit, move to an embedded Pebble store on disk. `?sync=each|periodic|off` picks durability.
- Hybrid backups while calls go on: `GET /v1/backup` (admin token), `runnel backup` and `runnel restore`, `hybrid.Store.Backup` and `hybrid.Restore`.
- A `FORMAT` file in each hybrid data directory; a build refuses a directory written in a format it does not read.
- Namespaces: one isolated queue system per service, with its own API keys and roles (admin, producer, worker, reader).
- Queues with `max_running`, `group_max_running`, `max_queued`, `group_max_queued`, token-bucket `rate` and `group_rate`, pause, retries with backoff, timeouts, retention and a default `wait`.
- Pools: `slots` and `group_slots` shared by every queue whose jobs name the pool, a token-bucket `rate`, and `paused` or `paused_until` to hold its jobs back while the rest run.
- Groups: a plain id per job that every group limit counts by.
- Outcomes beyond retry: fail, busy without using an attempt, pending with polling of an outside task, and release.
- `start_timeout`, `deadline` and `ttl`: a job past them ends `expired`, with an event and its callback.
- Unique jobs, workflows with dependencies, and cron schedules.
- Callbacks signed with HMAC-SHA256, retried, never redirected and never sent to private addresses. `runnel.VerifySignature` and `runnel.ReadCallback` check them, with several secrets while one is rotated.
- Waiting for results: `Prefer: wait=N` on enqueue and read, `Client.Wait`, `EnqueueAndWait`, and direct start with `Worker.Do`.
- Enqueue inside your own transaction on the SQL stores (`Client.EnqueueTx`).
- Fair claiming: a group or pool at its limit never blocks the jobs behind it.
- A store clock, so every process measures leases on the database's or Redis's time.
- Remote worker protocol over HTTP with long-poll claims and leases, and a Go client for it, which also sets queues and pools.
- `/healthz` for liveness and `/readyz` for readiness; on shutdown `/readyz` answers 503 for `server.drain_delay` first.
- OpenTelemetry tracing: `tracing.Handler` for API requests (`tracing.enabled` in `runnel serve`), `tracing.Middleware` for worker attempts and `tracing.Job` for producers. A job carries `traceparent` in its meta from the enqueue to the worker and its callback.
- Prometheus metrics, OpenAPI 3.1 document at `/openapi.yaml`, `runnel bench`, and signed multi-platform releases with SBOMs.
- SPEC.md, every rule with an ID and the tests that check it; DESIGN.md, the decisions behind them; RUNBOOK.md, how to operate it.

[Unreleased]: https://github.com/kinoforge/runnel/compare/v0.0.3-beta.3...HEAD
[0.0.3-beta.3]: https://github.com/kinoforge/runnel/compare/v0.0.3-beta.2...v0.0.3-beta.3
[0.0.3-beta.2]: https://github.com/kinoforge/runnel/compare/v0.0.3-beta.1...v0.0.3-beta.2
[0.0.3-beta.1]: https://github.com/kinoforge/runnel/compare/v0.0.2-alpha...v0.0.3-beta.1
[0.0.2-alpha]: https://github.com/kinoforge/runnel/compare/v0.0.1-alpha...v0.0.2-alpha
[0.0.1-alpha]: https://github.com/kinoforge/runnel/releases/tag/v0.0.1-alpha
