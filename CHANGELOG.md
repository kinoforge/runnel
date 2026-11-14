# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, minor versions may change the API.

## [Unreleased]

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

[Unreleased]: https://github.com/kinoforge/runnel/compare/v0.0.1-alpha...HEAD
[0.0.1-alpha]: https://github.com/kinoforge/runnel/releases/tag/v0.0.1-alpha
