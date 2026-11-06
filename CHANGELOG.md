# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, minor versions may change the API.

## [Unreleased]

### Added

- Hybrid store: a small record per job in memory, payloads and results on disk read through the OS page cache, every change in a write-ahead log, snapshots written without pausing calls. Open it with `hybrid:DIR` (`?sync=each|periodic|off`), or `memory:` for no disk.
- `store/hybrid/wal`, a group-commit write-ahead log usable on its own.
- `store/hybrid/cold`: finished jobs and their events leave memory for an embedded Pebble store on disk, so retention costs disk, not memory.
- Hybrid store: waiting jobs past a quarter of the memory limit wait in the cold store in claim order and come back in batches, so a backlog is bounded by disk. Inserts slow down while memory holds twice that many.
- `runnel` keeps Go's heap under 70% of the container's memory limit unless `GOMEMLIMIT` is set.
- Config sections `memory` and `hybrid` override every size and timing of the hybrid store; `hybrid.Options` and `cold.Options` take them in Go, defaults in `hybrid.DefaultOptions`.
- `runnel bench -payload` sets the payload size.

### Changed

- Tenants are now groups: a plain id with `group_max_queued`, `group_max_running`, `group_rate` and `group_burst`, the `group` job field and `runnel.Group`; `ErrGroupFull`.
- Pools keep their name; a pool's limits are `slots` and `group_slots`.
- `start_within` is now `start_timeout`.
- Webhooks are now callbacks: the job's `callback_url` (`runnel.Callback`), the namespace `callback_secret`, and the `callbacks` config section.
- A busy worker claims once a quarter of its slots are free, or after 2ms, so claims take jobs in batches.
- Queue limits use one pattern: `max_running` and `group_max_running` (were `concurrency` and `tenant_slots`), `max_queued` and `group_max_queued` (were `max_waiting` and `tenant_waiting`).

## [0.0.1]

First public release.

### Added

- Job queue library and HTTP server, on Postgres, MySQL 8, SQLite, Redis, Valkey and Dragonfly, all checked by one shared conformance suite.
- Namespaces: one isolated queue system per service, with its own API keys and roles (admin, producer, worker, reader).
- Queues with concurrency, tenant slots, waiting caps, rate limits, pause, retries with backoff, timeouts and retention.
- Pools: one limit shared by every queue that uses a resource.
- Outcomes beyond retry: fail, fallback to the next target, busy without using an attempt, pending with polling of an outside task, and release.
- Start-within deadlines that move a job to its fallback when it waits too long.
- Unique jobs, workflows with dependencies, cron schedules, and signed webhooks with SSRF protection.
- Waiting for results: `Prefer: wait=N` on enqueue and read, `Client.Wait`, `EnqueueAndWait`, and direct start with `Worker.Do`.
- Enqueue inside your own transaction on the SQL stores (`Client.EnqueueTx`).
- Fair claiming: a tenant or pool at its limit never blocks the jobs behind it.
- A store clock, so every process measures leases on the database's or Redis's time.
- Remote worker protocol over HTTP with long-poll claims and leases, and a Go client for it.
- OpenAPI 3.1 document, served at `/openapi.yaml`.
- Prometheus metrics, `runnel bench`, and signed multi-platform releases with SBOMs.
- SPEC.md, every rule with an ID and the tests that check it, and DESIGN.md, the decisions behind them.

[Unreleased]: https://github.com/kinoforge/runnel/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/kinoforge/runnel/releases/tag/v0.0.1
