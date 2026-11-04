# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, minor versions may change the API.

## [Unreleased]

### Changed

- Pools are now resources: `ResourceConfig`, `SetResource`, `runnel.Resource`, the `resource` job field and `/v1/ns/{ns}/resources`.
- Limits use one pattern: `max_running` and `tenant_max_running` (were `concurrency`, `tenant_slots` and pool `slots`), `max_queued` and `tenant_max_queued` (were `max_waiting` and `tenant_waiting`).

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
