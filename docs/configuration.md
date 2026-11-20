# Configuration

`runnel serve` and the other commands read settings in this order, each overriding the one before:

1. built-in defaults;
2. a YAML file: `--config FILE` or `RUNNEL_CONFIG=FILE` ([example](../deploy/runnel.example.yaml));
3. `RUNNEL_*` environment variables;
4. command-line flags, like `--store` and `--addr`.

A setting's variable is named after its path in upper case, with dots as underscores: `server.max_body` is `RUNNEL_SERVER_MAX_BODY`, `hybrid.min_free_bytes` is `RUNNEL_HYBRID_MIN_FREE_BYTES`. Two keep short names: `server.addr` is `RUNNEL_ADDR`, and `server.admin_token` is `RUNNEL_ADMIN_TOKEN`. Durations are written like `30s`, `5m` or `168h`. A number or duration left at `0` uses the default shown.

Settings that belong to one namespace, like queue limits and pools, are not here: they are stored in Runnel and changed through the API. See [concepts](concepts.md#queues).

## Store

| Key | Default | What it does |
|---|---|---|
| `store` | `sqlite:runnel.db` | where Runnel keeps its data: `postgres://…`, `mysql://…`, `sqlite:FILE`, `redis://…` (also Valkey and Dragonfly), `hybrid:DIR` or `memory:`. See [stores](stores.md) |

## Logs

| Key | Default | What it does |
|---|---|---|
| `log.format` | `text` | `text` or `json` |
| `log.level` | `info` | `debug`, `info`, `warn` or `error` |

## Server

| Key | Default | What it does |
|---|---|---|
| `server.addr` | `:8700` | the address to listen on |
| `server.admin_token` | none | the admin password, at least 24 characters; set it with `RUNNEL_ADMIN_TOKEN` |
| `server.admin_user` | `admin` | the Basic auth user that goes with the admin token |
| `server.auth` | `required` | `none` lets every request in as admin; use it only on a private network |
| `server.tls.cert`, `server.tls.key` | none | PEM certificate and key; with both, Runnel serves https |
| `server.tls.client_ca` | none | client certificates signed by this CA log in as admin |
| `server.tls.require_client_cert` | `false` | refuse clients without a certificate |
| `server.tls.client_names` | any | accept only certificates with these common or DNS names |
| `server.tls.min_version` | `1.2` | the lowest TLS version, `1.2` or `1.3` |
| `server.max_body` | 4 MiB | the largest request body |
| `server.max_batch` | 1000 | the most jobs in one enqueue or flow |
| `server.max_delay` | `8760h` | how far ahead a job may be set to run or to live |
| `server.max_claim` | 100 | the most jobs one claim returns |
| `server.max_claim_queues` | 64 | the most queues one claim names |
| `server.max_claim_wait` | `30s` | the longest a claim long-polls for work |
| `server.max_result_wait` | `60s` | the longest `Prefer: wait` holds a request |
| `server.default_lease` | `30s` | the lease when a claim asks for none |
| `server.min_lease`, `server.max_lease` | `5s`, `10m` | the range a requested lease is held to |
| `server.max_extend` | 10000 | the most jobs in one heartbeat |
| `server.retry_after` | `1s` | the `Retry-After` sent with `202`, `429` and `507` |
| `server.allow_http_callbacks` | `false` | allow `http://` callback URLs, not only `https://` |
| `server.key_cache_ttl` | `5s` | how long API keys are cached; a deleted key stops working on this server within it |
| `server.health_timeout` | `2s` | how long `/healthz` waits for the store |
| `server.read_header_timeout` | `10s` | HTTP server timeout |
| `server.read_timeout` | `1m` | HTTP server timeout |
| `server.write_timeout` | `2m` | HTTP server timeout; keep it above `max_result_wait` and `max_claim_wait` |
| `server.idle_timeout` | `2m` | how long an idle keep-alive connection stays open |
| `server.max_header_bytes` | 65536 | the largest request header |
| `server.shutdown_timeout` | `15s` | how long shutdown waits for requests in flight |
| `server.drain_delay` | `0s` | how long `/readyz` answers `503` before shutdown starts, so load balancers stop sending traffic |

## Maintainer

The maintainer takes back jobs whose worker died, expires jobs past their start timeout or deadline, fires schedules, sends callbacks and deletes old jobs. `runnel serve` runs one; `--no-maintain` turns it off.

| Key | Default | What it does |
|---|---|---|
| `maintainer.enabled` | `true` | run the maintainer in this server |
| `maintainer.interval` | `1s` | how often it checks leases, deadlines and schedules |
| `maintainer.prune_every` | `1m` | how often it deletes finished jobs and old events |
| `maintainer.batch` | 500 | how many rows each step handles at a time |
| `maintainer.archive` | `false` | copy each job to an archive table (`rn_archive` on SQL stores) before deleting it |
| `maintainer.retention.completed` | `24h` | how long completed jobs are kept |
| `maintainer.retention.cancelled` | `24h` | how long cancelled jobs are kept |
| `maintainer.retention.dead` | `168h` | how long dead and expired jobs are kept |
| `maintainer.retention.events` | `24h` | how long events are kept |
| `maintainer.retention.undelivered` | `168h` | how long an event whose callback was never made is kept |

A queue's own `retention` and `dead_retention` win over these.

## Callbacks

| Key | Default | What it does |
|---|---|---|
| `callbacks.enabled` | `true` | send callbacks from this server's maintainer |
| `callbacks.concurrency` | 4 | how many callbacks are sent at once |
| `callbacks.timeout` | `10s` | how long one delivery may take |
| `callbacks.allow_private` | `false` | allow callbacks to private and loopback addresses |
| `callbacks.max_attempts` | 10 | delivery attempts before giving up |
| `callbacks.backoff_base`, `callbacks.backoff_max` | `5s`, `30m` | the wait between attempts |
| `callbacks.max_retry_after` | `1h` | the longest `Retry-After` a receiver may ask for |
| `callbacks.unique_for` | `48h` | an event seen twice in this window is posted once |
| `callbacks.secret_ttl` | `30s` | how long namespace secrets are cached |
| `callbacks.max_payload` | `limits.max_payload` + 64 KiB | the largest delivery body. A delivery that would be larger is sent without the job's result |

## Limits

| Key | Default | What it does |
|---|---|---|
| `limits.max_payload` | 1048576 | the largest job payload in bytes |
| `limits.max_depends_on` | 256 | the most dependencies one job may have |
| `limits.max_meta` | 32 | the most meta labels on a job |
| `limits.max_meta_key`, `limits.max_meta_value` | 64, 512 | the longest meta key and value |
| `limits.max_unique_key` | 255 | the longest unique key |
| `limits.max_callback_url` | 2048 | the longest callback URL |

## Memory and tracing

| Key | Default | What it does |
|---|---|---|
| `memory.heap_percent` | 70 | Go's heap target, as a share of the container's memory limit |
| `tracing.enabled` | `false` | send OpenTelemetry spans over OTLP/HTTP; set `OTEL_EXPORTER_OTLP_ENDPOINT` and other `OTEL_*` variables |
| `tracing.flush_timeout` | `5s` | how long shutdown waits to send the last spans |

## Hybrid store

These apply only to `hybrid:` stores. The defaults suit most uses; [DESIGN.md](DESIGN.md) explains the parts.

| Key | Default | What it does |
|---|---|---|
| `hybrid.sync` | `each` | when writes reach the disk: `each` before every answer, `periodic` every `sync_interval`, `off` left to the OS. The store URL's `?sync=` wins |
| `hybrid.sync_interval` | `100ms` | the flush interval for `periodic` and `off` |
| `hybrid.snapshot_bytes` | 67108864 | the log size that starts a snapshot |
| `hybrid.check_every` | `1s` | how often the log size is checked |
| `hybrid.min_free_bytes` | 67108864 | new jobs are refused with `507` while the disk has less free space than this |
| `hybrid.disk_check_every` | `100ms` | how often free space is measured |
| `hybrid.hot_percent` | 25 | the share of the memory limit waiting jobs may take in memory |
| `hybrid.job_bytes` | 300 | the memory one waiting job is counted at |
| `hybrid.hot_jobs` | from `hot_percent` | a fixed number of waiting jobs to keep in memory |
| `hybrid.min_hot_jobs` | 16384 | the fewest waiting jobs kept in memory |
| `hybrid.no_limit_hot_jobs` | 1048576 | waiting jobs kept in memory when there is no memory limit |
| `hybrid.retire_after` | `1s` | how long a finished job stays in memory before moving to disk |
| `hybrid.retire_every` | `1s` | how often finished jobs move |
| `hybrid.retire_batch` | 4096 | how many move at a time |
| `hybrid.spill_every` | `100ms` | how often waiting jobs past the memory budget move to disk |
| `hybrid.spill_batch` | 4096 | how many move at a time |
| `hybrid.refill_batch` | 8192 | how many waiting jobs come back from disk at a time |
| `hybrid.refill_ahead` | 32768 | how far ahead of workers jobs come back |
| `hybrid.batch_bytes` | 4194304 | the most bytes moved in one batch |
| `hybrid.max_refills` | 8 | refills running at once |
| `hybrid.wake_batch` | 65536 | the most jobs made due at once |
| `hybrid.cold_cache_bytes` | 16777216 | the disk store's block cache |
| `hybrid.cold_memtable_bytes` | 8388608 | the disk store's write buffer |
| `hybrid.cold_bloom_bits` | 10 | bloom filter bits per key in the disk store |
