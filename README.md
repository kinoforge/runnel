# Runnel

[![ci](https://github.com/kinoforge/runnel/actions/workflows/ci.yml/badge.svg)](https://github.com/kinoforge/runnel/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/kinoforge/runnel.svg)](https://pkg.go.dev/github.com/kinoforge/runnel)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

## What it is

Runnel is a job queue manager. Your service puts jobs in, and your workers take them out and run them. Runnel manages everything in between:

- it keeps every job under an id until the job ends;
- it picks the next job by priority, then by age;
- it holds every limit across all workers: slots, pools, groups, rate, burst and pause;
- it retries failed jobs with backoff;
- it ends jobs that wait too long or miss their deadline, and tells you.

It is made for slow work that can fail: calling another company's API, generating an image, sending emails, collecting data.

| Who | Decides |
|---|---|
| Your service | which jobs to add, and each job's priority, group, pool, attempts and timeouts |
| Your workers | how many jobs they run at once, and how each job ends: done, failed, try again later, or still running somewhere else |
| You or an operator | queue and pool settings: slots, rate, burst, caps and pause, through the API or the [dashboard](https://github.com/kinoforge/runnel-dashboard) |
| Runnel | which job runs next, and that every limit holds |

Runnel knows nothing about your business: customers, prices and providers stay in your service. It does not start or scale workers either. Your app decides how many run.

## Why use it

Because it is fair, and it keeps you inside your limits.

- **One busy customer cannot block the others.** Give each job a group, like a customer id, and cap how many jobs each group runs at once.
- **You never go over a provider's limits.** A pool holds the number of calls one account allows at a time, and how many per second. Pause it while the provider is down. Other pools keep working.
- **No new database needed.** Runnel uses the Postgres, MySQL, SQLite or Redis you already run, or keeps everything itself on a local disk.
- **Any language.** Use it as a Go library, or run `runnel serve` and talk to it over HTTP.

## How it works

You enqueue a job. Runnel keeps it until a worker with a free slot claims it, and makes sure every limit you set still holds. The worker runs your handler and reports back: done, failed, try again later, or still running somewhere else. You get the result by waiting for it, by asking for it later, or by a callback to your URL.

In Go:

```
go get github.com/kinoforge/runnel
```

```go
store, _ := postgres.Open("postgres://localhost/app")
store.Migrate(ctx)

c := runnel.NewClient(store, "shop")
c.Enqueue(ctx, "emails", "welcome", map[string]string{"to": "ana@example.com"})

w := runnel.NewWorker(store, "shop", runnel.WorkerConfig{Queues: []string{"emails"}})
w.HandleFunc("welcome", func(ctx context.Context, j *runnel.Job) error {
	return sendWelcome(ctx, j.Payload)
})
w.Run(ctx)
```

Over HTTP, from any language:

```
runnel serve --store postgres://localhost/app

curl localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $KEY" \
  -d '{"queue":"emails","kind":"welcome","payload":{"to":"ana@example.com"}}'
```

## Learn more

Read in this order:

| | |
|---|---|
| [Getting started](docs/getting-started.md) | run Runnel and your first job in ten minutes |
| [Concepts](docs/concepts.md) | jobs, states, queues, pools, groups, leases, retries and deadlines |
| [Guides](docs/guides/README.md) | callbacks, rate-limited providers, async work, pipelines, fair sharing, schedules and more |
| [Features](docs/features.md) | everything Runnel does, with a full example |
| [HTTP API](docs/http-api.md) | every endpoint, its role and its errors; the schema is [openapi.yaml](server/openapi.yaml) |
| [SDKs](docs/sdks.md) | Go today, other languages next |
| [Configuration](docs/configuration.md) | every setting of `runnel serve` |
| [Stores](docs/stores.md) | which store to pick, connection limits, the hybrid store |
| [Runbook](docs/RUNBOOK.md) | running it in production: probes, metrics, tracing, backups, upgrades |
| [Spec](docs/SPEC.md) | every rule Runnel follows, each checked by a test |
| [Design](docs/DESIGN.md) | why it works the way it does, and its known limits |

Runnel is in beta. The model is settled, but the API may still change before 1.0. See the [changelog](CHANGELOG.md).

## Development

```
make up                       # postgres, mysql, redis, valkey and dragonfly on local ports
make test-box PKG=./server    # tests in a container with CPU, memory and disk caps
make drill                    # failure drills: kill, network cut, full disk, restart mid-write, restore
make test-all                 # every backend with the race detector; heavy, CI runs it
make lint vuln fuzz
make bench                    # RUNNEL_STORE=... make bench
```

See [CONTRIBUTING.md](.github/CONTRIBUTING.md). Security reports go through [SECURITY.md](.github/SECURITY.md).

## Roadmap

- A gRPC and Connect API, and SDKs for TypeScript, Python, Java and Kotlin, and PHP. See [SDKs](docs/sdks.md).
- Worker labels, so jobs can ask for workers with certain abilities, like a GPU or a region.
- Per-group ready sets on Redis, removing the 2,000-job examination bound.

## License

Apache 2.0. Copyright 2026 The Runnel Authors. Free for any use, including commercial.

## Benchmarks

On a 4-core desktop, Redis runs about 6,000 jobs a second and Postgres about 1,200. The hybrid store runs over 100,000 a second when it flushes to disk every 100 ms. See the [benchmarks](docs/benchmark/index.md) for the full tables, the hardware, and how to run them yourself.
