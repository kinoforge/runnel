# Getting started

In ten minutes you will run Runnel, give a service its own namespace and keys, enqueue a job, run it with a worker, and get its result. Pick one of two paths:

- **Server:** run `runnel serve` and talk to it over HTTP from any language. Start here if your services are not all in Go.
- **Go library:** link Runnel into your Go service and keep the queue in a database you already run.

## Path 1: the server

### 1. Start Runnel

Runnel ships as one binary and as a container image. With Docker:

```sh
export RUNNEL_ADMIN_TOKEN=$(openssl rand -hex 24)

docker run -d --name runnel -p 8700:8700 \
  -e RUNNEL_ADMIN_TOKEN \
  -v runnel-data:/data \
  ghcr.io/kinoforge/runnel:0.0.3-beta.5 serve --store hybrid:/data
```

`hybrid:/data` keeps everything in the container's `/data` volume, with no database to run. To use a database you already have, pass its URL instead, like `--store postgres://user:pass@host/db`. [Stores](stores.md) compares them.

Check that it is up:

```sh
curl localhost:8700/healthz
```

Without Docker, download the binary for your platform from the [releases page](https://github.com/kinoforge/runnel/releases), or build it with `go install github.com/kinoforge/runnel/cmd/runnel@latest`, then run `runnel serve --store hybrid:./data`.

### 2. Make a namespace and keys

A namespace is one service's own set of queues, settings and keys. The admin token logs in as the user `admin`:

```sh
curl -u admin:$RUNNEL_ADMIN_TOKEN -X PUT localhost:8700/v1/namespaces/shop \
  -d '{"callback_secret":"'$(openssl rand -hex 16)'"}'
```

Make one key for the app that adds jobs and one for the workers. Each answer holds the key once; store it like a password:

```sh
APP_KEY=$(curl -s -u admin:$RUNNEL_ADMIN_TOKEN localhost:8700/v1/namespaces/shop/keys \
  -d '{"name":"app","roles":["producer","reader"]}' | jq -r .key)

WORKER_KEY=$(curl -s -u admin:$RUNNEL_ADMIN_TOKEN localhost:8700/v1/namespaces/shop/keys \
  -d '{"name":"workers","roles":["worker"]}' | jq -r .key)
```

The roles are `producer` (add and cancel jobs), `reader` (read jobs and stats), `worker` (claim and finish jobs) and `admin` (settings and retries). [Concepts](concepts.md#namespaces-and-keys) has the details.

### 3. Enqueue a job

```sh
curl localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $APP_KEY" \
  -d '{"queue":"emails","kind":"welcome","payload":{"to":"ana@example.com"}}'
```

The answer is `201` with the job's `id`. The job now waits in the queue `emails`. A queue exists as soon as a job uses it; settings are optional.

### 4. Run it with a worker

A worker repeats three calls: claim jobs, keep their leases alive while working, and report how each ended. Here is one round with curl:

```sh
curl -s localhost:8700/v1/ns/shop/claim -H "Authorization: Bearer $WORKER_KEY" \
  -d '{"worker":"w1","queues":["emails"],"limit":1,"wait":"10s"}'
```

The answer lists the claimed jobs, each with its `id` and `version`. When the work is done, report it:

```sh
curl localhost:8700/v1/ns/shop/jobs/$JOB_ID/finish -H "Authorization: Bearer $WORKER_KEY" \
  -d '{"worker":"w1","version":'$VERSION',"outcome":{"kind":"done","result":{"sent":true}}}'
```

In practice a library does this loop for you. In Go:

```go
c, _ := remote.Open("http://"+workerKeyID+":"+workerKeySecret+"@localhost:8700", "shop")
w := remote.NewWorker(c, runnel.WorkerConfig{Queues: []string{"emails"}, Concurrency: 4})
w.HandleFunc("welcome", func(ctx context.Context, j *runnel.Job) error {
	var in struct{ To string }
	if err := j.Bind(&in); err != nil {
		return runnel.Fail(err)
	}
	return j.SetResult(map[string]bool{"sent": sendWelcome(ctx, in.To) == nil})
})
w.Run(ctx)
```

A key `rnk_<id>_<secret>` logs in as the user `<id>` with the password `<secret>`. A full program is in [`examples/remote`](../examples/remote/main.go). SDKs for other languages are on the way: see [SDKs](sdks.md).

### 5. Get the result

Read the job at any time:

```sh
curl localhost:8700/v1/ns/shop/jobs/$JOB_ID -H "Authorization: Bearer $APP_KEY"
```

Or wait for it when you enqueue. With `Prefer: wait=30`, the answer comes when the job ends, up to 30 seconds:

```sh
curl -i localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $APP_KEY" -H "Prefer: wait=30" \
  -d '{"queue":"emails","kind":"welcome","payload":{"to":"ana@example.com"}}'
```

You get `200` with the finished job, or `202` with a `Location` to wait on again. Runnel can also post the result to your URL when the job ends: see [callbacks](guides/callbacks.md).

## Path 2: the Go library

```sh
go get github.com/kinoforge/runnel
```

The library talks to the store directly, so producers and workers in Go need no server:

```go
store, err := postgres.Open("postgres://localhost/app")
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil {
	return err
}

c := runnel.NewClient(store, "shop")
if _, err := c.Enqueue(ctx, "emails", "welcome", map[string]string{"to": "ana@example.com"}); err != nil {
	return err
}

w := runnel.NewWorker(store, "shop", runnel.WorkerConfig{Queues: []string{"emails"}})
w.HandleFunc("welcome", func(ctx context.Context, j *runnel.Job) error {
	return sendWelcome(ctx, j.Payload)
})
go runnel.NewMaintainer(store, runnel.MaintainerConfig{}).Run(ctx)
return w.Run(ctx)
```

The maintainer does the housekeeping: it takes back jobs whose worker died, ends jobs past their deadlines, fires schedules, sends callbacks and deletes old jobs. Run one or more anywhere; `runnel serve` runs one for you. A full program is in [`examples/basic`](../examples/basic/main.go).

## Next

- [Concepts](concepts.md): jobs, states, queues, pools, groups, leases, retries and deadlines.
- [Guides](guides/README.md): one page per common job, from callbacks to rate-limited providers.
- [Configuration](configuration.md): every setting of `runnel serve`.
- [HTTP API](http-api.md): every endpoint, with curl.
- [Runbook](RUNBOOK.md): running it in production.
