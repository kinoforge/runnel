# Rate-limited providers

When jobs call an outside API, the API sets the limits: so many calls at once, so many a second, and sometimes it fails or goes down. A pool holds those limits for every worker together.

## One pool per account

Make a pool with the provider's limits, and name it on every job that calls it:

```go
c.SetPool(ctx, runnel.PoolConfig{Name: "acme-api", Slots: 5, Rate: 2, Burst: 4})
c.Enqueue(ctx, "sync", "orders.fetch", in, runnel.Pool("acme-api"))
```

Over HTTP:

```sh
curl -u admin:$RUNNEL_ADMIN_TOKEN -X PUT localhost:8700/v1/ns/shop/pools/acme-api \
  -d '{"name":"acme-api","slots":5,"rate":2,"burst":4}'

curl localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $APP_KEY" \
  -d '{"queue":"sync","kind":"orders.fetch","pool":"acme-api","payload":{"shop":42}}'
```

At most 5 of these jobs run at once, starting at most 2 a second, however many workers you run and whatever queues the jobs sit in. Jobs of other pools are not held up.

## When a call fails

Tell Runnel what the failure means:

```go
w.HandleFunc("orders.fetch", func(ctx context.Context, j *runnel.Job) error {
	res, err := acme.Fetch(ctx, j.Payload)
	switch {
	case errors.Is(err, acme.ErrRateLimited):
		return runnel.Busy(30 * time.Second)
	case errors.Is(err, acme.ErrUnavailable):
		return runnel.RetryIn(time.Minute, err)
	case errors.Is(err, acme.ErrBadRequest):
		return runnel.Fail(err)
	case err != nil:
		return err
	}
	return j.SetResult(res)
})
```

- `Busy` waits without using an attempt: the provider said "not now", not "this failed".
- `RetryIn` uses an attempt and waits exactly as long as you say.
- `Fail` stops: retrying a bad request will not help.
- Any other error retries with backoff.

## When the provider is down

Pause the pool until it should recover. Its jobs wait; every other pool keeps going:

```go
c.UpdatePool(ctx, "acme-api", func(p *runnel.PoolConfig) { p.PausedUntil = time.Now().Add(10 * time.Minute) })
```

```sh
curl -u admin:$RUNNEL_ADMIN_TOKEN -X PATCH -H 'Content-Type: application/merge-patch+json' \
  localhost:8700/v1/ns/shop/pools/acme-api -d '{"paused_until":"2026-11-14T18:00:00Z"}'
```

`UpdatePool` and `PATCH` change only the fields you send. So if your app measures and sets `slots` while an operator pauses the pool, both changes stay. Send `If-Match` with the `ETag` of your last read to refuse writing over a newer pool (`412`).

## Waiting too long

If a job should not wait forever behind a full or paused pool, give it a `start_timeout`. It ends `expired`, with an event and a callback, and your service decides what to do instead:

```go
c.Enqueue(ctx, "sync", "orders.fetch", in, runnel.Pool("acme-api"), runnel.StartTimeout(time.Minute))
```

The stories U02 and U03 in [SPEC.md](../SPEC.md) run this on every store.
