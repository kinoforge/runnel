# Safe re-enqueue

Networks fail, clients retry, and users click twice. A unique key makes sure one request makes one job.

## Give the job a key

```go
res, err := c.Enqueue(ctx, "payments", "charge", in, runnel.Unique("order-981:charge", 0))
if res.Duplicate {
	log.Printf("already queued as %s", res.ID)
}
```

```sh
curl localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $APP_KEY" \
  -d '{"queue":"payments","kind":"charge","unique_key":"order-981:charge","payload":{}}'
```

A second enqueue with the same key returns the first job's id, marked as a duplicate, and adds nothing.

## How long the key holds

| Window | The key is taken |
|---|---|
| `0` (none) | while the job has not finished; once it ends, the key is free again |
| a duration, like `24h` | from enqueue until the window ends, even after the job finished |

Use no window to avoid running the same thing twice at once. Use a window to run something at most once a day, like a nightly sync per customer: `runnel.Unique("sync:customer-42:2026-11-15", 24*time.Hour)`.

## Delivery is at least once

A unique key stops duplicate jobs, not duplicate runs. If a worker dies after doing the work but before reporting it, the job runs again. Make handlers safe to repeat, for example by using `j.ID` as the idempotency key when calling a payment provider.

The rule is J06 in [SPEC.md](../SPEC.md); the story is U11.
