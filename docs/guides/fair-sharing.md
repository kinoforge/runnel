# Fair sharing

When many customers share one queue, one of them can add thousands of jobs at once and everyone else waits behind them. Groups stop that.

## Give jobs a group

A group is any id: a customer, an API key, a project. Put it on each job:

```go
c.Enqueue(ctx, "reports", "report.build", in, runnel.Group("customer-42"))
```

```sh
curl localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $APP_KEY" \
  -d '{"queue":"reports","kind":"report.build","group":"customer-42","payload":{}}'
```

## Cap each group

Set the caps on the queue:

```go
c.SetQueue(ctx, runnel.QueueConfig{Name: "reports", GroupMaxRunning: 2, GroupMaxQueued: 1000})
```

- `group_max_running: 2`: one customer runs at most 2 reports at once. When a customer is at its cap, workers skip its jobs and take the next customer's.
- `group_max_queued: 1000`: one customer can have at most 1000 waiting; more are refused with `429`.
- `group_rate` and `group_burst` limit how fast each customer's jobs start.

A pool can cap groups too, with `group_slots`: how many of the pool's slots one group may hold.

## What you get

A customer that adds 30 jobs never has more than 2 running, and a customer that adds 2 jobs starts them right away instead of waiting behind the 30. Runnel knows nothing about your customers; the group is just a label with caps.

The story U02 in [SPEC.md](../SPEC.md) checks this on every store.
