# Scheduled chores

Cleanup, syncs and reports often run on a timer. A schedule makes Runnel enqueue them, so you need no cron job and no leader election in your service.

## Make a schedule

```go
c.SetSchedule(ctx, runnel.Schedule{
	Name:    "cleanup",
	Spec:    "@every 1m",
	Queue:   "house",
	Kind:    "cleanup.old-files",
	Overlap: runnel.OverlapSkip,
})
```

```sh
curl -u admin:$RUNNEL_ADMIN_TOKEN -X PUT localhost:8700/v1/ns/shop/schedules/cleanup \
  -d '{"name":"cleanup","spec":"@every 1m","queue":"house","kind":"cleanup.old-files","overlap":"skip"}'
```

From another Go service, `remote.Client` has the same `SetSchedule`, `Schedules`, `UpdateSchedule` and `DeleteSchedule`.

## Specs

- Five-field cron, like `0 3 * * *` for 03:00 every day.
- Descriptors: `@hourly`, `@daily`, `@weekly`, `@every 10m`, `@every 1s`.
- `timezone`, like `Europe/Rome`, for specs tied to local time.

A maintainer fires schedules on each tick, every second by default, so a firing reaches the queue within a tick or two. Each firing is enqueued once, however many maintainers run. Missed firings, such as during a restart, become one job, not a burst. Each job carries `schedule` and `fire_at` in its meta.

## When a chore runs long

| `overlap` | A firing while the last job is still waiting or running |
|---|---|
| `queue` (default) | is enqueued too, so jobs line up |
| `skip` | is dropped, logged at debug only |

Use `skip` for chores where a late run has no value, because the next run does the same work. To keep two different chores from running at the same time, also give them a pool with one slot: `Pool: "house"` on each schedule and `slots: 1` on the pool.

Pause a schedule with `paused: true`. The rules are T02 in [SPEC.md](../SPEC.md).
