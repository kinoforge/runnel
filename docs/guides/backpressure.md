# Backpressure

If producers add jobs faster than workers finish them, a queue grows without end. Runnel can refuse new jobs instead, and tells producers when to try again.

## Cap a queue

```go
c.SetQueue(ctx, runnel.QueueConfig{Name: "emails", MaxQueued: 10000, GroupMaxQueued: 500})
```

Past `max_queued` waiting jobs, or `group_max_queued` for one group, an enqueue is refused:

| Over HTTP | In Go |
|---|---|
| `429`, title `queue full` or `group full`, with `Retry-After` | `runnel.ErrQueueFull` or `runnel.ErrGroupFull` |

Once a worker claims a job, its waiting place is free and enqueues are taken again.

## When the disk is low

The hybrid store refuses new jobs while its disk has less free space than `hybrid.min_free_bytes`, 64 MiB by default:

| Over HTTP | In Go |
|---|---|
| `507`, title `storage full`, with `Retry-After` | `runnel.ErrNoSpace` |

Nothing else stops: workers keep claiming and finishing, and reads work. Once space is freed, enqueues are taken again without a restart. The [runbook](../RUNBOOK.md) says what to do.

## What producers should do

Treat `429` and `507` as "not now": wait for `Retry-After` and try again, or keep the work in your own database and offer it again on the next round. Never treat them as a failed job.

The story U23 and the drill D03 in [SPEC.md](../SPEC.md) check this.
