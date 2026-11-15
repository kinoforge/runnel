# Async work

Some work runs in an outside system: a video render, a large export, a model on a GPU service. You start it, get a task id, and check on it later. Runnel models this as a job that polls.

## In Go

```go
w.Handle("video.render", runnel.Async(
	func(ctx context.Context, j *runnel.Job) error {
		task, err := renderer.Start(ctx, j.Payload)
		if err != nil {
			return err
		}
		return runnel.Pending(task.ID, 10*time.Second)
	},
	func(ctx context.Context, j *runnel.Job) error {
		st, err := renderer.Status(ctx, j.Handle)
		switch {
		case err != nil:
			return err
		case st.Lost:
			return runnel.Fail(errors.New("renderer lost the task"))
		case st.Running:
			return runnel.Pending("", 10*time.Second)
		}
		return j.SetResult(st.URL)
	}))
```

1. The first function starts the task and returns `Pending(handle, delay)`. The job moves to `polling`, keeps the handle, and comes back after the delay.
2. The second function, the poller, runs each time it is due. `j.Handle` holds the task id.
3. The poller answers `Pending` again to keep waiting, a result to complete, or an error.

## Over HTTP

The worker finishes the attempt with a `pending` outcome:

```json
{"worker":"w1","version":4,"outcome":{"kind":"pending","handle":"task-81","delay":"10s"}}
```

The next claim returns the job with `"handle":"task-81"`. A job with a handle is a poll: check the task and finish again.

## What to know

- **Limits stay true.** A polling job keeps its slot in its queue, group and pool, so the limits match the work really running.
- **Attempts stay.** Polls do not use attempts; only the first start does.
- **Deadlines still apply.** A `deadline` or `ttl` ends a polling job too, as `expired`.

The rules are O07 and C09 in [SPEC.md](../SPEC.md); the story is U05.
