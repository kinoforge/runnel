# Graceful deploys

Workers restart on every deploy. Runnel makes sure no job is lost and none runs twice because of it.

## Stop workers with their context

Cancel the context passed to `Run`, usually on `SIGTERM`:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()
w := runnel.NewWorker(store, "shop", runnel.WorkerConfig{Queues: []string{"emails"}, ShutdownTimeout: 30 * time.Second})
w.Run(ctx)
```

When the context ends:

1. The worker stops claiming new jobs.
2. It waits up to `ShutdownTimeout` for running jobs to finish.
3. It ends the rest: their handlers' contexts end with `runnel.ErrShutdown`, and the jobs go back to `waiting` untouched, with no attempt used and no error recorded.
4. `Run` returns once every job is written back.

The next worker picks those jobs up and runs them from the start. Give your platform's stop grace period a few seconds more than `ShutdownTimeout`, or the process is killed before it writes the jobs back.

## If a worker is killed

If a worker dies without shutting down, nobody renews its leases. Once a lease ends, the maintainer puts the job back with the attempt counted and an error recorded, and another worker runs it. Shorter leases bring jobs back sooner; heartbeats run every third of the lease.

## The server

`runnel serve` stops the same way on `SIGTERM`. With `server.drain_delay`, `/readyz` answers `503` for that long first, so a load balancer stops sending traffic before the server stops. See the [runbook](../RUNBOOK.md).

The stories U06 and U07 in [SPEC.md](../SPEC.md) check this on every store.
