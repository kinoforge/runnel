// Package runnel is a job queue for Go services, backed by the database or
// Redis you already run.
//
// Producers enqueue jobs through a Client. Workers claim them under a lease,
// run the handler registered for the job's kind, heartbeat while it runs and
// report an outcome. A Maintainer returns the jobs of workers that died, fires
// schedules, delivers callbacks and prunes old jobs.
//
// A queue's settings limit how many of its jobs run at once (MaxRunning), how
// many per group (GroupMaxRunning) and how fast they start (Rate). A Pool is
// a limit several queues share, such as one partner account. When a handler
// cannot finish a job, its error says what happens next: retry later, Fail,
// Fallback to the next target, Busy (the pool is full: try later without
// using an attempt) or Pending (an outside task is running: poll it later).
//
// A caller chooses how to wait for the result: come back later (Get), wait
// (Client.Wait, EnqueueAndWait), or be called (a callback).
//
// docs/SPEC.md lists every rule Runnel follows and docs/DESIGN.md explains why.
//
//	c := runnel.NewClient(store, "billing")
//	c.Enqueue(ctx, "invoices", "invoice.render", Invoice{ID: 42}, runnel.Group("acme"))
//
//	w := runnel.NewWorker(store, "billing", runnel.WorkerConfig{Queues: []string{"invoices"}})
//	w.HandleFunc("invoice.render", func(ctx context.Context, j *runnel.Job) error {
//		var in Invoice
//		if err := j.Bind(&in); err != nil {
//			return runnel.Fail(err)
//		}
//		return render(ctx, in)
//	})
//	w.Run(ctx)
//
// Each namespace is an isolated queue system with its own keys, so every
// service can have its own. The server package serves the same model over
// HTTP for workers and producers in any language.
package runnel
