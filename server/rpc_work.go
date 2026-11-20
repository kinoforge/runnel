package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

const maxWorkCredit = 1 << 16

type claimed struct {
	jobs []*runnel.Job
	err  error
}

type workSession struct {
	h      *workerRPC
	st     *connect.BidiStream[pb.WorkRequest, pb.WorkResponse]
	ns     string
	worker string
	queues []string
	lease  time.Duration
	credit int
}

func (h *workerRPC) Work(ctx context.Context, st *connect.BidiStream[pb.WorkRequest, pb.WorkResponse]) error {
	if err := h.s.rpcAuth(ctx); err != nil {
		return err
	}
	first, err := st.Receive()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return h.s.rpcErr(fmt.Errorf("%w: a work stream starts with start", runnel.ErrInvalid))
	}
	worker, err := h.caller(ctx, start.GetNamespace(), start.GetWorker())
	if err != nil {
		return err
	}
	if err := h.s.checkClaimQueues(start.GetQueues()); err != nil {
		return h.s.rpcErr(err)
	}
	w := &workSession{h: h, st: st, ns: start.GetNamespace(), worker: worker, queues: start.GetQueues(), lease: h.s.clampLease(fromDur(start.GetLease()).D())}
	return w.run(ctx)
}

func (w *workSession) run(ctx context.Context) error {
	in := make(chan *pb.WorkRequest)
	gone := make(chan error, 1)
	cctx, cancel := context.WithCancel(ctx)
	go w.pump(cctx, in, gone)
	results := make(chan claimed, 1)
	claiming := false
	defer func() {
		cancel()
		if claiming {
			w.release((<-results).jobs)
		}
	}()
	for {
		if w.credit > 0 && !claiming {
			claiming = true
			go w.claim(cctx, w.credit, results)
		}
		select {
		case err := <-gone:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case m := <-in:
			if err := w.handle(ctx, m); err != nil {
				return err
			}
		case r := <-results:
			claiming = false
			if r.err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return w.h.s.rpcErr(r.err)
			}
			if err := w.deliver(r.jobs); err != nil {
				return err
			}
		}
	}
}

func (w *workSession) pump(ctx context.Context, in chan<- *pb.WorkRequest, gone chan<- error) {
	for {
		m, err := w.st.Receive()
		if err != nil {
			gone <- err
			return
		}
		select {
		case in <- m:
		case <-ctx.Done():
			return
		}
	}
}

func (w *workSession) claim(ctx context.Context, n int, out chan<- claimed) {
	jobs, err := w.h.s.claimJobs(ctx, claimArgs{ns: w.ns, worker: w.worker, queues: w.queues, limit: n, lease: w.lease, wait: w.h.s.cfg.MaxClaimWait})
	out <- claimed{jobs, err}
}

func (w *workSession) handle(ctx context.Context, m *pb.WorkRequest) error {
	s := w.h.s
	switch v := m.GetMsg().(type) {
	case *pb.WorkRequest_Credit:
		if v.Credit < 0 {
			return s.rpcErr(fmt.Errorf("%w: credit must not be negative", runnel.ErrInvalid))
		}
		w.credit = min(w.credit+int(v.Credit), maxWorkCredit)
	case *pb.WorkRequest_Extend:
		lost, err := s.extendJobs(ctx, w.ns, w.worker, v.Extend.GetIds(), w.leaseFor(v.Extend.GetLease()))
		if err != nil {
			return s.rpcErr(err)
		}
		return w.send(&pb.WorkResponse{Msg: &pb.WorkResponse_Extended{Extended: &pb.ExtendResponse{Lost: lost, Now: ts(s.clock.Now())}}})
	case *pb.WorkRequest_Finish:
		f := v.Finish
		done := &pb.WorkFinished{Id: f.GetId()}
		if err := s.finishJob(ctx, finishArgs{ns: w.ns, worker: w.worker, id: f.GetId(), version: f.GetVersion(), outcome: fromOutcome(f.GetOutcome()),
			payload: raw(f.GetPayload()), meta: f.GetMeta()}); err != nil {
			done.Error = s.rpcErrText(err)
		}
		return w.send(&pb.WorkResponse{Msg: &pb.WorkResponse_Finished{Finished: done}})
	default:
		return s.rpcErr(fmt.Errorf("%w: start is sent once, first", runnel.ErrInvalid))
	}
	return nil
}

func (w *workSession) leaseFor(d *durationpb.Duration) time.Duration {
	if d == nil {
		return w.lease
	}
	return d.AsDuration()
}

func (w *workSession) deliver(jobs []*runnel.Job) error {
	for i, j := range jobs {
		if err := w.send(&pb.WorkResponse{Msg: &pb.WorkResponse_Job{Job: pbJob(j)}}); err != nil {
			w.release(jobs[i:])
			return err
		}
		w.credit--
	}
	return nil
}

func (w *workSession) send(r *pb.WorkResponse) error {
	r.Now = ts(w.h.s.clock.Now())
	return w.st.Send(r)
}

func (w *workSession) release(jobs []*runnel.Job) {
	ctx := context.Background()
	for _, j := range jobs {
		_ = w.h.s.source(w.ns).Finish(ctx, w.worker, j, runnel.Outcome{Kind: runnel.OutcomeRelease})
	}
}
