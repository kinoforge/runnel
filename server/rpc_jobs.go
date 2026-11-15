package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

type jobRPC struct{ s *Server }

func (h *jobRPC) Enqueue(ctx context.Context, req *connect.Request[pb.EnqueueRequest]) (*connect.Response[pb.EnqueueResponse], error) {
	m := req.Msg
	if _, err := h.s.allow(ctx, m.GetNamespace(), runnel.RoleProducer); err != nil {
		return nil, err
	}
	if len(m.GetJobs()) == 0 || len(m.GetJobs()) > h.s.cfg.MaxBatch {
		return nil, h.s.rpcErr(fmt.Errorf("%w: send 1 to %d jobs", runnel.ErrInvalid, h.s.cfg.MaxBatch))
	}
	jobs := make([]*runnel.Job, len(m.GetJobs()))
	for i, n := range m.GetJobs() {
		j, err := h.s.toJob(jobRequest(n))
		if err != nil {
			return nil, h.s.rpcErr(fmt.Errorf("job %d: %w", i, err))
		}
		jobs[i] = j
	}
	res, err := h.s.client(m.GetNamespace()).EnqueueJobs(ctx, jobs)
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	if len(res) == 1 && res[0].Err != nil {
		return nil, h.s.rpcErr(res[0].Err)
	}
	out := &pb.EnqueueResponse{}
	for _, in := range res {
		out.Results = append(out.Results, pbInserted(in))
	}
	return connect.NewResponse(out), nil
}

func (h *jobRPC) EnqueueFlow(ctx context.Context, req *connect.Request[pb.EnqueueFlowRequest]) (*connect.Response[pb.EnqueueFlowResponse], error) {
	m := req.Msg
	if _, err := h.s.allow(ctx, m.GetNamespace(), runnel.RoleProducer); err != nil {
		return nil, err
	}
	if len(m.GetSteps()) == 0 || len(m.GetSteps()) > h.s.cfg.MaxBatch {
		return nil, h.s.rpcErr(fmt.Errorf("%w: send 1 to %d steps", runnel.ErrInvalid, h.s.cfg.MaxBatch))
	}
	f := runnel.NewFlow()
	for _, st := range m.GetSteps() {
		j, err := h.s.toJob(jobRequest(st.GetJob()))
		if err != nil {
			return nil, h.s.rpcErr(fmt.Errorf("step %q: %w", st.GetName(), err))
		}
		f.Add(st.GetName(), j, st.GetAfter()...)
	}
	res, err := h.s.client(m.GetNamespace()).EnqueueFlow(ctx, f)
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	out := &pb.EnqueueFlowResponse{Steps: map[string]*pb.Inserted{}}
	for name, in := range res {
		out.Steps[name] = pbInserted(in)
	}
	return connect.NewResponse(out), nil
}

func (h *jobRPC) GetJob(ctx context.Context, req *connect.Request[pb.GetJobRequest]) (*connect.Response[pb.GetJobResponse], error) {
	if _, err := h.s.allow(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	j, err := h.s.client(req.Msg.GetNamespace()).Get(ctx, req.Msg.GetId())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.GetJobResponse{Job: pbJob(j)}), nil
}

func (h *jobRPC) WaitJob(ctx context.Context, req *connect.Request[pb.WaitJobRequest]) (*connect.Response[pb.WaitJobResponse], error) {
	m := req.Msg
	if _, err := h.s.allow(ctx, m.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	c := h.s.client(m.GetNamespace())
	j, err := c.Get(ctx, m.GetId())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	if wait := min(max(fromDur(m.GetWait()).D(), 0), h.s.cfg.MaxResultWait); wait > 0 && !j.State.Final() {
		wctx, cancel := context.WithTimeout(ctx, wait)
		latest, err := c.Wait(wctx, j.ID)
		cancel()
		if ctx.Err() != nil {
			return nil, h.s.rpcErr(ctx.Err())
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, h.s.rpcErr(err)
		}
		if latest != nil {
			j = latest
		}
	}
	return connect.NewResponse(&pb.WaitJobResponse{Job: pbJob(j), Finished: j.State.Final()}), nil
}

func (h *jobRPC) ListJobs(ctx context.Context, req *connect.Request[pb.ListJobsRequest]) (*connect.Response[pb.ListJobsResponse], error) {
	m := req.Msg
	if _, err := h.s.allow(ctx, m.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	jobs, next, err := h.s.client(m.GetNamespace()).List(ctx, runnel.Query{Queue: m.GetQueue(), Kind: m.GetKind(), Group: m.GetGroup(),
		State: fromState(m.GetState()), Cursor: m.GetCursor(), Limit: int(m.GetLimit())})
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.ListJobsResponse{Jobs: pbJobs(jobs), NextCursor: next}), nil
}

func (h *jobRPC) CancelJob(ctx context.Context, req *connect.Request[pb.CancelJobRequest]) (*connect.Response[pb.CancelJobResponse], error) {
	if _, err := h.s.allow(ctx, req.Msg.GetNamespace(), runnel.RoleProducer); err != nil {
		return nil, err
	}
	j, err := h.s.client(req.Msg.GetNamespace()).Cancel(ctx, req.Msg.GetId())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.CancelJobResponse{Job: pbJob(j)}), nil
}

func (h *jobRPC) RetryJob(ctx context.Context, req *connect.Request[pb.RetryJobRequest]) (*connect.Response[pb.RetryJobResponse], error) {
	if _, err := h.s.allow(ctx, req.Msg.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	j, err := h.s.client(req.Msg.GetNamespace()).Retry(ctx, req.Msg.GetId())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.RetryJobResponse{Job: pbJob(j)}), nil
}

func (h *jobRPC) GetStats(ctx context.Context, req *connect.Request[pb.GetStatsRequest]) (*connect.Response[pb.GetStatsResponse], error) {
	if _, err := h.s.allow(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	st, err := h.s.client(req.Msg.GetNamespace()).Stats(ctx)
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	out := &pb.GetStatsResponse{}
	for _, q := range st {
		counts := map[string]int64{}
		for state, n := range q.Counts {
			counts[string(state)] = n
		}
		out.Queues = append(out.Queues, &pb.QueueStats{Queue: q.Queue, Counts: counts})
	}
	return connect.NewResponse(out), nil
}

func (h *jobRPC) ListEvents(ctx context.Context, req *connect.Request[pb.ListEventsRequest]) (*connect.Response[pb.ListEventsResponse], error) {
	m := req.Msg
	if _, err := h.s.allow(ctx, m.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	evs, err := h.s.client(m.GetNamespace()).Events(ctx, m.GetAfter(), int(m.GetLimit()))
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	out := &pb.ListEventsResponse{}
	for _, e := range evs {
		out.Events = append(out.Events, &pb.Event{Seq: e.Seq, JobId: e.JobID, Queue: e.Queue, Kind: e.Kind, State: pbState(e.State), At: ts(e.At)})
	}
	return connect.NewResponse(out), nil
}
