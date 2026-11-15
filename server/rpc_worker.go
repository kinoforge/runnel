package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

type workerRPC struct{ s *Server }

func (h *workerRPC) caller(ctx context.Context, ns, raw string) (string, error) {
	p, err := h.s.allow(ctx, ns, runnel.RoleWorker)
	if err != nil {
		return "", err
	}
	worker, err := workerID(p, raw)
	if err != nil {
		return "", h.s.rpcErr(err)
	}
	return worker, nil
}

func (h *workerRPC) Claim(ctx context.Context, req *connect.Request[pb.ClaimRequest]) (*connect.Response[pb.ClaimResponse], error) {
	m := req.Msg
	worker, err := h.caller(ctx, m.GetNamespace(), m.GetWorker())
	if err != nil {
		return nil, err
	}
	jobs, err := h.s.claimJobs(ctx, claimArgs{ns: m.GetNamespace(), worker: worker, queues: m.GetQueues(), limit: int(m.GetLimit()),
		lease: fromDur(m.GetLease()).D(), wait: fromDur(m.GetWait()).D()})
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.ClaimResponse{Jobs: pbJobs(jobs), Now: ts(h.s.clock.Now())}), nil
}

func (h *workerRPC) Extend(ctx context.Context, req *connect.Request[pb.ExtendRequest]) (*connect.Response[pb.ExtendResponse], error) {
	m := req.Msg
	worker, err := h.caller(ctx, m.GetNamespace(), m.GetWorker())
	if err != nil {
		return nil, err
	}
	lost, err := h.s.extendJobs(ctx, m.GetNamespace(), worker, m.GetIds(), fromDur(m.GetLease()).D())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.ExtendResponse{Lost: lost, Now: ts(h.s.clock.Now())}), nil
}

func (h *workerRPC) Finish(ctx context.Context, req *connect.Request[pb.FinishRequest]) (*connect.Response[pb.FinishResponse], error) {
	m := req.Msg
	worker, err := h.caller(ctx, m.GetNamespace(), m.GetWorker())
	if err != nil {
		return nil, err
	}
	if err := h.s.finishJob(ctx, finishArgs{ns: m.GetNamespace(), worker: worker, id: m.GetId(), version: m.GetVersion(),
		outcome: fromOutcome(m.GetOutcome()), payload: raw(m.GetPayload()), meta: m.GetMeta()}); err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.FinishResponse{}), nil
}

func (h *workerRPC) Dependencies(ctx context.Context, req *connect.Request[pb.DependenciesRequest]) (*connect.Response[pb.DependenciesResponse], error) {
	m := req.Msg
	if _, err := h.s.allow(ctx, m.GetNamespace(), runnel.RoleWorker); err != nil {
		return nil, err
	}
	if len(m.GetIds()) > h.s.cfg.Limits.MaxDependsOn {
		return nil, h.s.rpcErr(fmt.Errorf("%w: at most %d ids", runnel.ErrInvalid, h.s.cfg.Limits.MaxDependsOn))
	}
	jobs, err := h.s.store.Get(ctx, m.GetNamespace(), m.GetIds())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.DependenciesResponse{Jobs: pbJobs(jobs)}), nil
}
