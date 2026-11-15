package server

import (
	"context"

	"connectrpc.com/connect"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

type configRPC struct{ s *Server }

func (h *configRPC) queues() setting[runnel.QueueConfig, *pb.Queue] {
	return setting[runnel.QueueConfig, *pb.Queue]{s: h.s, kind: runnel.ConfigQueue, toPB: pbQueue, from: fromQueue, check: h.s.checkQueue}
}

func (h *configRPC) pools() setting[runnel.PoolConfig, *pb.Pool] {
	return setting[runnel.PoolConfig, *pb.Pool]{s: h.s, kind: runnel.ConfigPool, toPB: pbPool, from: fromPool, check: h.s.checkPool}
}

func (h *configRPC) schedules() setting[runnel.Schedule, *pb.Schedule] {
	return setting[runnel.Schedule, *pb.Schedule]{s: h.s, kind: runnel.ConfigSchedule, toPB: pbSchedule, from: fromSchedule, check: h.s.checkSchedule}
}

func (h *configRPC) gate(ctx context.Context, ns, role string) error {
	_, err := h.s.allow(ctx, ns, role)
	return err
}

func answer[T any](v *T, err error, s *Server) (*connect.Response[T], error) {
	if err != nil {
		return nil, s.rpcErr(err)
	}
	return connect.NewResponse(v), nil
}

func (h *configRPC) ListQueues(ctx context.Context, req *connect.Request[pb.ListSettingsRequest]) (*connect.Response[pb.ListQueuesResponse], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	all, err := h.queues().list(ctx, req.Msg.GetNamespace())
	return answer(&pb.ListQueuesResponse{Queues: all}, err, h.s)
}

func (h *configRPC) GetQueue(ctx context.Context, req *connect.Request[pb.GetSettingRequest]) (*connect.Response[pb.QueueSetting], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	v, tag, err := h.queues().get(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	return answer(&pb.QueueSetting{Queue: v, Etag: tag}, err, h.s)
}

func (h *configRPC) PutQueue(ctx context.Context, req *connect.Request[pb.PutQueueRequest]) (*connect.Response[pb.QueueSetting], error) {
	m := req.Msg
	if err := h.gate(ctx, m.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	v, tag, err := h.queues().put(ctx, m.GetNamespace(), m.GetQueue(), m.GetPrecondition())
	return answer(&pb.QueueSetting{Queue: v, Etag: tag}, err, h.s)
}

func (h *configRPC) PatchQueue(ctx context.Context, req *connect.Request[pb.PatchQueueRequest]) (*connect.Response[pb.QueueSetting], error) {
	m := req.Msg
	if err := h.gate(ctx, m.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	v, tag, err := h.queues().patch(ctx, m.GetNamespace(), m.GetQueue().GetName(), m.GetQueue(), m.GetUpdateMask(), m.GetPrecondition())
	return answer(&pb.QueueSetting{Queue: v, Etag: tag}, err, h.s)
}

func (h *configRPC) DeleteQueue(ctx context.Context, req *connect.Request[pb.DeleteSettingRequest]) (*connect.Response[pb.DeleteSettingResponse], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	return answer(&pb.DeleteSettingResponse{}, h.queues().remove(ctx, req.Msg.GetNamespace(), req.Msg.GetName()), h.s)
}

func (h *configRPC) ListPools(ctx context.Context, req *connect.Request[pb.ListSettingsRequest]) (*connect.Response[pb.ListPoolsResponse], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	all, err := h.pools().list(ctx, req.Msg.GetNamespace())
	return answer(&pb.ListPoolsResponse{Pools: all}, err, h.s)
}

func (h *configRPC) GetPool(ctx context.Context, req *connect.Request[pb.GetSettingRequest]) (*connect.Response[pb.PoolSetting], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	v, tag, err := h.pools().get(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	return answer(&pb.PoolSetting{Pool: v, Etag: tag}, err, h.s)
}

func (h *configRPC) PutPool(ctx context.Context, req *connect.Request[pb.PutPoolRequest]) (*connect.Response[pb.PoolSetting], error) {
	m := req.Msg
	if err := h.gate(ctx, m.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	v, tag, err := h.pools().put(ctx, m.GetNamespace(), m.GetPool(), m.GetPrecondition())
	return answer(&pb.PoolSetting{Pool: v, Etag: tag}, err, h.s)
}

func (h *configRPC) PatchPool(ctx context.Context, req *connect.Request[pb.PatchPoolRequest]) (*connect.Response[pb.PoolSetting], error) {
	m := req.Msg
	if err := h.gate(ctx, m.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	v, tag, err := h.pools().patch(ctx, m.GetNamespace(), m.GetPool().GetName(), m.GetPool(), m.GetUpdateMask(), m.GetPrecondition())
	return answer(&pb.PoolSetting{Pool: v, Etag: tag}, err, h.s)
}

func (h *configRPC) DeletePool(ctx context.Context, req *connect.Request[pb.DeleteSettingRequest]) (*connect.Response[pb.DeleteSettingResponse], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	return answer(&pb.DeleteSettingResponse{}, h.pools().remove(ctx, req.Msg.GetNamespace(), req.Msg.GetName()), h.s)
}

func (h *configRPC) ListSchedules(ctx context.Context, req *connect.Request[pb.ListSettingsRequest]) (*connect.Response[pb.ListSchedulesResponse], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	all, err := h.schedules().list(ctx, req.Msg.GetNamespace())
	return answer(&pb.ListSchedulesResponse{Schedules: all}, err, h.s)
}

func (h *configRPC) GetSchedule(ctx context.Context, req *connect.Request[pb.GetSettingRequest]) (*connect.Response[pb.ScheduleSetting], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleReader); err != nil {
		return nil, err
	}
	v, tag, err := h.schedules().get(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	return answer(&pb.ScheduleSetting{Schedule: v, Etag: tag}, err, h.s)
}

func (h *configRPC) PutSchedule(ctx context.Context, req *connect.Request[pb.PutScheduleRequest]) (*connect.Response[pb.ScheduleSetting], error) {
	m := req.Msg
	if err := h.gate(ctx, m.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	v, tag, err := h.schedules().put(ctx, m.GetNamespace(), m.GetSchedule(), m.GetPrecondition())
	return answer(&pb.ScheduleSetting{Schedule: v, Etag: tag}, err, h.s)
}

func (h *configRPC) PatchSchedule(ctx context.Context, req *connect.Request[pb.PatchScheduleRequest]) (*connect.Response[pb.ScheduleSetting], error) {
	m := req.Msg
	if err := h.gate(ctx, m.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	v, tag, err := h.schedules().patch(ctx, m.GetNamespace(), m.GetSchedule().GetName(), m.GetSchedule(), m.GetUpdateMask(), m.GetPrecondition())
	return answer(&pb.ScheduleSetting{Schedule: v, Etag: tag}, err, h.s)
}

func (h *configRPC) DeleteSchedule(ctx context.Context, req *connect.Request[pb.DeleteSettingRequest]) (*connect.Response[pb.DeleteSettingResponse], error) {
	if err := h.gate(ctx, req.Msg.GetNamespace(), runnel.RoleAdmin); err != nil {
		return nil, err
	}
	return answer(&pb.DeleteSettingResponse{}, h.schedules().remove(ctx, req.Msg.GetNamespace(), req.Msg.GetName()), h.s)
}
