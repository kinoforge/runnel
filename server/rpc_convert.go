package server

import (
	"encoding/json"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func fromTS(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func dur(d runnel.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d.D())
}

func fromDur(d *durationpb.Duration) runnel.Duration {
	if d == nil {
		return 0
	}
	return runnel.Duration(d.AsDuration())
}

func raw(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

func pbState(s runnel.State) pb.State {
	v, ok := pb.State_value["STATE_"+strings.ToUpper(string(s))]
	if !ok {
		return pb.State_STATE_UNSPECIFIED
	}
	return pb.State(v)
}

func fromState(s pb.State) runnel.State {
	if s == pb.State_STATE_UNSPECIFIED {
		return ""
	}
	return runnel.State(strings.ToLower(strings.TrimPrefix(s.String(), "STATE_")))
}

func pbBackoff(b runnel.Backoff) *pb.Backoff {
	if b.IsZero() {
		return nil
	}
	return &pb.Backoff{Base: dur(b.Base), Max: dur(b.Max), Factor: b.Factor, Jitter: b.Jitter}
}

func fromBackoff(b *pb.Backoff) runnel.Backoff {
	if b == nil {
		return runnel.Backoff{}
	}
	return runnel.Backoff{Base: fromDur(b.GetBase()), Max: fromDur(b.GetMax()), Factor: b.GetFactor(), Jitter: b.GetJitter()}
}

func pbJob(j *runnel.Job) *pb.Job {
	out := &pb.Job{
		Id: j.ID, Namespace: j.Namespace, Queue: j.Queue, Kind: j.Kind, Payload: j.Payload, Priority: int32(j.Priority),
		Group: j.Group, Pool: j.Pool, State: pbState(j.State), Attempt: int32(j.Attempt), MaxAttempts: int32(j.MaxAttempts),
		RunAt: ts(j.RunAt), WaitingSince: ts(j.WaitingSince), StartTimeout: dur(j.StartTimeout), StartBy: ts(j.StartBy),
		Deadline: ts(j.Deadline), Timeout: dur(j.Timeout), Backoff: pbBackoff(j.Backoff), Worker: j.Worker,
		LeaseUntil: ts(j.LeaseUntil), Version: j.Version, Handle: j.Handle, UniqueKey: j.UniqueKey, UniqueFor: dur(j.UniqueFor),
		DependsOn: j.DependsOn, Pending: int32(j.Pending), CallbackUrl: j.CallbackURL, Meta: j.Meta, Result: j.Result,
		CreatedAt: ts(j.CreatedAt), UpdatedAt: ts(j.UpdatedAt), StartedAt: ts(j.StartedAt), FinishedAt: ts(j.FinishedAt),
	}
	for _, e := range j.Errors {
		out.Errors = append(out.Errors, &pb.AttemptError{Attempt: int32(e.Attempt), Queue: e.Queue, Kind: e.Kind, At: ts(e.At), Error: e.Error})
	}
	return out
}

func pbJobs(jobs []*runnel.Job) []*pb.Job {
	out := make([]*pb.Job, len(jobs))
	for i, j := range jobs {
		out[i] = pbJob(j)
	}
	return out
}

func jobRequest(n *pb.NewJob) JobRequest {
	return JobRequest{
		Queue: n.GetQueue(), Kind: n.GetKind(), Payload: raw(n.GetPayload()), Priority: int(n.GetPriority()), Group: n.GetGroup(),
		Pool: n.GetPool(), RunAt: fromTS(n.GetRunAt()), Delay: fromDur(n.GetDelay()), MaxAttempts: int(n.GetMaxAttempts()),
		UniqueKey: n.GetUniqueKey(), UniqueFor: fromDur(n.GetUniqueFor()), StartTimeout: fromDur(n.GetStartTimeout()),
		Deadline: fromTS(n.GetDeadline()), TTL: fromDur(n.GetTtl()), Timeout: fromDur(n.GetTimeout()), Backoff: fromBackoff(n.GetBackoff()),
		DependsOn: n.GetDependsOn(), CallbackURL: n.GetCallbackUrl(), Meta: n.GetMeta(),
	}
}

func pbInserted(in runnel.Inserted) *pb.Inserted {
	v := viewInserted(in)
	return &pb.Inserted{Id: v.ID, Duplicate: v.Duplicate, Error: v.Error}
}

func fromOutcome(o *pb.Outcome) runnel.Outcome {
	kind := ""
	if o.GetKind() != pb.OutcomeKind_OUTCOME_KIND_UNSPECIFIED {
		kind = strings.ToLower(strings.TrimPrefix(o.GetKind().String(), "OUTCOME_KIND_"))
	}
	return runnel.Outcome{Kind: runnel.OutcomeKind(kind), Error: o.GetError(), Result: raw(o.GetResult()), Delay: fromDur(o.GetDelay()), Handle: o.GetHandle()}
}

func pbQueue(q runnel.QueueConfig) *pb.Queue {
	return &pb.Queue{
		Name: q.Name, Paused: q.Paused, MaxRunning: int32(q.MaxRunning), GroupMaxRunning: int32(q.GroupMaxRunning),
		GroupMaxQueued: int32(q.GroupMaxQueued), MaxQueued: int32(q.MaxQueued), Rate: q.Rate, Burst: int32(q.Burst),
		GroupRate: q.GroupRate, GroupBurst: int32(q.GroupBurst), MaxAttempts: int32(q.MaxAttempts), Backoff: pbBackoff(q.Backoff),
		Timeout: dur(q.Timeout), StartTimeout: dur(q.StartTimeout), Ttl: dur(q.TTL), Wait: dur(q.Wait), Retention: dur(q.Retention),
		DeadRetention: dur(q.DeadRetention), CallbackUrl: q.CallbackURL,
	}
}

func fromQueue(q *pb.Queue) runnel.QueueConfig {
	return runnel.QueueConfig{
		Name: q.GetName(), Paused: q.GetPaused(), MaxRunning: int(q.GetMaxRunning()), GroupMaxRunning: int(q.GetGroupMaxRunning()),
		GroupMaxQueued: int(q.GetGroupMaxQueued()), MaxQueued: int(q.GetMaxQueued()), Rate: q.GetRate(), Burst: int(q.GetBurst()),
		GroupRate: q.GetGroupRate(), GroupBurst: int(q.GetGroupBurst()), MaxAttempts: int(q.GetMaxAttempts()), Backoff: fromBackoff(q.GetBackoff()),
		Timeout: fromDur(q.GetTimeout()), StartTimeout: fromDur(q.GetStartTimeout()), TTL: fromDur(q.GetTtl()), Wait: fromDur(q.GetWait()),
		Retention: fromDur(q.GetRetention()), DeadRetention: fromDur(q.GetDeadRetention()), CallbackURL: q.GetCallbackUrl(),
	}
}

func pbPool(p runnel.PoolConfig) *pb.Pool {
	return &pb.Pool{Name: p.Name, Slots: int32(p.Slots), GroupSlots: int32(p.GroupSlots), Rate: p.Rate, Burst: int32(p.Burst), Paused: p.Paused, PausedUntil: ts(p.PausedUntil)}
}

func fromPool(p *pb.Pool) runnel.PoolConfig {
	return runnel.PoolConfig{Name: p.GetName(), Slots: int(p.GetSlots()), GroupSlots: int(p.GetGroupSlots()), Rate: p.GetRate(), Burst: int(p.GetBurst()),
		Paused: p.GetPaused(), PausedUntil: fromTS(p.GetPausedUntil())}
}

func pbSchedule(s runnel.Schedule) *pb.Schedule {
	return &pb.Schedule{Name: s.Name, Spec: s.Spec, Timezone: s.Timezone, Paused: s.Paused, Queue: s.Queue, Kind: s.Kind, Payload: s.Payload,
		Priority: int32(s.Priority), Group: s.Group, Pool: s.Pool, MaxAttempts: int32(s.MaxAttempts), Timeout: dur(s.Timeout),
		CallbackUrl: s.CallbackURL, Meta: s.Meta, Overlap: s.Overlap}
}

func fromSchedule(s *pb.Schedule) runnel.Schedule {
	return runnel.Schedule{Name: s.GetName(), Spec: s.GetSpec(), Timezone: s.GetTimezone(), Paused: s.GetPaused(), Queue: s.GetQueue(), Kind: s.GetKind(),
		Payload: raw(s.GetPayload()), Priority: int(s.GetPriority()), Group: s.GetGroup(), Pool: s.GetPool(), MaxAttempts: int(s.GetMaxAttempts()),
		Timeout: fromDur(s.GetTimeout()), CallbackURL: s.GetCallbackUrl(), Meta: s.GetMeta(), Overlap: s.GetOverlap()}
}
