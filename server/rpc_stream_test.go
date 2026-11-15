package server_test

import (
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pb "github.com/kinoforge/runnel/gen/runnel/v1"
	"github.com/kinoforge/runnel/internal/spec"
)

func TestRPCWorkStream(t *testing.T) {
	spec.Rule(t, "G03")
	e := rpcSetup(t)
	e.namespace(t, "media")
	prod := e.jobs(e.key(t, "media", "producer", "reader"))
	var ids []string
	for range 3 {
		res, err := prod.Enqueue(e.ctx(), connect.NewRequest(&pb.EnqueueRequest{Namespace: "media", Jobs: []*pb.NewJob{{Queue: "thumbs", Kind: "resize"}}}))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.Msg.GetResults()[0].GetId())
	}
	st := e.workers(e.key(t, "media", "worker")).Work(e.ctx())
	send := func(m *pb.WorkRequest) {
		t.Helper()
		if err := st.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	recv := func() *pb.WorkResponse {
		t.Helper()
		r, err := st.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if r.GetNow() == nil {
			t.Fatal("a work answer without the server's time")
		}
		return r
	}
	send(&pb.WorkRequest{Msg: &pb.WorkRequest_Start{Start: &pb.WorkStart{Namespace: "media", Worker: "gpu-1", Queues: []string{"thumbs"}, Lease: durationpb.New(20 * time.Second)}}})
	send(&pb.WorkRequest{Msg: &pb.WorkRequest_Credit{Credit: 2}})
	held := map[string]*pb.Job{}
	for range 2 {
		j := recv().GetJob()
		if j == nil || j.GetState() != pb.State_STATE_RUNNING {
			t.Fatalf("expected a running job, got %+v", j)
		}
		if left := time.Until(j.GetLeaseUntil().AsTime()); left < 15*time.Second || left > 21*time.Second {
			t.Fatalf("lease runs %v, want the 20s asked for", left)
		}
		held[j.GetId()] = j
	}
	got, err := prod.ListJobs(e.ctx(), connect.NewRequest(&pb.ListJobsRequest{Namespace: "media", State: pb.State_STATE_WAITING}))
	if err != nil || len(got.Msg.GetJobs()) != 1 {
		t.Fatalf("a credit of 2 claimed more than 2 jobs: %v %v", got, err)
	}
	var heldIDs []string
	for id := range held {
		heldIDs = append(heldIDs, id)
	}
	send(&pb.WorkRequest{Msg: &pb.WorkRequest_Extend{Extend: &pb.ExtendRequest{Ids: heldIDs}}})
	if ext := recv().GetExtended(); ext == nil || len(ext.GetLost()) != 0 {
		t.Fatalf("extend answered %+v", ext)
	}
	finish := func(j *pb.Job) {
		t.Helper()
		send(&pb.WorkRequest{Msg: &pb.WorkRequest_Finish{Finish: &pb.FinishRequest{Id: j.GetId(), Version: j.GetVersion(),
			Outcome: &pb.Outcome{Kind: pb.OutcomeKind_OUTCOME_KIND_DONE}}}})
		if f := recv().GetFinished(); f == nil || f.GetId() != j.GetId() || f.GetError() != "" {
			t.Fatalf("finish answered %+v", f)
		}
	}
	for _, j := range held {
		finish(j)
	}
	send(&pb.WorkRequest{Msg: &pb.WorkRequest_Credit{Credit: 1}})
	last := recv().GetJob()
	if last == nil || held[last.GetId()] != nil {
		t.Fatalf("third job %+v", last)
	}
	send(&pb.WorkRequest{Msg: &pb.WorkRequest_Finish{Finish: &pb.FinishRequest{Id: last.GetId(), Version: last.GetVersion() + 9,
		Outcome: &pb.Outcome{Kind: pb.OutcomeKind_OUTCOME_KIND_DONE}}}})
	if f := recv().GetFinished(); f == nil || f.GetError() == "" {
		t.Fatalf("a finish with a stale version was accepted: %+v", f)
	}
	finish(last)
	if err := st.CloseRequest(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream end: %v", err)
	}
	_ = st.CloseResponse()
	for _, id := range ids {
		j, err := prod.GetJob(e.ctx(), connect.NewRequest(&pb.GetJobRequest{Namespace: "media", Id: id}))
		if err != nil || j.Msg.GetJob().GetState() != pb.State_STATE_COMPLETED {
			t.Fatalf("job %s after the stream: %v %v", id, j, err)
		}
	}
}

func TestRPCSettings(t *testing.T) {
	spec.Rule(t, "G04")
	e := rpcSetup(t)
	e.namespace(t, "ops")
	c := e.settings(e.key(t, "ops", "admin", "reader"))
	put, err := c.PutPool(e.ctx(), connect.NewRequest(&pb.PutPoolRequest{Namespace: "ops", Pool: &pb.Pool{Name: "gpu", Slots: 2, Rate: 5}}))
	if err != nil || put.Msg.GetEtag() == "" {
		t.Fatalf("put %v %v", put, err)
	}
	patched, err := c.PatchPool(e.ctx(), connect.NewRequest(&pb.PatchPoolRequest{Namespace: "ops", Pool: &pb.Pool{Name: "gpu", Paused: true},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"paused"}}, Precondition: &pb.Precondition{IfMatch: put.Msg.GetEtag()}}))
	if err != nil || !patched.Msg.GetPool().GetPaused() || patched.Msg.GetPool().GetSlots() != 2 || patched.Msg.GetPool().GetRate() != 5 {
		t.Fatalf("patch changed more than paused: %v %v", patched, err)
	}
	req, _ := http.NewRequest("GET", e.srv.URL+"/v1/ns/ops/pools/gpu", nil)
	resp, err := e.http(admin).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if tag := resp.Header.Get("ETag"); tag != patched.Msg.GetEtag() {
		t.Fatalf("HTTP ETag %s, RPC etag %s", tag, patched.Msg.GetEtag())
	}
	stale := func() error {
		_, err := c.PatchPool(e.ctx(), connect.NewRequest(&pb.PatchPoolRequest{Namespace: "ops", Pool: &pb.Pool{Name: "gpu", Slots: 9},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"slots"}}, Precondition: &pb.Precondition{IfMatch: put.Msg.GetEtag()}}))
		return err
	}
	rename := func() error {
		_, err := c.PatchPool(e.ctx(), connect.NewRequest(&pb.PatchPoolRequest{Namespace: "ops", Pool: &pb.Pool{Name: "gpu"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}}}))
		return err
	}
	exists := func() error {
		_, err := c.PutPool(e.ctx(), connect.NewRequest(&pb.PutPoolRequest{Namespace: "ops", Pool: &pb.Pool{Name: "gpu", Slots: 1}, Precondition: &pb.Precondition{IfNoneMatch: true}}))
		return err
	}
	del := func() error {
		_, err := c.DeletePool(e.ctx(), connect.NewRequest(&pb.DeleteSettingRequest{Namespace: "ops", Name: "gpu"}))
		return err
	}
	for what, c := range map[string]struct {
		err  error
		code connect.Code
	}{
		"stale etag":      {stale(), connect.CodeAborted},
		"name in mask":    {rename(), connect.CodeInvalidArgument},
		"create existing": {exists(), connect.CodeAborted},
	} {
		if got, _ := code(c.err); got != c.code {
			t.Errorf("%s: %v, want %v", what, got, c.code)
		}
	}
	list, err := c.ListPools(e.ctx(), connect.NewRequest(&pb.ListSettingsRequest{Namespace: "ops"}))
	if err != nil || len(list.Msg.GetPools()) != 1 {
		t.Fatalf("list %v %v", list, err)
	}
	if err := del(); err != nil {
		t.Fatal(err)
	}
	if got, _ := code(del()); got != connect.CodeNotFound {
		t.Fatalf("deleting a missing pool: %v", got)
	}
}
