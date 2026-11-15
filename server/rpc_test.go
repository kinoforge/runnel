package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
	"github.com/kinoforge/runnel/gen/runnel/v1/runnelv1connect"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/sqlite"
)

type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if b.token != "" {
		r.Header.Set("Authorization", "Bearer "+b.token)
	}
	return b.next.RoundTrip(r)
}

type rpcEnv struct {
	srv *httptest.Server
	t   *testing.T
}

func (e *rpcEnv) ctx() context.Context {
	return e.t.Context()
}

func rpcSetup(t *testing.T) *rpcEnv {
	t.Helper()
	s, err := sqlite.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(server.New(s, server.Config{AdminToken: admin}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &rpcEnv{srv: srv, t: t}
}

func (e *rpcEnv) http(token string) *http.Client {
	c := *e.srv.Client()
	c.Transport = bearer{token: token, next: e.srv.Client().Transport}
	return &c
}

func (e *rpcEnv) jobs(token string, opts ...connect.ClientOption) runnelv1connect.JobServiceClient {
	return runnelv1connect.NewJobServiceClient(e.http(token), e.srv.URL, opts...)
}

func (e *rpcEnv) workers(token string) runnelv1connect.WorkerServiceClient {
	return runnelv1connect.NewWorkerServiceClient(e.http(token), e.srv.URL, connect.WithGRPC())
}

func (e *rpcEnv) settings(token string) runnelv1connect.ConfigServiceClient {
	return runnelv1connect.NewConfigServiceClient(e.http(token), e.srv.URL)
}

func (e *rpcEnv) admin() runnelv1connect.AdminServiceClient {
	return runnelv1connect.NewAdminServiceClient(e.http(admin), e.srv.URL, connect.WithGRPC())
}

func (e *rpcEnv) namespace(t *testing.T, ns string) {
	t.Helper()
	if _, err := e.admin().PutNamespace(e.ctx(), connect.NewRequest(&pb.PutNamespaceRequest{Name: ns})); err != nil {
		t.Fatal(err)
	}
}

func (e *rpcEnv) key(t *testing.T, ns string, roles ...string) string {
	t.Helper()
	res, err := e.admin().CreateKey(e.ctx(), connect.NewRequest(&pb.CreateKeyRequest{Namespace: ns, Name: "k", Roles: roles}))
	if err != nil {
		t.Fatal(err)
	}
	return res.Msg.GetToken()
}

func code(err error) (connect.Code, string) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return 0, ""
	}
	return ce.Code(), ce.Meta().Get("Runnel-Error")
}

func TestRPCJobsAndWorkers(t *testing.T) {
	spec.Rule(t, "G01", "G02")
	e := rpcSetup(t)
	e.namespace(t, "shop")
	prod := e.jobs(e.key(t, "shop", "producer", "reader"), connect.WithProtoJSON())
	work := e.workers(e.key(t, "shop", "worker"))

	res, err := prod.Enqueue(e.ctx(), connect.NewRequest(&pb.EnqueueRequest{Namespace: "shop", Jobs: []*pb.NewJob{
		{Queue: "emails", Kind: "welcome", Payload: []byte(`{"to":"ana@example.com","n":12345678901234567}`)},
	}}))
	if err != nil || len(res.Msg.GetResults()) != 1 || res.Msg.GetResults()[0].GetId() == "" {
		t.Fatalf("enqueue %v %v", res, err)
	}
	id := res.Msg.GetResults()[0].GetId()
	claim, err := work.Claim(e.ctx(), connect.NewRequest(&pb.ClaimRequest{Namespace: "shop", Worker: "w1", Queues: []string{"emails"}, Limit: 5, Wait: durationpb.New(time.Second)}))
	if err != nil || len(claim.Msg.GetJobs()) != 1 || claim.Msg.GetNow() == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	j := claim.Msg.GetJobs()[0]
	if j.GetId() != id || j.GetState() != pb.State_STATE_RUNNING || string(j.GetPayload()) != `{"to":"ana@example.com","n":12345678901234567}` {
		t.Fatalf("claimed %+v", j)
	}
	if _, err := work.Extend(e.ctx(), connect.NewRequest(&pb.ExtendRequest{Namespace: "shop", Worker: "w1", Ids: []string{id}})); err != nil {
		t.Fatal(err)
	}
	if _, err := work.Finish(e.ctx(), connect.NewRequest(&pb.FinishRequest{Namespace: "shop", Worker: "w1", Id: id, Version: j.GetVersion(),
		Outcome: &pb.Outcome{Kind: pb.OutcomeKind_OUTCOME_KIND_DONE, Result: []byte(`{"sent":true}`)}})); err != nil {
		t.Fatal(err)
	}
	got, err := prod.WaitJob(e.ctx(), connect.NewRequest(&pb.WaitJobRequest{Namespace: "shop", Id: id, Wait: durationpb.New(time.Second)}))
	if err != nil || !got.Msg.GetFinished() || got.Msg.GetJob().GetState() != pb.State_STATE_COMPLETED || string(got.Msg.GetJob().GetResult()) != `{"sent":true}` {
		t.Fatalf("finished job %v %v", got, err)
	}
	list, err := prod.ListJobs(e.ctx(), connect.NewRequest(&pb.ListJobsRequest{Namespace: "shop", State: pb.State_STATE_COMPLETED}))
	if err != nil || len(list.Msg.GetJobs()) != 1 {
		t.Fatalf("list %v %v", list, err)
	}
	stats, err := prod.GetStats(e.ctx(), connect.NewRequest(&pb.GetStatsRequest{Namespace: "shop"}))
	if err != nil || len(stats.Msg.GetQueues()) != 1 || stats.Msg.GetQueues()[0].GetCounts()["completed"] != 1 {
		t.Fatalf("stats %v %v", stats, err)
	}

	if _, err := e.settings(e.key(t, "shop", "admin")).PutQueue(e.ctx(), connect.NewRequest(&pb.PutQueueRequest{Namespace: "shop", Queue: &pb.Queue{Name: "full", MaxQueued: 1}})); err != nil {
		t.Fatal(err)
	}
	full := func() error {
		_, err := prod.Enqueue(e.ctx(), connect.NewRequest(&pb.EnqueueRequest{Namespace: "shop", Jobs: []*pb.NewJob{{Queue: "full", Kind: "k"}}}))
		return err
	}
	_ = full()
	for what, c := range map[string]struct {
		err  error
		code connect.Code
		name string
	}{
		"queue full":            {full(), connect.CodeResourceExhausted, "queue_full"},
		"cancel a finished job": {cancelErr(e, prod, id), connect.CodeFailedPrecondition, "final"},
		"unknown job":           {getErr(e, prod, runnel.NewID()), connect.CodeNotFound, "not_found"},
		"bad kind":              {badKind(e, prod), connect.CodeInvalidArgument, "invalid"},
		"no role":               {roleErr(e, t), connect.CodePermissionDenied, ""},
		"no credentials":        {getErr(e, e.jobs(""), id), connect.CodeUnauthenticated, ""},
	} {
		if got, name := code(c.err); got != c.code || name != c.name {
			t.Errorf("%s: %v (%s), want %v (%s)", what, got, name, c.code, c.name)
		}
	}
}

func cancelErr(e *rpcEnv, c runnelv1connect.JobServiceClient, id string) error {
	_, err := c.CancelJob(e.ctx(), connect.NewRequest(&pb.CancelJobRequest{Namespace: "shop", Id: id}))
	return err
}

func getErr(e *rpcEnv, c runnelv1connect.JobServiceClient, id string) error {
	_, err := c.GetJob(e.ctx(), connect.NewRequest(&pb.GetJobRequest{Namespace: "shop", Id: id}))
	return err
}

func badKind(e *rpcEnv, c runnelv1connect.JobServiceClient) error {
	_, err := c.Enqueue(e.ctx(), connect.NewRequest(&pb.EnqueueRequest{Namespace: "shop", Jobs: []*pb.NewJob{{Queue: "q", Kind: "a b"}}}))
	return err
}

func roleErr(e *rpcEnv, t *testing.T) error {
	_, err := e.jobs(e.key(t, "shop", "reader")).Enqueue(e.ctx(), connect.NewRequest(&pb.EnqueueRequest{Namespace: "shop", Jobs: []*pb.NewJob{{Queue: "q", Kind: "k"}}}))
	return err
}
