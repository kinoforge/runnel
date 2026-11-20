package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/sqlite"
)

func setupWith(t *testing.T, cfg server.Config) (*httptest.Server, runnel.Store) {
	t.Helper()
	s, err := sqlite.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg.AdminToken = admin
	srv := httptest.NewServer(server.New(s, cfg))
	t.Cleanup(srv.Close)
	return srv, s
}

func claimOne(t *testing.T, srv *httptest.Server, k, ns string) map[string]any {
	t.Helper()
	code, out := call(t, srv, k, "POST", "/v1/ns/"+ns+"/claim", map[string]any{"worker": "w", "queues": []string{"q"}, "limit": 1, "lease": "60s"})
	jobs, _ := out["jobs"].([]any)
	if code != 200 || len(jobs) != 1 {
		t.Fatalf("claim %d %v", code, out)
	}
	return jobs[0].(map[string]any)
}

func TestReservedCallbackQueueOverAPI(t *testing.T) {
	spec.Rule(t, "S03")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/app", map[string]any{})
	work, adm := key(t, srv, "app", "worker"), key(t, srv, "app", "admin")
	if code, out := call(t, srv, work, "POST", "/v1/ns/app/claim", map[string]any{"worker": "w", "queues": []string{runnel.CallbackQueue}, "limit": 1}); code != 400 {
		t.Fatalf("a worker key claimed from %s: %d %v", runnel.CallbackQueue, code, out)
	}
	if code, _ := call(t, srv, adm, "PUT", "/v1/ns/app/queues/"+runnel.CallbackQueue, map[string]any{"name": runnel.CallbackQueue}); code != 400 {
		t.Fatalf("PUT of the reserved queue's settings: %d", code)
	}
	if got := ask(t, srv, "PATCH", "/v1/ns/app/queues/"+runnel.CallbackQueue, `{"paused":true}`, "Content-Type", mergePatch); got.code != 400 {
		t.Fatalf("PATCH of the reserved queue's settings: %d", got.code)
	}
}

func TestReservedCallbackQueueOverRPC(t *testing.T) {
	spec.Rule(t, "S03", "G01")
	e := rpcSetup(t)
	e.namespace(t, "app")
	w := e.workers(e.key(t, "app", "worker"))
	_, err := w.Claim(e.ctx(), connect.NewRequest(&pb.ClaimRequest{Namespace: "app", Worker: "w", Queues: []string{runnel.CallbackQueue}, Limit: 1}))
	if c, _ := code(err); c != connect.CodeInvalidArgument {
		t.Fatalf("rpc claim from the reserved queue: %v", err)
	}
	st := w.Work(e.ctx())
	start := &pb.WorkRequest{Msg: &pb.WorkRequest_Start{Start: &pb.WorkStart{Namespace: "app", Worker: "w", Queues: []string{runnel.CallbackQueue}}}}
	_ = st.Send(start)
	if _, err := st.Receive(); func() connect.Code { c, _ := code(err); return c }() != connect.CodeInvalidArgument {
		t.Fatalf("work stream on the reserved queue: %v", err)
	}
	_ = st.CloseRequest()
	_ = st.CloseResponse()
}

func TestUnauthenticatedWorkStreamEndsAtOnce(t *testing.T) {
	spec.Rule(t, "G01", "A01")
	e := rpcSetup(t)
	ctx, cancel := context.WithTimeout(e.ctx(), 5*time.Second)
	defer cancel()
	st := e.workers("").Work(ctx)
	_ = st.Send(nil)
	_, err := st.Receive()
	if c, _ := code(err); c != connect.CodeUnauthenticated {
		t.Fatalf("work stream without credentials: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the server held the unauthenticated stream open")
	}
	_ = st.CloseResponse()
}

func TestExtendWithoutLeaseUsesTheDefault(t *testing.T) {
	spec.Rule(t, "S16", "L01")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/app", map[string]any{})
	k := key(t, srv, "app", "admin")
	call(t, srv, k, "POST", "/v1/ns/app/jobs", map[string]any{"queue": "q", "kind": "k"})
	j := claimOne(t, srv, k, "app")
	id := j["id"].(string)
	code, out := call(t, srv, k, "POST", "/v1/ns/app/extend", map[string]any{"worker": "w", "ids": []string{id}})
	if code != 200 {
		t.Fatalf("extend %d %v", code, out)
	}
	_, got := call(t, srv, k, "GET", "/v1/ns/app/jobs/"+id, nil)
	until, err := time.Parse(time.RFC3339Nano, got["lease_until"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if left := time.Until(until); left < 25*time.Second || left > 35*time.Second {
		t.Fatalf("lease after an extend without a lease ends in %s, want about the 30s default", left)
	}
}

func TestFinishRefusesAResultOverTheLimit(t *testing.T) {
	spec.Rule(t, "S16", "J02")
	srv, _ := setupWith(t, server.Config{Limits: runnel.Limits{MaxPayload: 1 << 10}})
	call(t, srv, admin, "PUT", "/v1/namespaces/app", map[string]any{})
	k := key(t, srv, "app", "admin")
	call(t, srv, k, "POST", "/v1/ns/app/jobs", map[string]any{"queue": "q", "kind": "k"})
	j := claimOne(t, srv, k, "app")
	big := `"` + strings.Repeat("r", 2<<10) + `"`
	path := "/v1/ns/app/jobs/" + j["id"].(string) + "/finish"
	body := `{"worker":"w","version":` + jsonNumber(j["version"]) + `,"outcome":{"kind":"done","result":` + big + `}}`
	if code, out := call(t, srv, k, "POST", path, body); code != 400 {
		t.Fatalf("finish with a result over max_payload: %d %v", code, out)
	}
}

func jsonNumber(v any) string {
	f, _ := v.(float64)
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func TestServerCapsReachEveryEnqueue(t *testing.T) {
	spec.Rule(t, "S16")
	srv, _ := setupWith(t, server.Config{MaxBatch: 2, MaxDelay: time.Hour})
	call(t, srv, admin, "PUT", "/v1/namespaces/app", map[string]any{})
	k := key(t, srv, "app", "admin")
	late := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	for _, field := range []string{"run_at", "deadline"} {
		if code, out := call(t, srv, k, "POST", "/v1/ns/app/jobs", map[string]any{"queue": "q", "kind": "k", field: late}); code != 400 {
			t.Fatalf("%s past max_delay: %d %v", field, code, out)
		}
	}
	steps := []map[string]any{{"name": "a", "queue": "q", "kind": "k"}, {"name": "b", "queue": "q", "kind": "k"}, {"name": "c", "queue": "q", "kind": "k"}}
	if code, out := call(t, srv, k, "POST", "/v1/ns/app/flows", map[string]any{"steps": steps}); code != 400 {
		t.Fatalf("flow over max_batch: %d %v", code, out)
	}
	if code, out := call(t, srv, k, "POST", "/v1/ns/app/flows", map[string]any{"steps": steps[:2]}); code >= 300 {
		t.Fatalf("flow within max_batch: %d %v", code, out)
	}
}

func TestFinalAndNotFinalStayApartOverTheAPI(t *testing.T) {
	spec.Rule(t, "S02")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/app", map[string]any{})
	c := remote.New(srv.URL, key(t, srv, "app", "admin"), "app")
	ctx := t.Context()
	waiting, err := c.Enqueue(ctx, "q", "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Retry(ctx, waiting.ID); !errors.Is(err, runnel.ErrNotFinal) || errors.Is(err, runnel.ErrFinal) {
		t.Fatalf("retry of a waiting job: %v", err)
	}
	if _, err := c.Cancel(ctx, waiting.ID); err != nil {
		t.Fatal(err)
	}
	done, _ := c.Enqueue(ctx, "q", "k", nil)
	k := key(t, srv, "app", "worker")
	j := claimOne(t, srv, k, "app")
	if code, out := call(t, srv, k, "POST", "/v1/ns/app/jobs/"+done.ID+"/finish", map[string]any{"worker": "w", "version": j["version"], "outcome": map[string]any{"kind": "done"}}); code != 200 {
		t.Fatalf("finish %d %v", code, out)
	}
	if _, err := c.Cancel(ctx, done.ID); !errors.Is(err, runnel.ErrFinal) || errors.Is(err, runnel.ErrNotFinal) {
		t.Fatalf("cancel of a completed job: %v", err)
	}
}

func TestAnEmptyIfMatchIsAPrecondition(t *testing.T) {
	spec.Rule(t, "S19")
	srv, _ := setup(t)
	req, _ := http.NewRequest("PUT", srv.URL+"/v1/ns/default/queues/q", strings.NewReader(`{"name":"q"}`))
	req.Header.Set("Authorization", "Bearer "+admin)
	req.Header["If-Match"] = []string{""}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("PUT with an empty If-Match on a missing setting: %d", resp.StatusCode)
	}
}

func TestRemoteClientDoesNotFollowRedirects(t *testing.T) {
	spec.Rule(t, "S18")
	var hit atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Store(true) }))
	defer target.Close()
	mover := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer mover.Close()
	c := remote.New(mover.URL, "secret", "app")
	if _, err := c.Enqueue(t.Context(), "q", "k", nil); err == nil {
		t.Fatal("an enqueue answered with a redirect succeeded")
	}
	if hit.Load() {
		t.Fatal("the remote client followed a redirect and sent its key elsewhere")
	}
}
