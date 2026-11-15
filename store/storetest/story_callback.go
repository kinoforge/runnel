package storetest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

type inbox struct {
	reader runnel.CallbackReader
	mu     sync.Mutex
	got    []runnel.CallbackBody
	paths  []string
	bad    []error
}

func (in *inbox) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cb, err := in.reader.Read(r)
	in.mu.Lock()
	defer in.mu.Unlock()
	if err != nil {
		in.bad = append(in.bad, err)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	in.got = append(in.got, cb)
	in.paths = append(in.paths, r.URL.Path)
}

func (in *inbox) seen() ([]runnel.CallbackBody, []string, []error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]runnel.CallbackBody(nil), in.got...), append([]string(nil), in.paths...), append([]error(nil), in.bad...)
}

func testStoryWelcomeEmail(t *testing.T, open Opener) {
	spec.Rule(t, "U01", "H01", "H02", "H05", "O01", "O04", "M05")
	e := setup(t, open)
	const secret = "shop-callback-secret"
	if err := runnel.PutNamespace(e.ctx, e.s, runnel.Namespace{Name: e.ns, CallbackSecret: secret}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runnel.DeleteNamespace(context.Background(), e.s, e.ns) })
	rcv := &inbox{reader: runnel.CallbackReader{Secrets: []string{secret}}}
	srv := httptest.NewTLSServer(rcv)
	defer srv.Close()

	w := runnel.NewWorker(e.s, e.ns, fast("emails"))
	w.HandleFunc("welcome", func(ctx context.Context, j *runnel.Job) error {
		var in struct{ To string }
		if err := j.Bind(&in); err != nil {
			return runnel.Fail(err)
		}
		if in.To == "" {
			return runnel.Fail(errors.New("no address"))
		}
		return j.SetResult(map[string]string{"sent": in.To})
	})
	stopWorker := runWorker(t, e, w)
	defer stopWorker()
	stopMaintainer := e.maintain(t, runnel.MaintainerConfig{Callbacks: runnel.CallbackConfig{Enabled: true, Client: srv.Client()}})
	defer stopMaintainer()

	hook := runnel.Callback(srv.URL + "/hooks/runnel")
	sent, err := e.c.Enqueue(e.ctx, "emails", "welcome", map[string]string{"to": "ana@example.com"}, hook)
	if err != nil {
		t.Fatal(err)
	}
	bounced, err := e.c.Enqueue(e.ctx, "emails", "welcome", map[string]string{"to": ""}, hook)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both jobs final", 10*time.Second, func() bool {
		return e.get(sent.ID).State == runnel.StateCompleted && e.get(bounced.ID).State == runnel.StateDead
	})
	waitFor(t, "both deliveries done", 20*time.Second, func() bool { return e.count(runnel.CallbackQueue, runnel.StateCompleted) == 2 })

	got, paths, bad := rcv.seen()
	if len(bad) != 0 || len(got) != 2 {
		t.Fatalf("receiver got %d callbacks and refused %d: %v", len(got), len(bad), bad)
	}
	byJob := map[string]runnel.CallbackBody{}
	for i, cb := range got {
		if paths[i] != "/hooks/runnel" {
			t.Errorf("callback posted to %s", paths[i])
		}
		byJob[cb.Job.ID] = cb
	}
	ok := byJob[sent.ID]
	if ok.Event != "job.completed" || ok.Job.State != runnel.StateCompleted || ok.Job.Attempt != 1 ||
		string(ok.Job.Result) != `{"sent":"ana@example.com"}` || ok.Job.Namespace != e.ns || ok.Job.Kind != "welcome" {
		t.Errorf("completed callback %+v", ok)
	}
	dead := byJob[bounced.ID]
	if dead.Event != "job.dead" || dead.Job.State != runnel.StateDead || dead.Job.Attempt != 1 || dead.Job.LastError != "no address" {
		t.Errorf("dead callback %+v", dead)
	}
	if ok.Delivery == 0 || ok.Delivery == dead.Delivery {
		t.Errorf("deliveries %d and %d are not distinct", ok.Delivery, dead.Delivery)
	}
	for _, id := range []string{sent.ID, bounced.ID} {
		if evs := e.events(id); len(evs) != 1 {
			t.Errorf("job %s has %d events", id, len(evs))
		}
	}
}
