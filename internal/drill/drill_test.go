//go:build drill

package drill_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func setupNamespace(t *testing.T, url string) {
	t.Helper()
	if code := admin(t, url, "PUT", "/v1/namespaces/"+ns, nil); code != 200 && code != 201 {
		t.Fatalf("make namespace: %d", code)
	}
}

func listAll(t *testing.T, url string) map[string]*runnel.Job {
	t.Helper()
	c := client(url)
	out := map[string]*runnel.Job{}
	cursor := ""
	for {
		page, next, err := c.List(context.Background(), runnel.Query{Limit: 500, Cursor: cursor})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, j := range page {
			out[j.ID] = j
		}
		if next == "" {
			return out
		}
		cursor = next
	}
}

func eventsPerJob(t *testing.T, url string) map[string][]runnel.State {
	t.Helper()
	out := map[string][]runnel.State{}
	after := int64(0)
	for {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/ns/%s/events?after=%d&limit=1000", url, ns, after), nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var page struct{ Items []runnel.Event }
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Items {
			out[e.JobID] = append(out[e.JobID], e.State)
		}
		if len(page.Items) == 0 {
			return out
		}
		after = page.Items[len(page.Items)-1].Seq
	}
}

func missing(acked []string, jobs map[string]*runnel.Job) []string {
	var out []string
	for _, id := range acked {
		if jobs[id] == nil {
			out = append(out, id)
		}
	}
	return out
}

func allDone(t *testing.T, url string, acked []string, within time.Duration) map[string]*runnel.Job {
	t.Helper()
	var jobs map[string]*runnel.Job
	eventually(t, "every job completed", within, func() bool {
		jobs = listAll(t, url)
		for _, j := range jobs {
			if j.State != runnel.StateCompleted {
				return false
			}
		}
		return len(missing(acked, jobs)) == 0
	})
	events := eventsPerJob(t, url)
	for id := range jobs {
		if got := events[id]; len(got) != 1 || got[0] != runnel.StateCompleted {
			t.Errorf("job %s has events %v, want one completed", id, got)
		}
	}
	return jobs
}

func TestKillUnderLoad(t *testing.T) {
	spec.Rule(t, "D01", "X04", "L02")
	srv := serve(t, "hybrid:"+t.TempDir())
	px := newProxy(t, srv.addr)
	setupNamespace(t, srv.url())
	w := work(t, px.url(), 2)
	p := produce(px.url(), 4, map[string]int{"n": 1})
	eventually(t, "load before the kill", time.Minute, func() bool { return p.count() >= 300 })
	srv.restart()
	before := p.count()
	eventually(t, "load after the restart", time.Minute, func() bool { return p.count() >= before+300 })
	acked, _ := p.halt()
	jobs := allDone(t, srv.url(), acked, 2*time.Minute)
	w.mu.Lock()
	defer w.mu.Unlock()
	for id := range jobs {
		if w.runs[id] == 0 {
			t.Errorf("job %s completed without running", id)
		}
	}
	t.Logf("%d jobs acknowledged, %d stored, all completed across a kill", len(acked), len(jobs))
}

func TestNetworkCut(t *testing.T) {
	spec.Rule(t, "D02", "L01", "L02")
	srv := serve(t, "hybrid:"+t.TempDir())
	px := newProxy(t, srv.addr)
	setupNamespace(t, srv.url())
	work(t, px.url(), 2)
	p := produce(px.url(), 2, nil)
	eventually(t, "load before the cut", time.Minute, func() bool { return p.count() >= 200 })
	px.cut()
	time.Sleep(time.Second)
	cutAt := p.count()
	time.Sleep(7 * time.Second)
	if got := p.count(); got != cutAt {
		t.Fatalf("%d enqueues acknowledged while the network was cut", got-cutAt)
	}
	running := 0
	for _, j := range listAll(t, srv.url()) {
		if j.State == runnel.StateRunning {
			running++
		}
	}
	if running != 0 {
		t.Errorf("%d jobs still running on cut-off workers after their leases ended", running)
	}
	px.heal()
	eventually(t, "load after the cut", time.Minute, func() bool { return p.count() >= cutAt+200 })
	acked, _ := p.halt()
	jobs := allDone(t, srv.url(), acked, 2*time.Minute)
	t.Logf("%d jobs acknowledged, %d stored, all completed across an 8s cut", len(acked), len(jobs))
}

func TestRestartMidWrite(t *testing.T) {
	spec.Rule(t, "D04", "X04")
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rnd := rand.New(rand.NewPCG(seed, seed))
	srv := serve(t, "hybrid:"+t.TempDir())
	setupNamespace(t, srv.url())
	var acked []string
	for round := range 6 {
		p := produce(srv.url(), 4, map[string]string{"round": fmt.Sprint(round)})
		time.Sleep(time.Duration(100+rnd.IntN(500)) * time.Millisecond)
		srv.kill()
		got, _ := p.halt()
		acked = append(acked, got...)
		srv.launch()
		jobs := listAll(t, srv.url())
		if lost := missing(acked, jobs); len(lost) != 0 {
			t.Fatalf("round %d: %d of %d acknowledged jobs lost: %v", round, len(lost), len(acked), lost[:min(5, len(lost))])
		}
		for id, j := range jobs {
			if j.State != runnel.StateWaiting || j.Queue != "work" {
				t.Fatalf("round %d: job %s came back %s on %q", round, id, j.State, j.Queue)
			}
		}
	}
	if len(acked) == 0 {
		t.Fatal("no write was acknowledged")
	}
	t.Logf("%d acknowledged jobs survived 6 kills during writes", len(acked))
}

func TestRestoreFromBackup(t *testing.T) {
	spec.Rule(t, "D05", "X07", "S14")
	src := serve(t, "hybrid:"+t.TempDir())
	setupNamespace(t, src.url())
	p := produce(src.url(), 4, map[string]int{"n": 1})
	eventually(t, "load before the backup", time.Minute, func() bool { return p.count() >= 300 })
	p.mu.Lock()
	before := append([]string(nil), p.acked...)
	p.mu.Unlock()
	file := filepath.Join(t.TempDir(), "backup.tar")
	req, _ := http.NewRequest("GET", src.url()+"/v1/backup", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("backup: %v %v", resp, err)
	}
	f, _ := os.Create(file)
	_, err = io.Copy(f, resp.Body)
	resp.Body.Close()
	if cerr := f.Close(); err != nil || cerr != nil {
		t.Fatalf("saving the backup: %v %v", err, cerr)
	}
	p.halt()

	dir := filepath.Join(t.TempDir(), "restored")
	if out, err := exec.Command(binary, "restore", "--store", "hybrid:"+dir, file).CombinedOutput(); err != nil {
		t.Fatalf("restore: %v\n%s", err, out)
	}
	if out, err := exec.Command(binary, "restore", "--store", "hybrid:"+dir, file).CombinedOutput(); err == nil {
		t.Fatalf("restore into a store that is not empty worked:\n%s", out)
	}
	restored := serve(t, "hybrid:"+dir)
	jobs := listAll(t, restored.url())
	if lost := missing(before, jobs); len(lost) != 0 {
		t.Fatalf("%d of %d jobs acknowledged before the backup are missing: %v", len(lost), len(before), lost[:min(5, len(lost))])
	}
	if !src.running() {
		t.Fatal("the source server stopped during the backup")
	}
	t.Logf("restored %d jobs, %d acknowledged before the backup began", len(jobs), len(before))
}
