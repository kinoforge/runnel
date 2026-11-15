//go:build drill

package drill_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/remote"
)

const (
	token = "drill-admin-token-0123456789"
	ns    = "drill"
)

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "runnel-drill-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binary = filepath.Join(dir, "runnel")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/runnel")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building runnel:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) tail() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.buf.String()
	return s[max(0, len(s)-4000):]
}

type server struct {
	t      *testing.T
	store  string
	addr   string
	env    []string
	cmd    *exec.Cmd
	exited chan struct{}
	log    *logs
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func serve(t *testing.T, store string, env ...string) *server {
	t.Helper()
	s := &server{t: t, store: store, addr: freeAddr(t), env: env, log: &logs{}}
	s.launch()
	t.Cleanup(func() {
		if s.running() {
			s.kill()
		}
		if t.Failed() {
			t.Logf("server log:\n%s", s.log.tail())
		}
	})
	return s
}

func (s *server) launch() {
	s.t.Helper()
	s.cmd = exec.Command(binary, "serve", "--store", s.store, "--addr", s.addr, "--admin-token", token)
	s.cmd.Env = append(os.Environ(), append([]string{"RUNNEL_MAINTAINER_INTERVAL=100ms", "RUNNEL_LOG_LEVEL=warn"}, s.env...)...)
	s.cmd.Stdout, s.cmd.Stderr = s.log, s.log
	if err := s.cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	exited := make(chan struct{})
	s.exited = exited
	go func(c *exec.Cmd) {
		_ = c.Wait()
		close(exited)
	}(s.cmd)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			s.t.Fatalf("server exited at start:\n%s", s.log.tail())
		default:
		}
		resp, err := http.Get(s.url() + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.t.Fatalf("server not ready:\n%s", s.log.tail())
}

func (s *server) running() bool {
	select {
	case <-s.exited:
		return false
	default:
		return true
	}
}

func (s *server) kill() {
	_ = s.cmd.Process.Signal(syscall.SIGKILL)
	<-s.exited
}

func (s *server) restart() {
	s.t.Helper()
	s.kill()
	s.launch()
}

func (s *server) url() string { return "http://" + s.addr }

type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	down   bool
	conns  map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(func() { _ = ln.Close(); p.cut() })
	return p
}

func (p *proxy) accept() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			_ = in.Close()
			continue
		}
		out, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			_ = in.Close()
			continue
		}
		if !p.track(in, out) {
			continue
		}
		go p.pipe(in, out)
		go p.pipe(out, in)
	}
}

func (p *proxy) track(in, out net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down {
		_ = in.Close()
		_ = out.Close()
		return false
	}
	p.conns[in], p.conns[out] = struct{}{}, struct{}{}
	return true
}

func (p *proxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

func (p *proxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close()
	}
}

func (p *proxy) heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

func (p *proxy) url() string { return "http://" + p.ln.Addr().String() }

func client(url string) *remote.Client {
	return remote.New(url, token, ns).WithWaits(0, time.Second).WithHTTPClient(&http.Client{Timeout: 5 * time.Second})
}

func admin(t *testing.T, url, method, path string, body any) int {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url+path, r)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

type producers struct {
	mu     sync.Mutex
	acked  []string
	failed int
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func produce(url string, n int, payload any) *producers {
	ctx, cancel := context.WithCancel(context.Background())
	p := &producers{cancel: cancel}
	for range n {
		c := client(url)
		p.wg.Go(func() {
			for ctx.Err() == nil {
				res, err := c.Enqueue(ctx, "work", "k", payload)
				p.mu.Lock()
				if err == nil {
					p.acked = append(p.acked, res.ID)
				} else {
					p.failed++
				}
				p.mu.Unlock()
				if err != nil {
					time.Sleep(20 * time.Millisecond)
				}
			}
		})
	}
	return p
}

func (p *producers) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.acked)
}

func (p *producers) halt() ([]string, int) {
	p.cancel()
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.acked...), p.failed
}

type workers struct {
	mu   sync.Mutex
	runs map[string]int
	stop func()
}

func work(t *testing.T, url string, n int) *workers {
	t.Helper()
	w := &workers{runs: map[string]int{}}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range n {
		rw := remote.NewWorker(client(url), runnel.WorkerConfig{Queues: []string{"work"}, Concurrency: 4, Lease: 5 * time.Second,
			PollInterval: 50 * time.Millisecond, ShutdownTimeout: 2 * time.Second, Logger: quiet()})
		rw.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
			w.mu.Lock()
			w.runs[j.ID]++
			w.mu.Unlock()
			return nil
		})
		wg.Go(func() { _ = rw.Run(ctx) })
	}
	w.stop = func() {
		cancel()
		wg.Wait()
	}
	t.Cleanup(w.stop)
	return w
}

func eventually(t *testing.T, what string, within time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", within, what)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
