package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/config"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/internal/testcert"
	"github.com/kinoforge/runnel/remote"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/sqlite"
)

func store(t *testing.T) runnel.Store {
	t.Helper()
	s, err := sqlite.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type authReply struct {
	StatusCode int
	challenge  string
}

func status(t *testing.T, c *http.Client, url string, auth func(*http.Request)) authReply {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if auth != nil {
		auth(req)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return authReply{resp.StatusCode, resp.Header.Get("WWW-Authenticate")}
}

func basic(user, pass string) func(*http.Request) {
	return func(r *http.Request) { r.SetBasicAuth(user, pass) }
}

func TestBasicAuth(t *testing.T) {
	spec.Rule(t, "A01", "A07")
	srv, _ := setup(t)
	c := srv.Client()
	if code := status(t, c, srv.URL+"/v1/namespaces", basic("admin", admin)).StatusCode; code != 200 {
		t.Fatalf("admin password: %d", code)
	}
	for name, auth := range map[string]func(*http.Request){
		"wrong password": basic("admin", admin+"x"),
		"other user":     basic("root", admin),
		"empty password": basic("admin", ""),
	} {
		resp := status(t, c, srv.URL+"/v1/namespaces", auth)
		if resp.StatusCode != 401 || !strings.Contains(resp.challenge, "Basic") {
			t.Errorf("%s: %d %q", name, resp.StatusCode, resp.challenge)
		}
	}
	call(t, srv, admin, "PUT", "/v1/namespaces/app", nil)
	id, secret, _ := runnel.ParseAPIKey(key(t, srv, "app", "reader"))
	if code := status(t, c, srv.URL+"/v1/ns/app/stats", basic(id, secret)).StatusCode; code != 200 {
		t.Fatalf("key as user and password: %d", code)
	}
	if code := status(t, c, srv.URL+"/v1/namespaces", basic(id, secret)).StatusCode; code != 403 {
		t.Fatalf("key on an admin route: %d", code)
	}
	if code := status(t, c, srv.URL+"/v1/ns/app/stats", basic(id, strings.Repeat("0", len(secret)))).StatusCode; code != 401 {
		t.Fatalf("key with a wrong secret: %d", code)
	}
}

func TestAdminUserName(t *testing.T) {
	spec.Rule(t, "A07")
	srv := httptest.NewServer(server.New(store(t), server.Config{AdminToken: admin, AdminUser: "ops"}))
	defer srv.Close()
	if code := status(t, srv.Client(), srv.URL+"/v1/namespaces", basic("ops", admin)).StatusCode; code != 200 {
		t.Fatalf("renamed admin: %d", code)
	}
	if code := status(t, srv.Client(), srv.URL+"/v1/namespaces", basic("admin", admin)).StatusCode; code != 401 {
		t.Fatalf("old admin name: %d", code)
	}
}

func TestNoAuth(t *testing.T) {
	spec.Rule(t, "A08")
	srv := httptest.NewServer(server.New(store(t), server.Config{NoAuth: true}))
	defer srv.Close()
	if code, _ := call(t, srv, "", "PUT", "/v1/namespaces/open", nil); code != 201 {
		t.Fatalf("create namespace without credentials: %d", code)
	}
	if code, _ := call(t, srv, "", "GET", "/v1/system", nil); code != 200 {
		t.Fatalf("admin route without credentials: %d", code)
	}
}

func tlsServer(t *testing.T, ca *testcert.Authority, tc config.TLS, cfg server.Config) *httptest.Server {
	t.Helper()
	tc.Cert, tc.Key = ca.Server(t)
	tc.ClientCA, tc.MinVersion = ca.File, "1.2"
	built, err := tc.Build()
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(server.New(store(t), cfg))
	srv.TLS = built
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func endpoint(t *testing.T, raw string) remote.Endpoint {
	t.Helper()
	e, err := remote.ParseURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func connects(e remote.Endpoint, path string) bool {
	resp, err := e.HTTPClient(0).Get(e.Base + path)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

func TestClientCertificates(t *testing.T) {
	spec.Rule(t, "A09")
	ca := testcert.New(t)
	srv := tlsServer(t, ca, config.TLS{ClientNames: []string{"dash"}}, server.Config{AdminToken: admin, CertAdmin: true, CertNames: []string{"dash"}})
	host := strings.TrimPrefix(srv.URL, "https://")
	with := func(name string) string {
		cert, key := ca.Client(t, name)
		return "https://" + host + "?ca=" + ca.File + "&cert=" + cert + "&key=" + key
	}
	cases := map[string]struct {
		url  string
		want int
	}{
		"named cert":        {with("dash"), 200},
		"other name":        {with("intruder"), 401},
		"no cert":           {"https://" + host + "?ca=" + ca.File, 401},
		"no cert, password": {"https://admin:" + admin + "@" + host + "?ca=" + ca.File, 200},
		"wrong password":    {"https://admin:nope@" + host + "?ca=" + ca.File, 401},
	}
	for name, tc := range cases {
		e := endpoint(t, tc.url)
		if code := status(t, e.HTTPClient(0), e.Base+"/v1/namespaces", e.Authorize).StatusCode; code != tc.want {
			t.Errorf("%s: %d, want %d", name, code, tc.want)
		}
	}
	other := testcert.New(t)
	cert, key := other.Client(t, "dash")
	e := endpoint(t, "https://"+host+"?ca="+ca.File+"&cert="+cert+"&key="+key)
	if connects(e, "/v1/namespaces") {
		t.Fatal("a cert from another authority got through")
	}
}

func TestRequiredClientCertificate(t *testing.T) {
	spec.Rule(t, "A09")
	ca := testcert.New(t)
	srv := tlsServer(t, ca, config.TLS{RequireClientCert: true}, server.Config{AdminToken: admin, CertAdmin: true})
	e := endpoint(t, "https://admin:"+admin+"@"+strings.TrimPrefix(srv.URL, "https://")+"?ca="+ca.File)
	if connects(e, "/healthz") {
		t.Fatal("a client without a cert finished the handshake")
	}
}

func TestRemoteOpen(t *testing.T) {
	spec.Rule(t, "S18")
	ca := testcert.New(t)
	srv := tlsServer(t, ca, config.TLS{}, server.Config{AdminToken: admin})
	host := strings.TrimPrefix(srv.URL, "https://")
	ctx := context.Background()
	c, err := remote.Open("https://admin:"+admin+"@"+host+"/?ca="+ca.File, "app")
	if err != nil {
		t.Fatal(err)
	}
	e := endpoint(t, "https://admin:"+admin+"@"+host+"?ca="+ca.File)
	req, _ := http.NewRequest("PUT", e.Base+"/v1/namespaces/app", nil)
	e.Authorize(req)
	resp, err := e.HTTPClient(0).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if err := c.SetQueue(ctx, runnel.QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, "q", "k", nil); err != nil {
		t.Fatal(err)
	}
	anon, err := remote.Open("https://"+host+"?ca="+ca.File, "app")
	if err != nil {
		t.Fatal(err)
	}
	var apiErr *remote.APIError
	if _, err := anon.Enqueue(ctx, "q", "k", nil); !errors.As(err, &apiErr) || apiErr.Status != 401 {
		t.Fatalf("no credentials: %v", err)
	}
}
