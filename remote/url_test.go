package remote

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestParseURL(t *testing.T) {
	spec.Rule(t, "S18")
	e, err := ParseURL("https://admin:p%40ss%3Aword@runnel.internal:8700/base/")
	if err != nil {
		t.Fatal(err)
	}
	if e.Base != "https://runnel.internal:8700/base" || !e.HasCredentials() || e.TLS != nil {
		t.Fatalf("endpoint %v", e)
	}
	req, _ := http.NewRequest("GET", e.Base, nil)
	e.Authorize(req)
	if user, pass, ok := req.BasicAuth(); !ok || user != "admin" || pass != "p@ss:word" {
		t.Fatalf("basic auth %q %q %v", user, pass, ok)
	}
	open, err := ParseURL("http://localhost:8700")
	if err != nil {
		t.Fatal(err)
	}
	req, _ = http.NewRequest("GET", open.Base, nil)
	open.Authorize(req)
	if open.HasCredentials() || req.Header.Get("Authorization") != "" {
		t.Fatal("a url without credentials sent some")
	}
}

func TestParseURLRejects(t *testing.T) {
	spec.Rule(t, "S18")
	for _, raw := range []string{
		"ftp://host", "http://", "http://host#frag", "http://host?foo=1", "http://host?ca=x",
		"https://host?ca=a&ca=b", "https://host?ca=", "https://host?cert=c.pem", "https://host?ca=/no/such.pem",
		"mailto:x@y", "https://admin:s3cr3t-pass@host:port",
	} {
		_, err := ParseURL(raw)
		if err == nil {
			t.Errorf("%s: accepted", raw)
			continue
		}
		if strings.Contains(err.Error(), "s3cr3t-pass") {
			t.Errorf("%s: error shows the password: %v", raw, err)
		}
	}
}

func TestEndpointHidesPassword(t *testing.T) {
	spec.Rule(t, "S18")
	e, err := ParseURL("https://admin:s3cr3t-pass@host:8700")
	if err != nil {
		t.Fatal(err)
	}
	if s := fmt.Sprintf("%v %+v %#v %s", e, e, e, e); strings.Contains(s, "s3cr3t-pass") {
		t.Fatalf("printed endpoint shows the password: %s", s)
	}
}
