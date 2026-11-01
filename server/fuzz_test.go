package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func FuzzPreferWait(f *testing.F) {
	spec.Rule(f, "S05")
	for _, s := range []string{"wait=10", "respond-async, wait=5", "wait=-1", "wait=99999999999999999999", "wait=\"3\"", ";;,,", ""} {
		f.Add(s)
	}
	s := &Server{cfg: Config{MaxResultWait: time.Minute}}
	f.Fuzz(func(t *testing.T, h string) {
		r := &http.Request{Header: http.Header{"Prefer": {h}}}
		if d := s.preferWait(r); d < 0 || d > time.Minute {
			t.Fatalf("Prefer %q gave wait %v", h, d)
		}
	})
}
