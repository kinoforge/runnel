package main

import (
	"strings"
	"testing"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestRedactHidesEveryPassword(t *testing.T) {
	spec.Rule(t, "S18")
	for _, raw := range []string{
		"postgres://runnel:p%40ss@db:5432/runnel",
		"redis://:hunter2@cache:6379/0",
		"mysql://root:a@b@tcp(db:3306)/runnel",
		"root:s3cret@tcp(db:3306)/runnel",
	} {
		got := redact(raw)
		for _, secret := range []string{"p%40ss", "p@ss", "hunter2", "a@b", "s3cret"} {
			if strings.Contains(got, secret) {
				t.Errorf("redact(%q) = %q shows %q", raw, got, secret)
			}
		}
	}
	if got := redact("sqlite:bench.db"); got != "sqlite:bench.db" {
		t.Errorf("a URL without a password changed: %q", got)
	}
}
