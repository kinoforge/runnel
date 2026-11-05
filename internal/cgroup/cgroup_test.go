package cgroup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadLimit(t *testing.T) {
	dir := t.TempDir()
	write := func(name, v string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(v), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := map[string]int64{"524288000\n": 524288000, "max\n": 0, "9223372036854771712": 0, "": 0}
	for v, want := range cases {
		if got := readLimit([]string{write("limit", v)}); got != want {
			t.Errorf("%q: got %d want %d", v, got, want)
		}
	}
	if got := readLimit([]string{filepath.Join(dir, "missing"), write("v1", "1048576")}); got != 1<<20 {
		t.Errorf("fallback: got %d", got)
	}
}
