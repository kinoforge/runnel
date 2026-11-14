package spec

import (
	"os"
	"strings"
	"testing"
)

func Rule(t testing.TB, ids ...string) {
	t.Helper()
	want := os.Getenv("RUNNEL_RULES")
	if want == "" || Selected(want, ids) {
		return
	}
	t.Skipf("checks %s, not %s", strings.Join(ids, ","), want)
}

func Selected(want string, ids []string) bool {
	for w := range strings.SplitSeq(want, ",") {
		w = strings.TrimSpace(w)
		for _, id := range ids {
			if w != "" && strings.HasPrefix(id, w) {
				return true
			}
		}
	}
	return false
}
