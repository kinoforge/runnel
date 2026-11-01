// Package spec ties tests to the rules of SPEC.md.
package spec

import (
	"os"
	"strings"
	"testing"
)

// Rule marks the test as checking the given SPEC.md rules, such as "C04".
// When RUNNEL_RULES lists rule ids or area letters, separated by commas, a
// test that names none of them is skipped, so
//
//	RUNNEL_RULES=C04,W go test ./...
//
// runs only the tests of rule C04 and of the W area.
func Rule(t testing.TB, ids ...string) {
	t.Helper()
	want := os.Getenv("RUNNEL_RULES")
	if want == "" || Selected(want, ids) {
		return
	}
	t.Skipf("checks %s, not %s", strings.Join(ids, ","), want)
}

// Selected reports whether a RUNNEL_RULES value picks any of ids.
func Selected(want string, ids []string) bool {
	for _, w := range strings.Split(want, ",") {
		w = strings.TrimSpace(w)
		for _, id := range ids {
			if w != "" && strings.HasPrefix(id, w) {
				return true
			}
		}
	}
	return false
}
