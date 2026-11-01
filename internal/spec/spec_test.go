package spec

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const root = "../.."

var ruleLine = regexp.MustCompile(`(?m)^- \*\*([A-Z]\d{2})\*\*`)

func rules(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "SPEC.md"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range ruleLine.FindAllSubmatch(b, -1) {
		out = append(out, string(m[1]))
	}
	if len(out) == 0 {
		t.Fatal("SPEC.md lists no rules")
	}
	return out
}

func isRule(c *ast.CallExpr) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Rule" {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	return ok && x.Name == "spec"
}

func skipDir(path string, d fs.DirEntry) bool {
	name := d.Name()
	return path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "bin")
}

func marks(t *testing.T) map[string][]string {
	t.Helper()
	found := map[string][]string{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDir(path, d) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isRule(call) {
				return true
			}
			pos := fset.Position(call.Pos())
			where := fmt.Sprintf("%s:%d", strings.TrimPrefix(pos.Filename, root+"/"), pos.Line)
			if len(call.Args) < 2 {
				t.Errorf("%s: spec.Rule names no rule", where)
			}
			for _, arg := range call.Args[min(1, len(call.Args)):] {
				lit, ok := arg.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					t.Errorf("%s: spec.Rule takes rule ids as string literals", where)
					continue
				}
				id, _ := strconv.Unquote(lit.Value)
				found[id] = append(found[id], where)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestRulesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, id := range rules(t) {
		if seen[id] {
			t.Errorf("rule %s is listed twice in SPEC.md", id)
		}
		seen[id] = true
	}
}

func TestEveryRuleIsChecked(t *testing.T) {
	checked := marks(t)
	for _, id := range rules(t) {
		if len(checked[id]) == 0 {
			t.Errorf("rule %s has no test calling spec.Rule(t, %q)", id, id)
		}
	}
}

func TestEveryMarkNamesARule(t *testing.T) {
	known := rules(t)
	for id, where := range marks(t) {
		if !slices.Contains(known, id) {
			t.Errorf("%s: names rule %s, which SPEC.md does not list", strings.Join(where, ", "), id)
		}
	}
}

func TestSelected(t *testing.T) {
	cases := []struct {
		want string
		ids  []string
		ok   bool
	}{
		{"C04", []string{"C04"}, true},
		{"C", []string{"C10"}, true},
		{"C04, W", []string{"W02"}, true},
		{"C04", []string{"C05", "W01"}, false},
		{",", []string{"C04"}, false},
	}
	for _, c := range cases {
		if got := Selected(c.want, c.ids); got != c.ok {
			t.Errorf("Selected(%q, %v) = %v", c.want, c.ids, got)
		}
	}
}
