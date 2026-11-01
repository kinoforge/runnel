package server

import (
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/sqlite"
)

func TestOpenAPICoversRoutes(t *testing.T) {
	spec.Rule(t, "S10")
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := New(st, Config{AdminToken: "test-admin-token-0123456789"})
	var doc map[string]any
	if err := yaml.Unmarshal(openAPI, &doc); err != nil {
		t.Fatalf("openapi.yaml does not parse: %v", err)
	}
	paths, _ := doc["paths"].(map[string]any)
	var documented []string
	for path, item := range paths {
		ops, _ := item.(map[string]any)
		for method := range ops {
			if slices.Contains([]string{"get", "put", "post", "delete", "patch"}, method) {
				documented = append(documented, strings.ToUpper(method)+" "+path)
			}
		}
	}
	if len(documented) < 30 || len(s.patterns) < 30 {
		t.Fatalf("found %d documented and %d served routes", len(documented), len(s.patterns))
	}
	for _, p := range s.patterns {
		if !slices.Contains(documented, p) {
			t.Errorf("route %s is not in openapi.yaml", p)
		}
	}
	for _, p := range documented {
		if !slices.Contains(s.patterns, p) {
			t.Errorf("openapi.yaml documents %s, which the server does not serve", p)
		}
	}
	walkRefs(t, doc, doc)
}

func walkRefs(t *testing.T, root map[string]any, node any) {
	switch v := node.(type) {
	case map[string]any:
		for k, child := range v {
			if ref, ok := child.(string); ok && k == "$ref" && !resolves(root, ref) {
				t.Errorf("$ref %s points nowhere", ref)
			}
			walkRefs(t, root, child)
		}
	case []any:
		for _, child := range v {
			walkRefs(t, root, child)
		}
	}
}

func resolves(root map[string]any, ref string) bool {
	path, ok := strings.CutPrefix(ref, "#/")
	if !ok {
		return false
	}
	var node any = root
	for _, part := range strings.Split(path, "/") {
		m, ok := node.(map[string]any)
		if !ok {
			return false
		}
		if node, ok = m[part]; !ok {
			return false
		}
	}
	return true
}
