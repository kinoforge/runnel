package docs_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/config"
)

const root = "../.."

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func keys(t reflect.Type, prefix string) []string {
	var out []string
	for f := range t.Fields() {
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			continue
		}
		path := prefix + name
		ft := f.Type
		if ft.Kind() == reflect.Struct && ft != reflect.TypeFor[time.Duration]() && ft != reflect.TypeFor[time.Time]() {
			out = append(out, keys(ft, path+".")...)
			continue
		}
		out = append(out, path)
	}
	return out
}

func TestEverySettingIsDocumented(t *testing.T) {
	doc := read(t, "docs/configuration.md")
	all := keys(reflect.TypeFor[config.Config](), "")
	if len(all) < 50 {
		t.Fatalf("found only %d settings; the walk over config.Config is broken", len(all))
	}
	for _, k := range all {
		if !strings.Contains(doc, "`"+k+"`") {
			t.Errorf("setting %s is missing from docs/configuration.md", k)
		}
	}
}

func TestEveryEndpointIsDocumented(t *testing.T) {
	doc := read(t, "docs/http-api.md")
	paths := regexp.MustCompile(`(?m)^  (/\S+):$`).FindAllStringSubmatch(read(t, "server/openapi.yaml"), -1)
	if len(paths) < 20 {
		t.Fatalf("found only %d paths in openapi.yaml", len(paths))
	}
	for _, p := range paths {
		if !strings.Contains(doc, p[1]+"`") && !strings.Contains(doc, p[1]+",") {
			t.Errorf("endpoint %s is missing from docs/http-api.md", p[1])
		}
	}
}

func TestEveryRPCIsDocumented(t *testing.T) {
	doc := read(t, "docs/grpc-api.md")
	protos, err := filepath.Glob(filepath.Join(root, "proto/runnel/v1/*.proto"))
	if err != nil {
		t.Fatal(err)
	}
	rpc := regexp.MustCompile(`(?m)^\s*rpc (\w+)\(`)
	var n int
	for _, p := range protos {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range rpc.FindAllStringSubmatch(string(b), -1) {
			n++
			if !strings.Contains(doc, "`"+m[1]+"`") {
				t.Errorf("rpc %s is missing from docs/grpc-api.md", m[1])
			}
		}
	}
	if n < 30 {
		t.Fatalf("found only %d rpcs in proto/runnel/v1", n)
	}
}

func TestLocalLinksResolve(t *testing.T) {
	link := regexp.MustCompile(`\]\(([^)\s]+)\)`)
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "vendor" || d.Name() == "testdata") {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range link.FindAllStringSubmatch(string(b), -1) {
			target, _, _ := strings.Cut(m[1], "#")
			if target == "" || strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(f), target)); err != nil {
				t.Errorf("%s links to %s, which does not exist", strings.TrimPrefix(f, root+"/"), m[1])
			}
		}
	}
}
