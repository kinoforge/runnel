package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/kinoforge/runnel"
)

func configRoutes[T any](s *Server, path string, kind runnel.ConfigKind, check func(T) (string, error)) {
	base := "/v1/ns/{ns}/" + path
	s.handle("GET "+base, runnel.RoleReader, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		all, err := runnel.LoadTyped[T](r.Context(), s.store, r.PathValue("ns"), kind)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": all})
		return nil
	})
	s.handle("GET "+base+"/{name}", runnel.RoleReader, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		all, err := runnel.LoadTyped[T](r.Context(), s.store, r.PathValue("ns"), kind)
		if err != nil {
			return err
		}
		v, ok := all[r.PathValue("name")]
		if !ok {
			return runnel.ErrNotFound
		}
		return writeConfig(w, v)
	})
	s.handle("PUT "+base+"/{name}", runnel.RoleAdmin, putConfig(s, kind, check))
	s.handle("PATCH "+base+"/{name}", runnel.RoleAdmin, patchConfig(s, kind, check))
	s.handle("DELETE "+base+"/{name}", runnel.RoleAdmin, func(w http.ResponseWriter, r *http.Request, _ principal) error {
		if err := s.store.DeleteConfig(r.Context(), r.PathValue("ns"), kind, r.PathValue("name")); err != nil {
			return err
		}
		s.client(r.PathValue("ns")).Refresh()
		w.WriteHeader(http.StatusNoContent)
		return nil
	})
}

func putConfig[T any](s *Server, kind runnel.ConfigKind, check func(T) (string, error)) handler {
	return func(w http.ResponseWriter, r *http.Request, _ principal) error {
		var v T
		if err := decode(r, &v); err != nil {
			return err
		}
		ns, name := r.PathValue("ns"), r.PathValue("name")
		if err := named(check, v, name); err != nil {
			return err
		}
		cond := conditions(r)
		if !cond.set() {
			if err := runnel.PutTyped(r.Context(), s.store, ns, kind, name, v); err != nil {
				return err
			}
		} else if _, err := runnel.UpdateTyped(r.Context(), s.store, ns, kind, name, func(cur *T, found bool) error {
			if err := cond.check(*cur, found); err != nil {
				return err
			}
			*cur = v
			return nil
		}); err != nil {
			return err
		}
		s.client(ns).Refresh()
		return writeConfig(w, v)
	}
}

func patchConfig[T any](s *Server, kind runnel.ConfigKind, check func(T) (string, error)) handler {
	return func(w http.ResponseWriter, r *http.Request, _ principal) error {
		patch, err := readPatch(r)
		if err != nil {
			return err
		}
		ns, name := r.PathValue("ns"), r.PathValue("name")
		cond := conditions(r)
		v, err := runnel.UpdateTyped(r.Context(), s.store, ns, kind, name, func(cur *T, found bool) error {
			if err := cond.check(*cur, found); err != nil {
				return err
			}
			next, err := applyPatch(*cur, found, patch, name)
			if err != nil {
				return err
			}
			if err := named(check, next, name); err != nil {
				return err
			}
			*cur = next
			return nil
		})
		if err != nil {
			return err
		}
		s.client(ns).Refresh()
		return writeConfig(w, v)
	}
}

func named[T any](check func(T) (string, error), v T, want string) error {
	name, err := check(v)
	if err != nil {
		return err
	}
	if name != want {
		return fmt.Errorf("%w: name in body must match the path", runnel.ErrInvalid)
	}
	return nil
}

func writeConfig(w http.ResponseWriter, v any) error {
	tag, err := etag(v)
	if err != nil {
		return err
	}
	w.Header().Set("ETag", tag)
	writeJSON(w, http.StatusOK, v)
	return nil
}

func etag(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return `"` + base64.RawURLEncoding.EncodeToString(sum[:16]) + `"`, nil
}

type preconditions struct {
	match, noneMatch []string
}

func conditions(r *http.Request) preconditions {
	return preconditions{match: tags(r.Header.Values("If-Match")), noneMatch: tags(r.Header.Values("If-None-Match"))}
}

func tags(values []string) []string {
	var out []string
	for _, v := range values {
		for t := range strings.SplitSeq(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				out = append(out, t)
			}
		}
	}
	return out
}

func (p preconditions) set() bool { return p.match != nil || p.noneMatch != nil }

func (p preconditions) check(cur any, found bool) error {
	if !p.set() {
		return nil
	}
	tag := ""
	if found {
		t, err := etag(cur)
		if err != nil {
			return err
		}
		tag = t
	}
	if p.match != nil && (!found || !matches(p.match, tag, false)) {
		return runnel.ErrStale
	}
	if p.noneMatch != nil && found && matches(p.noneMatch, tag, true) {
		return runnel.ErrStale
	}
	return nil
}

func matches(list []string, tag string, weak bool) bool {
	for _, t := range list {
		if weak {
			t = strings.TrimPrefix(t, "W/")
		}
		if t == "*" || t == tag {
			return true
		}
	}
	return false
}
