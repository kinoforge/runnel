package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

type keyEntry struct {
	ns  string
	key runnel.APIKey
}

type principal struct {
	admin bool
	ns    string
	key   runnel.APIKey
}

func (p principal) id() string {
	if p.admin {
		return "admin"
	}
	return p.key.ID
}

func (p principal) can(ns, role string) bool {
	if p.admin {
		return true
	}
	return p.ns == ns && p.key.Has(role)
}

func (s *Server) authenticate(r *http.Request) (principal, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" || len(token) > 512 {
		return principal{}, false
	}
	if s.hasRoot {
		sum := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(sum[:], s.admin[:]) == 1 {
			return principal{admin: true}, true
		}
	}
	id, secret, ok := runnel.ParseAPIKey(token)
	if !ok {
		return principal{}, false
	}
	e, ok := s.lookupKey(r.Context(), id)
	if !ok || !e.key.Matches(secret, time.Now()) {
		return principal{}, false
	}
	return principal{ns: e.ns, key: e.key}, true
}

func (s *Server) lookupKey(ctx context.Context, id string) (keyEntry, bool) {
	s.mu.Lock()
	e, ok := s.keys[id]
	stale := time.Since(s.loaded) > 5*time.Second || (!ok && time.Since(s.loaded) > time.Second)
	s.mu.Unlock()
	if !stale {
		return e, ok
	}
	if err := s.reloadKeys(ctx); err != nil {
		s.log.Error("runnel: loading keys failed", "err", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok = s.keys[id]
	return e, ok
}

func (s *Server) reloadKeys(ctx context.Context) error {
	all, err := runnel.Namespaces(ctx, s.store)
	if err != nil {
		return err
	}
	stored, err := runnel.Keys(ctx, s.store)
	if err != nil {
		return err
	}
	keys := map[string]keyEntry{}
	for id, k := range stored {
		if _, ok := all[k.Namespace]; ok {
			keys[id] = keyEntry{ns: k.Namespace, key: k}
		}
	}
	s.mu.Lock()
	s.keys, s.loaded = keys, time.Now()
	s.mu.Unlock()
	return nil
}

func (s *Server) namespaceNames(ctx context.Context) ([]string, error) {
	all, err := runnel.Namespaces(ctx, s.store)
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(all)), nil
}

var workerPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,100}$`)

func workerID(p principal, raw string) (string, error) {
	if !workerPattern.MatchString(raw) {
		return "", fmt.Errorf("%w: worker must match %s", runnel.ErrInvalid, workerPattern)
	}
	return p.id() + ":" + raw, nil
}
