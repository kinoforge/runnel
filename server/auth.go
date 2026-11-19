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

const (
	maxTokenLen  = 512
	keyMissRetry = time.Second
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
	if s.cfg.NoAuth || s.certAdmin(r) {
		return principal{admin: true}, true
	}
	if user, pass, ok := r.BasicAuth(); ok {
		if user == s.cfg.AdminUser {
			return s.adminSecret(pass)
		}
		return s.apiKey(r.Context(), runnel.APIKeyToken(user, pass))
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return principal{}, false
	}
	if p, ok := s.adminSecret(token); ok {
		return p, true
	}
	return s.apiKey(r.Context(), token)
}

func (s *Server) adminSecret(secret string) (principal, bool) {
	if !s.hasRoot || secret == "" || len(secret) > maxTokenLen {
		return principal{}, false
	}
	sum := sha256.Sum256([]byte(secret))
	if subtle.ConstantTimeCompare(sum[:], s.admin[:]) != 1 {
		return principal{}, false
	}
	return principal{admin: true}, true
}

func (s *Server) apiKey(ctx context.Context, token string) (principal, bool) {
	id, secret, ok := runnel.ParseAPIKey(token)
	if !ok {
		return principal{}, false
	}
	e, ok := s.lookupKey(ctx, id)
	if !ok || !e.key.Matches(secret, time.Now()) {
		return principal{}, false
	}
	return principal{ns: e.ns, key: e.key}, true
}

func (s *Server) certAdmin(r *http.Request) bool {
	if !s.cfg.CertAdmin || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return false
	}
	if len(s.cfg.CertNames) == 0 {
		return true
	}
	leaf := r.TLS.VerifiedChains[0][0]
	named := func(n string) bool { return slices.Contains(s.cfg.CertNames, n) }
	return named(leaf.Subject.CommonName) || slices.ContainsFunc(leaf.DNSNames, named)
}

func (s *Server) lookupKey(ctx context.Context, id string) (keyEntry, bool) {
	s.mu.Lock()
	e, ok := s.keys[id]
	stale := time.Since(s.loaded) > s.cfg.KeyCacheTTL || (!ok && time.Since(s.loaded) > keyMissRetry)
	s.mu.Unlock()
	if !stale {
		return e, ok
	}
	if err := s.reloadKeys(ctx); err != nil && ctx.Err() == nil {
		s.log.Error("runnel: loading keys failed", "err", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok = s.keys[id]
	return e, ok
}

func (s *Server) reloadKeys(ctx context.Context) error {
	asked := time.Now()
	s.reload.Lock()
	defer s.reload.Unlock()
	s.mu.Lock()
	fresh := s.loaded.After(asked)
	s.mu.Unlock()
	if fresh {
		return nil
	}
	began := time.Now()
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
	s.keys, s.loaded = keys, began
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
