package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
)

const maxKeysPerNamespace = 100

type namespaceView struct {
	Name      string              `json:"name"`
	HasSecret bool                `json:"has_callback_secret"`
	Keys      []keyView           `json:"keys"`
	CreatedAt time.Time           `json:"created_at"`
	Stats     []runnel.QueueStats `json:"stats,omitempty"`
}

type keyView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	Roles     []string  `json:"roles"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

func viewNamespace(n runnel.Namespace, keys []runnel.APIKey) namespaceView {
	v := namespaceView{Name: n.Name, HasSecret: n.CallbackSecret != "", CreatedAt: n.CreatedAt, Keys: []keyView{}}
	for _, k := range keys {
		v.Keys = append(v.Keys, keyView{ID: k.ID, Name: k.Name, Roles: k.Roles, CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt})
	}
	return v
}

func keysByNamespace(all map[string]runnel.APIKey) map[string][]runnel.APIKey {
	out := map[string][]runnel.APIKey{}
	for _, k := range all {
		out[k.Namespace] = append(out[k.Namespace], k)
	}
	for _, list := range out {
		slices.SortFunc(list, func(a, b runnel.APIKey) int { return a.CreatedAt.Compare(b.CreatedAt) })
	}
	return out
}

func (s *Server) namespace(ctx context.Context, name string) (runnel.Namespace, error) {
	all, err := runnel.Namespaces(ctx, s.store)
	if err != nil {
		return runnel.Namespace{}, err
	}
	n, ok := all[name]
	if !ok {
		return runnel.Namespace{}, runnel.ErrNotFound
	}
	return n, nil
}

func (s *Server) listNamespaces(w http.ResponseWriter, r *http.Request, _ principal) error {
	all, err := runnel.Namespaces(r.Context(), s.store)
	if err != nil {
		return err
	}
	keys, err := runnel.Keys(r.Context(), s.store)
	if err != nil {
		return err
	}
	byNS := keysByNamespace(keys)
	out := make([]namespaceView, 0, len(all))
	for _, n := range all {
		out = append(out, viewNamespace(n, byNS[n.Name]))
	}
	slices.SortFunc(out, func(a, b namespaceView) int {
		if a.Name < b.Name {
			return -1
		}
		return 1
	})
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *Server) putNamespace(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		CallbackSecret *string `json:"callback_secret"`
	}
	if r.ContentLength != 0 {
		if err := decode(r, &body); err != nil {
			return err
		}
	}
	name := r.PathValue("name")
	n, created, err := s.upsertNamespace(r.Context(), name, body.CallbackSecret)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	keys, err := runnel.NamespaceKeys(r.Context(), s.store, name)
	if err != nil {
		return err
	}
	writeJSON(w, status, viewNamespace(n, keys))
	return nil
}

func (s *Server) upsertNamespace(ctx context.Context, name string, secret *string) (runnel.Namespace, bool, error) {
	n, err := s.namespace(ctx, name)
	created := false
	if errors.Is(err, runnel.ErrNotFound) {
		n, created = runnel.Namespace{Name: name}, true
	} else if err != nil {
		return n, false, err
	}
	if secret != nil {
		n.CallbackSecret = *secret
	}
	if err := runnel.PutNamespace(ctx, s.store, n); err != nil {
		return n, false, err
	}
	_ = s.reloadKeys(ctx)
	return n, created, nil
}

func (s *Server) getNamespace(w http.ResponseWriter, r *http.Request, _ principal) error {
	n, err := s.namespace(r.Context(), r.PathValue("name"))
	if err != nil {
		return err
	}
	keys, err := runnel.NamespaceKeys(r.Context(), s.store, n.Name)
	if err != nil {
		return err
	}
	v := viewNamespace(n, keys)
	if v.Stats, err = s.store.Stats(r.Context(), n.Name); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, v)
	return nil
}

func (s *Server) deleteNamespace(w http.ResponseWriter, r *http.Request, _ principal) error {
	if err := runnel.DeleteNamespace(r.Context(), s.store, r.PathValue("name")); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request, _ principal) error {
	var body struct {
		Name      string    `json:"name"`
		Roles     []string  `json:"roles"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	token, key, err := s.newKey(r.Context(), r.PathValue("name"), body.Name, body.Roles, body.ExpiresAt)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": key.ID, "key": token, "roles": key.Roles, "expires_at": key.ExpiresAt})
	return nil
}

func (s *Server) newKey(ctx context.Context, ns, name string, roles []string, expires time.Time) (string, runnel.APIKey, error) {
	if _, err := s.namespace(ctx, ns); err != nil {
		return "", runnel.APIKey{}, err
	}
	existing, err := runnel.NamespaceKeys(ctx, s.store, ns)
	if err != nil {
		return "", runnel.APIKey{}, err
	}
	if len(existing) >= maxKeysPerNamespace {
		return "", runnel.APIKey{}, fmt.Errorf("%w: at most %d keys per namespace", runnel.ErrInvalid, maxKeysPerNamespace)
	}
	token, key, err := runnel.NewAPIKey(ns, name, roles, expires)
	if err != nil {
		return "", runnel.APIKey{}, err
	}
	if err := runnel.PutKey(ctx, s.store, key); err != nil {
		return "", runnel.APIKey{}, err
	}
	_ = s.reloadKeys(ctx)
	return token, key, nil
}

func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request, _ principal) error {
	if err := runnel.DeleteKey(r.Context(), s.store, r.PathValue("name"), r.PathValue("id")); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	w.WriteHeader(http.StatusNoContent)
	return nil
}
