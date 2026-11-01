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

const (
	maxKeysPerNamespace = 100
)

type namespaceView struct {
	Name      string              `json:"name"`
	HasSecret bool                `json:"has_webhook_secret"`
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
	v := namespaceView{Name: n.Name, HasSecret: n.WebhookSecret != "", CreatedAt: n.CreatedAt, Keys: []keyView{}}
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
		WebhookSecret *string `json:"webhook_secret"`
	}
	if r.ContentLength != 0 {
		if err := decode(r, &body); err != nil {
			return err
		}
	}
	name := r.PathValue("name")
	n, err := s.namespace(r.Context(), name)
	status := http.StatusOK
	if errors.Is(err, runnel.ErrNotFound) {
		n, status = runnel.Namespace{Name: name}, http.StatusCreated
	} else if err != nil {
		return err
	}
	if body.WebhookSecret != nil {
		n.WebhookSecret = *body.WebhookSecret
	}
	if err := runnel.PutNamespace(r.Context(), s.store, n); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	keys, err := runnel.NamespaceKeys(r.Context(), s.store, name)
	if err != nil {
		return err
	}
	writeJSON(w, status, viewNamespace(n, keys))
	return nil
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
	ns := r.PathValue("name")
	if _, err := s.namespace(r.Context(), ns); err != nil {
		return err
	}
	existing, err := runnel.NamespaceKeys(r.Context(), s.store, ns)
	if err != nil {
		return err
	}
	if len(existing) >= maxKeysPerNamespace {
		return fmt.Errorf("%w: at most %d keys per namespace", runnel.ErrInvalid, maxKeysPerNamespace)
	}
	token, key, err := runnel.NewAPIKey(ns, body.Name, body.Roles, body.ExpiresAt)
	if err != nil {
		return err
	}
	if err := runnel.PutKey(r.Context(), s.store, key); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	writeJSON(w, http.StatusCreated, map[string]any{"id": key.ID, "key": token, "roles": key.Roles, "expires_at": key.ExpiresAt})
	return nil
}

func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request, _ principal) error {
	if err := runnel.DeleteKey(r.Context(), s.store, r.PathValue("name"), r.PathValue("id")); err != nil {
		return err
	}
	_ = s.reloadKeys(r.Context())
	w.WriteHeader(http.StatusNoContent)
	return nil
}
