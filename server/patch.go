package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"

	"github.com/kinoforge/runnel"
)

var errMediaType = errors.New("PATCH takes application/merge-patch+json")

func readPatch(r *http.Request) (any, error) {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (mt != "application/merge-patch+json" && mt != "application/json") {
		return nil, errMediaType
	}
	var raw json.RawMessage
	if err := decode(r, &raw); err != nil {
		return nil, err
	}
	return numbered(raw)
}

func numbered(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: body: %w", runnel.ErrInvalid, err)
	}
	return v, nil
}

func applyPatch[T any](cur T, found bool, patch any, name string) (T, error) {
	var next T
	doc := any(map[string]any{})
	if found {
		b, err := json.Marshal(cur)
		if err != nil {
			return next, err
		}
		if doc, err = numbered(b); err != nil {
			return next, err
		}
	}
	merged := mergePatch(doc, patch)
	if m, ok := merged.(map[string]any); ok {
		if _, has := m["name"]; !has {
			m["name"] = name
		}
	}
	b, err := json.Marshal(merged)
	if err != nil {
		return next, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&next); err != nil {
		return next, fmt.Errorf("%w: patch: %w", runnel.ErrInvalid, err)
	}
	return next, nil
}

func mergePatch(target, patch any) any {
	p, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	t, ok := target.(map[string]any)
	if !ok {
		t = map[string]any{}
	}
	for k, v := range p {
		if v == nil {
			delete(t, k)
			continue
		}
		t[k] = mergePatch(t[k], v)
	}
	return t
}
