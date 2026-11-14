package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

const statusClientClosed = 499

type problemBody struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func (s *Server) problem(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemBody{Type: "about:blank", Title: title, Status: status, Detail: detail})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		s.problem(w, http.StatusRequestEntityTooLarge, "too large", "request body too large")
	case errors.Is(err, runnel.ErrInvalid):
		s.problem(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, runnel.ErrNotFound):
		s.problem(w, http.StatusNotFound, "not found", "")
	case errors.Is(err, runnel.ErrStale):
		s.problem(w, http.StatusPreconditionFailed, "precondition failed", err.Error())
	case errors.Is(err, errMediaType):
		s.problem(w, http.StatusUnsupportedMediaType, "unsupported media type", err.Error())
	case errors.Is(err, runnel.ErrGroupFull):
		w.Header().Set("Retry-After", s.retryAfter())
		s.problem(w, http.StatusTooManyRequests, "group full", err.Error())
	case errors.Is(err, runnel.ErrQueueFull):
		w.Header().Set("Retry-After", s.retryAfter())
		s.problem(w, http.StatusTooManyRequests, "queue full", err.Error())
	case errors.Is(err, runnel.ErrDependency):
		s.problem(w, http.StatusUnprocessableEntity, "dependency", err.Error())
	case errors.Is(err, runnel.ErrConflict), errors.Is(err, runnel.ErrFinal), errors.Is(err, runnel.ErrNotFinal):
		s.problem(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, context.Canceled):
		s.problem(w, statusClientClosed, "cancelled", "")
	default:
		s.log.Error("runnel: request failed", "err", err)
		s.problem(w, http.StatusInternalServerError, "internal error", "")
	}
}

func (s *Server) retryAfter() string {
	return strconv.Itoa(int(max(1, s.cfg.RetryAfter.Round(time.Second)/time.Second)))
}

func (s *Server) preferWait(r *http.Request) (time.Duration, bool) {
	for _, h := range r.Header.Values("Prefer") {
		for _, part := range strings.FieldsFunc(h, func(c rune) bool { return c == ',' || c == ';' }) {
			v, ok := strings.CutPrefix(strings.TrimSpace(part), "wait=")
			if !ok {
				continue
			}
			n, err := strconv.Atoi(strings.Trim(v, `"`))
			if err != nil || n <= 0 {
				return 0, true
			}
			if most := int(s.cfg.MaxResultWait / time.Second); n > most {
				return s.cfg.MaxResultWait, true
			}
			return time.Duration(n) * time.Second, true
		}
	}
	return 0, false
}

func (s *Server) enqueueWait(r *http.Request, ns, queue string) time.Duration {
	if wait, ok := s.preferWait(r); ok {
		return wait
	}
	cfg, err := s.client(ns).Queue(r.Context(), queue)
	if err != nil {
		return 0
	}
	return s.capWait(cfg.Wait.D())
}

func (s *Server) capWait(d time.Duration) time.Duration { return min(d, s.cfg.MaxResultWait) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return err
		}
		return fmt.Errorf("%w: body: %w", runnel.ErrInvalid, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: body must hold one JSON value", runnel.ErrInvalid)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data after body", runnel.ErrInvalid)
	}
	return nil
}

func (s *Server) withCallback(u string, validate func() error) error {
	if u != "" {
		if err := s.checkCallback(u); err != nil {
			return err
		}
	}
	return validate()
}

func (s *Server) checkCallback(u string) error {
	if err := runnel.ValidCallbackURL(u, s.cfg.Limits.MaxCallbackURL); err != nil {
		return err
	}
	if !s.cfg.AllowHTTPCallbacks && !strings.HasPrefix(u, "https://") {
		return fmt.Errorf("%w: callback must use https", runnel.ErrInvalid)
	}
	return nil
}
