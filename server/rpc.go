package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/gen/runnel/v1/runnelv1connect"
)

type principalKey struct{}

type rpcCaller struct {
	p  principal
	ok bool
}

func (s *Server) mountRPC() {
	opts := []connect.HandlerOption{connect.WithReadMaxBytes(int(s.cfg.MaxBody))}
	mounts := []func() (string, http.Handler){
		func() (string, http.Handler) { return runnelv1connect.NewJobServiceHandler(&jobRPC{s}, opts...) },
		func() (string, http.Handler) { return runnelv1connect.NewWorkerServiceHandler(&workerRPC{s}, opts...) },
		func() (string, http.Handler) { return runnelv1connect.NewConfigServiceHandler(&configRPC{s}, opts...) },
		func() (string, http.Handler) { return runnelv1connect.NewAdminServiceHandler(&adminRPC{s}, opts...) },
	}
	for _, m := range mounts {
		path, h := m()
		s.mux.Handle(path, s.withCaller(h))
	}
}

func (s *Server) withCaller(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := s.authenticate(r)
		if r.URL.Path == runnelv1connect.WorkerServiceWorkProcedure {
			rc := http.NewResponseController(w)
			_ = rc.SetReadDeadline(time.Time{})
			_ = rc.SetWriteDeadline(time.Time{})
		}
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, rpcCaller{p: p, ok: ok})))
	})
}

func (s *Server) allow(ctx context.Context, ns, role string) (principal, error) {
	c, _ := ctx.Value(principalKey{}).(rpcCaller)
	if !c.ok {
		return principal{}, connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid credentials"))
	}
	if role == "" {
		if !c.p.admin {
			return principal{}, connect.NewError(connect.CodePermissionDenied, errors.New("admin token required"))
		}
		return c.p, nil
	}
	if !runnel.ValidNamespace(ns) {
		return principal{}, connect.NewError(connect.CodeNotFound, errors.New("unknown namespace"))
	}
	if !c.p.can(ns, role) {
		return principal{}, connect.NewError(connect.CodePermissionDenied, errors.New("this key lacks the "+role+" role for namespace "+ns))
	}
	return c.p, nil
}

var rpcErrors = []struct {
	err  error
	code connect.Code
	name string
}{
	{runnel.ErrInvalid, connect.CodeInvalidArgument, "invalid"},
	{runnel.ErrNotFound, connect.CodeNotFound, "not_found"},
	{runnel.ErrStale, connect.CodeAborted, "stale"},
	{runnel.ErrQueueFull, connect.CodeResourceExhausted, "queue_full"},
	{runnel.ErrGroupFull, connect.CodeResourceExhausted, "group_full"},
	{runnel.ErrNoSpace, connect.CodeResourceExhausted, "no_space"},
	{runnel.ErrDependency, connect.CodeFailedPrecondition, "dependency"},
	{runnel.ErrConflict, connect.CodeFailedPrecondition, "conflict"},
	{runnel.ErrFinal, connect.CodeFailedPrecondition, "final"},
	{runnel.ErrNotFinal, connect.CodeFailedPrecondition, "not_final"},
}

func (s *Server) rpcErr(err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	for _, e := range rpcErrors {
		if errors.Is(err, e.err) {
			out := connect.NewError(e.code, err)
			out.Meta().Set("Runnel-Error", e.name)
			if e.code == connect.CodeResourceExhausted {
				out.Meta().Set("Retry-After", s.retryAfter())
			}
			return out
		}
	}
	s.log.Error("runnel: rpc failed", "err", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}
