package server

import (
	"context"
	"maps"
	"slices"

	"connectrpc.com/connect"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

type adminRPC struct{ s *Server }

func pbNamespace(n runnel.Namespace) *pb.Namespace {
	return &pb.Namespace{Name: n.Name, HasCallbackSecret: n.CallbackSecret != "", CreatedAt: ts(n.CreatedAt)}
}

func pbKey(k runnel.APIKey) *pb.Key {
	return &pb.Key{Id: k.ID, Namespace: k.Namespace, Name: k.Name, Roles: k.Roles, CreatedAt: ts(k.CreatedAt), ExpiresAt: ts(k.ExpiresAt)}
}

func (h *adminRPC) ListNamespaces(ctx context.Context, _ *connect.Request[pb.ListNamespacesRequest]) (*connect.Response[pb.ListNamespacesResponse], error) {
	if _, err := h.s.allow(ctx, "", ""); err != nil {
		return nil, err
	}
	all, err := runnel.Namespaces(ctx, h.s.store)
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	out := &pb.ListNamespacesResponse{}
	for _, name := range slices.Sorted(maps.Keys(all)) {
		out.Namespaces = append(out.Namespaces, pbNamespace(all[name]))
	}
	return connect.NewResponse(out), nil
}

func (h *adminRPC) PutNamespace(ctx context.Context, req *connect.Request[pb.PutNamespaceRequest]) (*connect.Response[pb.PutNamespaceResponse], error) {
	if _, err := h.s.allow(ctx, "", ""); err != nil {
		return nil, err
	}
	n, created, err := h.s.upsertNamespace(ctx, req.Msg.GetName(), req.Msg.CallbackSecret)
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.PutNamespaceResponse{Namespace: pbNamespace(n), Created: created}), nil
}

func (h *adminRPC) DeleteNamespace(ctx context.Context, req *connect.Request[pb.DeleteNamespaceRequest]) (*connect.Response[pb.DeleteNamespaceResponse], error) {
	if _, err := h.s.allow(ctx, "", ""); err != nil {
		return nil, err
	}
	if err := runnel.DeleteNamespace(ctx, h.s.store, req.Msg.GetName()); err != nil {
		return nil, h.s.rpcErr(err)
	}
	_ = h.s.reloadKeys(ctx)
	return connect.NewResponse(&pb.DeleteNamespaceResponse{}), nil
}

func (h *adminRPC) CreateKey(ctx context.Context, req *connect.Request[pb.CreateKeyRequest]) (*connect.Response[pb.CreateKeyResponse], error) {
	if _, err := h.s.allow(ctx, "", ""); err != nil {
		return nil, err
	}
	m := req.Msg
	token, key, err := h.s.newKey(ctx, m.GetNamespace(), m.GetName(), m.GetRoles(), fromTS(m.GetExpiresAt()))
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	return connect.NewResponse(&pb.CreateKeyResponse{Key: pbKey(key), Token: token}), nil
}

func (h *adminRPC) ListKeys(ctx context.Context, req *connect.Request[pb.ListKeysRequest]) (*connect.Response[pb.ListKeysResponse], error) {
	if _, err := h.s.allow(ctx, "", ""); err != nil {
		return nil, err
	}
	keys, err := runnel.NamespaceKeys(ctx, h.s.store, req.Msg.GetNamespace())
	if err != nil {
		return nil, h.s.rpcErr(err)
	}
	out := &pb.ListKeysResponse{}
	for _, k := range keys {
		out.Keys = append(out.Keys, pbKey(k))
	}
	return connect.NewResponse(out), nil
}

func (h *adminRPC) DeleteKey(ctx context.Context, req *connect.Request[pb.DeleteKeyRequest]) (*connect.Response[pb.DeleteKeyResponse], error) {
	if _, err := h.s.allow(ctx, "", ""); err != nil {
		return nil, err
	}
	if err := runnel.DeleteKey(ctx, h.s.store, req.Msg.GetNamespace(), req.Msg.GetId()); err != nil {
		return nil, h.s.rpcErr(err)
	}
	_ = h.s.reloadKeys(ctx)
	return connect.NewResponse(&pb.DeleteKeyResponse{}), nil
}
