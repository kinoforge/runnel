package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/kinoforge/runnel"
	pb "github.com/kinoforge/runnel/gen/runnel/v1"
)

type setting[T any, P proto.Message] struct {
	s     *Server
	kind  runnel.ConfigKind
	toPB  func(T) P
	from  func(P) T
	check func(T) (string, error)
}

func (st setting[T, P]) list(ctx context.Context, ns string) ([]P, error) {
	all, err := runnel.LoadTyped[T](ctx, st.s.store, ns, st.kind)
	if err != nil {
		return nil, err
	}
	out := make([]P, 0, len(all))
	for _, name := range slices.Sorted(maps.Keys(all)) {
		out = append(out, st.toPB(all[name]))
	}
	return out, nil
}

func (st setting[T, P]) get(ctx context.Context, ns, name string) (P, string, error) {
	var zero P
	all, err := runnel.LoadTyped[T](ctx, st.s.store, ns, st.kind)
	if err != nil {
		return zero, "", err
	}
	v, ok := all[name]
	if !ok {
		return zero, "", runnel.ErrNotFound
	}
	tag, err := etag(v)
	return st.toPB(v), tag, err
}

func rpcConditions(p *pb.Precondition) preconditions {
	var c preconditions
	if p.GetIfMatch() != "" {
		c.hasMatch, c.match = true, []string{p.GetIfMatch()}
	}
	if p.GetIfNoneMatch() {
		c.hasNoneMatch, c.noneMatch = true, []string{"*"}
	}
	return c
}

func (st setting[T, P]) put(ctx context.Context, ns string, in P, pre *pb.Precondition) (P, string, error) {
	var zero P
	v := st.from(in)
	name, err := st.check(v)
	if err != nil {
		return zero, "", err
	}
	cond := rpcConditions(pre)
	if !cond.set() {
		if err := runnel.PutTyped(ctx, st.s.store, ns, st.kind, name, v); err != nil {
			return zero, "", err
		}
	} else if _, err := runnel.UpdateTyped(ctx, st.s.store, ns, st.kind, name, func(cur *T, found bool) error {
		if err := cond.check(*cur, found); err != nil {
			return err
		}
		*cur = v
		return nil
	}); err != nil {
		return zero, "", err
	}
	st.s.client(ns).Refresh()
	tag, err := etag(v)
	return st.toPB(v), tag, err
}

func (st setting[T, P]) patch(ctx context.Context, ns, name string, in P, mask *fieldmaskpb.FieldMask, pre *pb.Precondition) (P, string, error) {
	var zero P
	if len(mask.GetPaths()) == 0 {
		return zero, "", fmt.Errorf("%w: update_mask names the fields to change", runnel.ErrInvalid)
	}
	fields := in.ProtoReflect().Descriptor().Fields()
	for _, path := range mask.GetPaths() {
		if path == "name" || fields.ByName(protoreflect.Name(path)) == nil {
			return zero, "", fmt.Errorf("%w: update_mask: %q is not a field that can change", runnel.ErrInvalid, path)
		}
	}
	cond := rpcConditions(pre)
	v, err := runnel.UpdateTyped(ctx, st.s.store, ns, st.kind, name, func(cur *T, found bool) error {
		if err := cond.check(*cur, found); err != nil {
			return err
		}
		next := st.toPB(*cur).ProtoReflect()
		src := in.ProtoReflect()
		for _, path := range mask.GetPaths() {
			fd := fields.ByName(protoreflect.Name(path))
			if src.Has(fd) {
				next.Set(fd, src.Get(fd))
			} else {
				next.Clear(fd)
			}
		}
		next.Set(fields.ByName("name"), protoreflect.ValueOfString(name))
		out, ok := next.Interface().(P)
		if !ok {
			return errors.New("runnel: setting type mismatch")
		}
		nv := st.from(out)
		got, err := st.check(nv)
		if err != nil {
			return err
		}
		if got != name {
			return fmt.Errorf("%w: name must not change", runnel.ErrInvalid)
		}
		*cur = nv
		return nil
	})
	if err != nil {
		return zero, "", err
	}
	st.s.client(ns).Refresh()
	tag, err := etag(v)
	return st.toPB(v), tag, err
}

func (st setting[T, P]) remove(ctx context.Context, ns, name string) error {
	if err := st.s.store.DeleteConfig(ctx, ns, st.kind, name); err != nil {
		return err
	}
	st.s.client(ns).Refresh()
	return nil
}
