package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/kinoforge/runnel"
)

func configCommand[T interface{ Validate() error }](ctx context.Context, name string, kind runnel.ConfigKind, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s needs set, list or delete", name)
	}
	c := newCmd(name + " " + args[0])
	raw := c.fs.String("json", "", "JSON body")
	need, ok := map[string]int{"set": 2, "list": 1, "delete": 2}[args[0]]
	if !ok {
		return fmt.Errorf("unknown %s command %q", name, args[0])
	}
	pos, s, err := c.start(ctx, args[1:], need)
	if err != nil {
		return err
	}
	defer s.Close()
	switch args[0] {
	case "set":
		var v T
		body := map[string]any{}
		if *raw != "" {
			if err := json.Unmarshal([]byte(*raw), &body); err != nil {
				return fmt.Errorf("--json: %w", err)
			}
		}
		body["name"] = pos[1]
		b, _ := json.Marshal(body)
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf("--json: %w", err)
		}
		if err := v.Validate(); err != nil {
			return err
		}
		if err := runnel.PutTyped(ctx, s, pos[0], kind, pos[1], v); err != nil {
			return err
		}
		return printJSON(v)
	case "list":
		all, err := runnel.LoadTyped[T](ctx, s, pos[0], kind)
		if err != nil {
			return err
		}
		return printJSON(all)
	case "delete":
		return s.DeleteConfig(ctx, pos[0], kind, pos[1])
	}
	return nil
}
