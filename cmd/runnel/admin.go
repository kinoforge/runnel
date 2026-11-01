package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

func namespaces(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("ns needs create, list or delete")
	}
	need, ok := map[string]int{"create": 1, "list": 0, "delete": 1}[args[0]]
	if !ok {
		return fmt.Errorf("unknown ns command %q", args[0])
	}
	c := newCmd("ns " + args[0])
	secret := c.fs.String("webhook-secret", "", "secret used to sign webhooks")
	pos, s, err := c.start(ctx, args[1:], need)
	if err != nil {
		return err
	}
	defer s.Close()
	if args[0] == "delete" {
		return runnel.DeleteNamespace(ctx, s, pos[0])
	}
	all, err := runnel.Namespaces(ctx, s)
	if err != nil {
		return err
	}
	if args[0] == "create" {
		n, ok := all[pos[0]]
		if !ok {
			n = runnel.Namespace{Name: pos[0]}
		}
		if *secret != "" {
			n.WebhookSecret = *secret
		}
		if err := runnel.PutNamespace(ctx, s, n); err != nil {
			return err
		}
		fmt.Println("namespace", pos[0], "ready")
		return nil
	}
	keys, err := runnel.Keys(ctx, s)
	if err != nil {
		return err
	}
	count := map[string]int{}
	for _, k := range keys {
		count[k.Namespace]++
	}
	tw := table()
	fmt.Fprintln(tw, "NAME\tKEYS\tCREATED")
	for _, n := range slices.Sorted(maps.Keys(all)) {
		fmt.Fprintf(tw, "%s\t%d\t%s\n", n, count[n], all[n].CreatedAt.Format(time.RFC3339))
	}
	return tw.Flush()
}

func keys(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("key needs create, list or delete")
	}
	c := newCmd("key " + args[0])
	roles := c.fs.String("roles", "producer", "comma separated roles: admin, producer, worker, reader")
	name := c.fs.String("name", "", "label for the key")
	expires := c.fs.Duration("expires", 0, "lifetime, for example 720h (default never)")
	need, ok := map[string]int{"create": 1, "list": 1, "delete": 2}[args[0]]
	if !ok {
		return fmt.Errorf("unknown key command %q", args[0])
	}
	pos, s, err := c.start(ctx, args[1:], need)
	if err != nil {
		return err
	}
	defer s.Close()
	all, err := runnel.Namespaces(ctx, s)
	if err != nil {
		return err
	}
	if _, ok := all[pos[0]]; !ok {
		return fmt.Errorf("namespace %q does not exist, run: runnel ns create %s", pos[0], pos[0])
	}
	switch args[0] {
	case "create":
		var exp time.Time
		if *expires > 0 {
			exp = time.Now().UTC().Add(*expires)
		}
		token, k, err := runnel.NewAPIKey(pos[0], *name, strings.Split(*roles, ","), exp)
		if err != nil {
			return err
		}
		if err := runnel.PutKey(ctx, s, k); err != nil {
			return err
		}
		fmt.Println(token)
		fmt.Fprintln(os.Stderr, "key", k.ID, "created; the token is shown only once")
	case "list":
		tw := table()
		fmt.Fprintln(tw, "ID\tNAME\tROLES\tEXPIRES")
		list, err := runnel.NamespaceKeys(ctx, s, pos[0])
		if err != nil {
			return err
		}
		for _, k := range list {
			exp := "never"
			if !k.ExpiresAt.IsZero() {
				exp = k.ExpiresAt.Format(time.RFC3339)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", k.ID, k.Name, strings.Join(k.Roles, ","), exp)
		}
		return tw.Flush()
	case "delete":
		if err := runnel.DeleteKey(ctx, s, pos[0], pos[1]); err != nil {
			return fmt.Errorf("key %q: %w", pos[1], err)
		}
	}
	return nil
}
