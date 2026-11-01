package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/stores"
)

const usage = `runnel: queues for every service

usage:
  runnel serve      [--addr :8700] [--no-maintain] [--archive] [--no-webhooks]
  runnel migrate
  runnel ns create NAME [--webhook-secret S] | ns list | ns delete NAME
  runnel key create NS --roles producer,worker [--name N] [--expires 720h] | key list NS | key delete NS ID
  runnel enqueue NS QUEUE KIND [--payload JSON] [--priority N] [--delay 10s] [--tenant T] [--pool P]
  runnel jobs NS [--queue Q] [--state S] [--limit N]
  runnel job NS ID | cancel NS ID | retry NS ID | stats NS
  runnel queue|pool|schedule set NS NAME --json '{...}' | list NS | delete NS NAME
  runnel version
  runnel bench [--jobs 20000] [--workers 4] [--concurrency 25] [--batch 100] [--tenants N] [--tenant-slots N]

every command reads the store from --store or RUNNEL_STORE, for example
  postgres://user:pass@host/db  mysql://user:pass@host/db  sqlite:runnel.db  redis://host:6379/0
`

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
		fmt.Print(usage)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "runnel:", err)
		os.Exit(1)
	}
}

var version = ""

func buildVersion() string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

type cmd struct {
	fs    *flag.FlagSet
	store *string
}

func newCmd(name string) *cmd {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return &cmd{fs: fs, store: fs.String("store", env("RUNNEL_STORE", "sqlite:runnel.db"), "store url")}
}

func (c *cmd) parse(args []string, positional int) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := c.fs.Parse(args); err != nil {
			return nil, err
		}
		args = c.fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	if len(pos) < positional {
		return nil, fmt.Errorf("%s needs %d argument(s)\n\n%s", c.fs.Name(), positional, usage)
	}
	return pos, nil
}

func (c *cmd) open(ctx context.Context) (runnel.Store, error) { return stores.Open(ctx, *c.store) }

func run(ctx context.Context, name string, args []string) error {
	switch name {
	case "serve":
		return serve(ctx, args)
	case "migrate":
		c := newCmd(name)
		if _, err := c.parse(args, 0); err != nil {
			return err
		}
		s, err := c.open(ctx)
		if err != nil {
			return err
		}
		defer s.Close()
		fmt.Println("store ready")
		return nil
	case "ns":
		return namespaces(ctx, args)
	case "key":
		return keys(ctx, args)
	case "enqueue":
		return enqueue(ctx, args)
	case "jobs":
		return listJobs(ctx, args)
	case "job", "cancel", "retry":
		return jobCommand(ctx, name, args)
	case "stats":
		return stats(ctx, args)
	case "bench":
		return bench(ctx, args)
	case "version":
		fmt.Println("runnel", buildVersion())
		return nil
	case "queue":
		return configCommand[runnel.QueueConfig](ctx, name, runnel.ConfigQueue, args, func(v runnel.QueueConfig) (string, error) { return v.Name, v.Validate() })
	case "pool":
		return configCommand[runnel.PoolConfig](ctx, name, runnel.ConfigPool, args, func(v runnel.PoolConfig) (string, error) { return v.Name, v.Validate() })
	case "schedule":
		return configCommand[runnel.Schedule](ctx, name, runnel.ConfigSchedule, args, func(v runnel.Schedule) (string, error) { return v.Name, v.Validate() })
	}
	return fmt.Errorf("unknown command %q\n\n%s", name, usage)
}

func serve(ctx context.Context, args []string) error {
	c := newCmd("serve")
	addr := c.fs.String("addr", env("RUNNEL_ADDR", ":8700"), "listen address")
	token := c.fs.String("admin-token", "", "admin token (default RUNNEL_ADMIN_TOKEN)")
	noMaintain := c.fs.Bool("no-maintain", false, "do not run lease recovery, start waits, cron, webhooks and pruning here")
	archive := c.fs.Bool("archive", false, "copy pruned jobs to the archive")
	noHooks := c.fs.Bool("no-webhooks", false, "do not deliver webhooks")
	privateHooks := c.fs.Bool("allow-private-webhooks", false, "allow webhooks to private and loopback addresses")
	httpHooks := c.fs.Bool("allow-http-webhooks", false, "allow plain http webhook urls")
	jsonLogs := c.fs.Bool("json-logs", false, "log as JSON")
	if _, err := c.parse(args, 0); err != nil {
		return err
	}
	if *token == "" {
		*token = os.Getenv("RUNNEL_ADMIN_TOKEN")
	}
	if *token != "" && len(*token) < 24 {
		return errors.New("admin token must be at least 24 characters")
	}
	var log *slog.Logger
	if *jsonLogs {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	} else {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	srv := server.New(s, server.Config{AdminToken: *token, AllowHTTPWebhooks: *httpHooks, Logger: log})
	if !*noMaintain {
		m := runnel.NewMaintainer(s, runnel.MaintainerConfig{Archive: *archive, Logger: log,
			Webhooks: runnel.WebhookConfig{Enabled: !*noHooks, AllowPrivate: *privateHooks}})
		go func() {
			if err := m.Run(ctx); err != nil {
				log.Error("maintainer stopped", "err", err)
			}
		}()
	}
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: time.Minute,
		WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10}
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("runnel listening", "addr", *addr, "admin", *token != "")
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}

func namespaces(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("ns needs create, list or delete")
	}
	c := newCmd("ns " + args[0])
	secret := c.fs.String("webhook-secret", "", "secret used to sign webhooks")
	switch args[0] {
	case "create":
		pos, err := c.parse(args[1:], 1)
		if err != nil {
			return err
		}
		s, err := c.open(ctx)
		if err != nil {
			return err
		}
		defer s.Close()
		all, err := runnel.Namespaces(ctx, s)
		if err != nil {
			return err
		}
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
	case "list":
		if _, err := c.parse(args[1:], 0); err != nil {
			return err
		}
		s, err := c.open(ctx)
		if err != nil {
			return err
		}
		defer s.Close()
		all, err := runnel.Namespaces(ctx, s)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(all))
		for n := range all {
			names = append(names, n)
		}
		slices.Sort(names)
		keys, err := runnel.Keys(ctx, s)
		if err != nil {
			return err
		}
		count := map[string]int{}
		for _, k := range keys {
			count[k.Namespace]++
		}
		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tKEYS\tCREATED")
		for _, n := range names {
			fmt.Fprintf(tw, "%s\t%d\t%s\n", n, count[n], all[n].CreatedAt.Format(time.RFC3339))
		}
		return tw.Flush()
	case "delete":
		pos, err := c.parse(args[1:], 1)
		if err != nil {
			return err
		}
		s, err := c.open(ctx)
		if err != nil {
			return err
		}
		defer s.Close()
		return runnel.DeleteNamespace(ctx, s, pos[0])
	}
	return fmt.Errorf("unknown ns command %q", args[0])
}

func keys(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("key needs create, list or delete")
	}
	c := newCmd("key " + args[0])
	roles := c.fs.String("roles", "producer", "comma separated roles: admin, producer, worker, reader")
	name := c.fs.String("name", "", "label for the key")
	expires := c.fs.Duration("expires", 0, "lifetime, for example 720h (default never)")
	need := map[string]int{"create": 1, "list": 1, "delete": 2}[args[0]]
	if need == 0 {
		return fmt.Errorf("unknown key command %q", args[0])
	}
	pos, err := c.parse(args[1:], need)
	if err != nil {
		return err
	}
	s, err := c.open(ctx)
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
		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
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

func enqueue(ctx context.Context, args []string) error {
	c := newCmd("enqueue")
	payload := c.fs.String("payload", "", "JSON payload")
	priority := c.fs.Int("priority", 0, "priority, -1000 to 1000")
	delay := c.fs.Duration("delay", 0, "run after this delay")
	tenant := c.fs.String("tenant", "", "tenant")
	pool := c.fs.String("pool", "", "pool")
	attempts := c.fs.Int("max-attempts", 0, "max attempts")
	pos, err := c.parse(args, 3)
	if err != nil {
		return err
	}
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	var body any
	if *payload != "" {
		if !json.Valid([]byte(*payload)) {
			return errors.New("--payload must be valid JSON")
		}
		body = json.RawMessage(*payload)
	}
	opts := []runnel.Option{runnel.Priority(*priority), runnel.Tenant(*tenant), runnel.Pool(*pool), runnel.MaxAttempts(*attempts)}
	if *delay > 0 {
		opts = append(opts, runnel.In(*delay))
	}
	res, err := runnel.NewClient(s, pos[0]).Enqueue(ctx, pos[1], pos[2], body, opts...)
	if err != nil {
		return err
	}
	if res.Duplicate {
		fmt.Println(res.ID, "(duplicate)")
		return nil
	}
	fmt.Println(res.ID)
	return nil
}

func listJobs(ctx context.Context, args []string) error {
	c := newCmd("jobs")
	queue := c.fs.String("queue", "", "queue")
	state := c.fs.String("state", "", "state")
	limit := c.fs.Int("limit", 20, "rows")
	pos, err := c.parse(args, 1)
	if err != nil {
		return err
	}
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	jobs, next, err := runnel.NewClient(s, pos[0]).List(ctx, runnel.Query{Queue: *queue, State: runnel.State(*state), Limit: *limit})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tQUEUE\tKIND\tSTATE\tATTEMPT\tCREATED")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%s\n", j.ID, j.Queue, j.Kind, j.State, j.Attempt, j.MaxAttempts, j.CreatedAt.Format(time.RFC3339))
	}
	if next != "" {
		fmt.Fprintln(tw, "more after\t"+next)
	}
	return tw.Flush()
}

func jobCommand(ctx context.Context, name string, args []string) error {
	c := newCmd(name)
	pos, err := c.parse(args, 2)
	if err != nil {
		return err
	}
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	cl := runnel.NewClient(s, pos[0])
	var j *runnel.Job
	switch name {
	case "job":
		j, err = cl.Get(ctx, pos[1])
	case "cancel":
		j, err = cl.Cancel(ctx, pos[1])
	case "retry":
		j, err = cl.Retry(ctx, pos[1])
	}
	if err != nil {
		return err
	}
	return printJSON(j)
}

func stats(ctx context.Context, args []string) error {
	c := newCmd("stats")
	pos, err := c.parse(args, 1)
	if err != nil {
		return err
	}
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	st, err := s.Stats(ctx, pos[0])
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	header := []string{"QUEUE"}
	for _, state := range runnel.States {
		header = append(header, strings.ToUpper(string(state)))
	}
	fmt.Fprintln(tw, strings.Join(header, "\t"))
	for _, q := range st {
		row := []string{q.Queue}
		for _, state := range runnel.States {
			row = append(row, fmt.Sprint(q.Counts[state]))
		}
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	return tw.Flush()
}

func configCommand[T any](ctx context.Context, name string, kind runnel.ConfigKind, args []string, check func(T) (string, error)) error {
	if len(args) == 0 {
		return fmt.Errorf("%s needs set, list or delete", name)
	}
	c := newCmd(name + " " + args[0])
	raw := c.fs.String("json", "", "JSON body")
	need := map[string]int{"set": 2, "list": 1, "delete": 2}[args[0]]
	if need == 0 {
		return fmt.Errorf("unknown %s command %q", name, args[0])
	}
	pos, err := c.parse(args[1:], need)
	if err != nil {
		return err
	}
	s, err := c.open(ctx)
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
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf("--json: %w", err)
		}
		if _, err := check(v); err != nil {
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

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
