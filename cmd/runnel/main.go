package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"text/tabwriter"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/config"
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

every command reads settings from defaults, then --config FILE or RUNNEL_CONFIG
(YAML, see deploy/runnel.example.yaml), then RUNNEL_* variables, then flags.
the store comes from --store, RUNNEL_STORE or the store key, for example
  postgres://user:pass@host/db  mysql://user:pass@host/db  sqlite:runnel.db  redis://host:6379/0
`

func main() {
	if err := runMain(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "runnel:", err)
		os.Exit(1)
	}
}

func runMain(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Print(usage)
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, args[0], args[1:])
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

type cmd struct {
	fs     *flag.FlagSet
	store  *string
	config *string
}

func newCmd(name string) *cmd {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return &cmd{
		fs:     fs,
		store:  fs.String("store", "", "store url (default from config)"),
		config: fs.String("config", os.Getenv("RUNNEL_CONFIG"), "YAML config file"),
	}
}

func (c *cmd) settings() (config.Config, error) {
	cfg, err := config.Load(*c.config, os.Getenv)
	if err != nil {
		return config.Config{}, err
	}
	if *c.store != "" {
		cfg.Store = *c.store
	}
	return cfg, nil
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

func open(ctx context.Context, cfg config.Config) (runnel.Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return stores.Open(ctx, cfg.Store)
}

func (c *cmd) start(ctx context.Context, args []string, positional int) ([]string, runnel.Store, error) {
	pos, err := c.parse(args, positional)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := c.settings()
	if err != nil {
		return nil, nil, err
	}
	s, err := open(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return pos, s, nil
}

func table() *tabwriter.Writer { return tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0) }

func run(ctx context.Context, name string, args []string) error {
	switch name {
	case "serve":
		return serve(ctx, args)
	case "migrate":
		_, s, err := newCmd(name).start(ctx, args, 0)
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
		return configCommand[runnel.QueueConfig](ctx, name, runnel.ConfigQueue, args)
	case "pool":
		return configCommand[runnel.PoolConfig](ctx, name, runnel.ConfigPool, args)
	case "schedule":
		return configCommand[runnel.Schedule](ctx, name, runnel.ConfigSchedule, args)
	}
	return fmt.Errorf("unknown command %q\n\n%s", name, usage)
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
