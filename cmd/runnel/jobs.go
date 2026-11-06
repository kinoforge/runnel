package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

func enqueue(ctx context.Context, args []string) error {
	c := newCmd("enqueue")
	payload := c.fs.String("payload", "", "JSON payload")
	priority := c.fs.Int("priority", 0, "priority, -1000 to 1000")
	delay := c.fs.Duration("delay", 0, "run after this delay")
	group := c.fs.String("group", "", "group")
	pool := c.fs.String("pool", "", "pool")
	attempts := c.fs.Int("max-attempts", 0, "max attempts")
	pos, s, err := c.start(ctx, args, 3)
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
	opts := []runnel.Option{runnel.Priority(*priority), runnel.Group(*group), runnel.Pool(*pool), runnel.MaxAttempts(*attempts)}
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
	pos, s, err := c.start(ctx, args, 1)
	if err != nil {
		return err
	}
	defer s.Close()
	jobs, next, err := runnel.NewClient(s, pos[0]).List(ctx, runnel.Query{Queue: *queue, State: runnel.State(*state), Limit: *limit})
	if err != nil {
		return err
	}
	tw := table()
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
	pos, s, err := c.start(ctx, args, 2)
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
	pos, s, err := c.start(ctx, args, 1)
	if err != nil {
		return err
	}
	defer s.Close()
	st, err := s.Stats(ctx, pos[0])
	if err != nil {
		return err
	}
	tw := table()
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
