# Pipelines

When steps depend on each other, enqueue them together as a flow. Each step waits for the steps it names, reads their results, and the whole flow stops if one fails.

## In Go

```go
fetch, _ := runnel.NewJob("media", "image.fetch", map[string]string{"url": src})
resize, _ := runnel.NewJob("media", "image.resize", map[string]int{"width": 640})
publish, _ := runnel.NewJob("media", "image.publish", nil)

ids, err := c.EnqueueFlow(ctx, runnel.NewFlow().
	Add("fetch", fetch).
	Add("resize", resize, "fetch").
	Add("publish", publish, "fetch", "resize"))
```

A step reads the results of the steps before it:

```go
w.HandleFunc("image.publish", func(ctx context.Context, j *runnel.Job) error {
	deps, err := runnel.Dependencies(ctx)
	if err != nil {
		return err
	}
	for _, d := range deps {
		log.Printf("%s gave %s", d.Kind, d.Result)
	}
	return nil
})
```

## Over HTTP

```sh
curl localhost:8700/v1/ns/shop/flows -H "Authorization: Bearer $APP_KEY" -d '{"steps":[
  {"name":"fetch","queue":"media","kind":"image.fetch","payload":{"url":"https://example.com/a.png"}},
  {"name":"resize","queue":"media","kind":"image.resize","after":["fetch"]},
  {"name":"publish","queue":"media","kind":"image.publish","after":["fetch","resize"]}
]}'
```

The answer maps each step name to its job id. A worker reads a job's dependencies with `POST /v1/ns/{ns}/dependencies`.

## What happens

- A step with dependencies starts `blocked` and becomes `waiting` once all of them complete.
- If a step ends dead, cancelled or expired, every step after it is cancelled, each with its own event.
- The flow is checked whole before anything is stored: unknown step names or a cycle refuse the whole flow.

You can also link single jobs with `runnel.After(id...)`, or `depends_on` over HTTP. The rules are F01 to F06 in [SPEC.md](../SPEC.md); the story is U08.
