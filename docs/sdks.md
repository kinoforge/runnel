# SDKs

| Language | Package | Status |
|---|---|---|
| Go | `github.com/kinoforge/runnel` (library) and `github.com/kinoforge/runnel/remote` (over HTTP) | ready |
| TypeScript and Node | `@kinoforge/runnel` | planned |
| Python | `kinoforge-runnel` | planned |
| Java and Kotlin | `dev.kinoforge:runnel` | planned |
| PHP | `kinoforge/runnel` | planned |

## Go

The library talks to the store directly; `remote` talks to `runnel serve`. Both give you the same `Client` and `Worker`:

```go
c := runnel.NewClient(store, "shop")
c, err := remote.Open("https://KEY_ID:KEY_SECRET@runnel.internal:8700", "shop")

w := runnel.NewWorker(store, "shop", runnel.WorkerConfig{Queues: []string{"emails"}})
w := remote.NewWorker(c, runnel.WorkerConfig{Queues: []string{"emails"}})
```

The API reference is on [pkg.go.dev](https://pkg.go.dev/github.com/kinoforge/runnel), and full programs are in [`examples/`](../examples).

## Other languages

The other SDKs are generated from the [gRPC and Connect API](grpc-api.md) that `runnel serve` offers next to HTTP, so every language gets the same calls and a streaming worker. Each will give you a client, a worker with heartbeats and graceful shutdown, the outcome helpers (`Pending`, `RetryIn`, `Fail`, `Busy`), and a callback check.

Until they ship, any language can use the [HTTP API](http-api.md) directly (three calls make a worker, and one makes a producer), or generate a client from [`proto/runnel/v1`](../proto/runnel/v1) with its own gRPC or Connect tools.

Every SDK passes the shared checks in [`sdk-contract/`](../sdk-contract): callback signature test vectors and a list of scenarios run against a real server.
