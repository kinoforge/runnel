# SDKs

| Language | Package | Status |
|---|---|---|
| Go | `github.com/kinoforge/runnel` (library) and `github.com/kinoforge/runnel/remote` (over HTTP) | ready |
| TypeScript and Node | `@kinoforge/runnel`, in [runnel-js](https://github.com/kinoforge/runnel-js) | beta, install from GitHub until it is on npm |
| Python | `kinoforge-runnel`, in [runnel-py](https://github.com/kinoforge/runnel-py) | beta, install from GitHub until it is on PyPI |
| Java and Kotlin | `dev.kinoforge:runnel`, in [runnel-jvm](https://github.com/kinoforge/runnel-jvm) | beta, install from GitHub through JitPack until it is on Maven Central |
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

## TypeScript and Node

```sh
npm install github:kinoforge/runnel-js
```

```ts
const runnel = new Client({ url: "http://runnel.internal:8700", namespace: "shop", token: process.env.RUNNEL_KEY });
await runnel.enqueue({ queue: "emails", kind: "welcome", payload: { to: "ana@example.com" } });

const worker = new Worker(runnel, { queues: ["emails"] });
worker.handle("welcome", async (job, { signal }) => ({ sent: true }));
await worker.run();
```

Needs Node 22.12 or later. The [README](https://github.com/kinoforge/runnel-js#readme) has the rest.

## Python

```sh
pip install git+https://github.com/kinoforge/runnel-py
```

```python
runnel = AsyncClient("http://runnel.internal:8700", "shop", token=os.environ["RUNNEL_KEY"])
await runnel.enqueue("emails", "welcome", {"to": "ana@example.com"})

worker = Worker(runnel, ["emails"])
worker.handle("welcome", send_welcome)
worker.serve()
```

Needs Python 3.11 or later. Workers run on asyncio and take async or plain handlers; producers can use the blocking `Client` instead. The [README](https://github.com/kinoforge/runnel-py#readme) has the rest.

## Java and Kotlin

```kotlin
repositories { maven("https://jitpack.io") }
dependencies { implementation("com.github.kinoforge:runnel-jvm:main-SNAPSHOT") }
```

```kotlin
val runnel = RunnelClient("http://runnel.internal:8700", "shop") { token = System.getenv("RUNNEL_KEY") }
runnel.enqueue("emails", "welcome", mapOf("to" to "ana@example.com"))

val worker = Worker(runnel, listOf("emails"))
worker.handle("welcome") { job, ctx -> mapOf("sent" to true) }
worker.serve()
```

Needs Java 17 or later. Kotlin gets suspend calls; Java gets `BlockingClient`, `handleBlocking` and a `CompletableFuture` from `worker.start()`. The [README](https://github.com/kinoforge/runnel-jvm#readme) has the rest.

## Other languages

The SDKs other than Go are generated from the [gRPC and Connect API](grpc-api.md) that `runnel serve` offers next to HTTP, so every language gets the same calls and a streaming worker. Each gives you a client, a worker with heartbeats and graceful shutdown, the outcome helpers (`Pending`, `RetryIn`, `Fail`, `Busy`), and a callback check. PHP comes next.

Until yours ships, any language can use the [HTTP API](http-api.md) directly (three calls make a worker, and one makes a producer), or generate a client from [`proto/runnel/v1`](../proto/runnel/v1) with its own gRPC or Connect tools.

Every SDK passes the shared checks in [`sdk-contract/`](../sdk-contract): callback signature test vectors and a list of scenarios run against a real server.
