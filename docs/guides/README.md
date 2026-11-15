# Guides

Each guide solves one common job with Runnel, in Go and over HTTP. They assume you have read [getting started](../getting-started.md).

| Guide | When you need it |
|---|---|
| [Callbacks](callbacks.md) | your service wants to hear when a job ends, without polling |
| [Rate-limited providers](rate-limited-providers.md) | you call an outside API that limits calls, fails now and then, or goes down |
| [Async work](async-work.md) | the work runs in an outside system and you check on it later |
| [Pipelines](pipelines.md) | steps depend on each other and read each other's results |
| [Fair sharing](fair-sharing.md) | one busy customer must not slow down the others |
| [Safe re-enqueue](safe-reenqueue.md) | a client may send the same request twice |
| [Scheduled chores](scheduled-chores.md) | something must run every few seconds, hourly or nightly |
| [Graceful deploys](graceful-deploys.md) | you restart workers without losing or repeating work |
| [Backpressure](backpressure.md) | producers can outpace workers, or the disk can fill |

Each guide matches a user story in [SPEC.md](../SPEC.md#stories-u) that runs as a test on every store.
