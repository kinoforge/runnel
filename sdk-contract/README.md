# SDK contract

Every Runnel SDK, in any language, passes the same checks. This folder holds them, so the SDKs agree with the server and with each other.

- [`callback-signatures.json`](callback-signatures.json): test vectors for the `Runnel-Signature` header of callbacks. Each vector gives the secrets, the exact body, the time to check at, the allowed age and whether the signature is valid. An SDK's `verifyCallback` must give the same answer for every vector. Runnel's own `VerifySignature` is tested against this file, so the file and the server cannot drift apart.
- The scenarios below: each SDK runs them in its CI against the released `ghcr.io/kinoforge/runnel` image, through its public API only.

The signature is `t=<unix seconds>,v1=<lowercase hex HMAC-SHA256 of "<t>.<body>">`, as rule H02 in the [spec](../docs/SPEC.md) says. A header may carry several `v1` values; one match is enough. Compare in constant time.

## Scenarios

Each scenario starts with a fresh namespace and keys made through the admin API.

| Id | Scenario | Passes when |
|---|---|---|
| C01 | Enqueue a job, a worker handles it and sets a result | the job is `completed` with that result, and an enqueue-and-wait call returns it |
| C02 | The handler throws once, then succeeds | the job is `completed` on attempt 2, with the first error in its history |
| C03 | The handler always throws, `max_attempts: 2` | the job is `dead` after 2 attempts |
| C04 | The handler returns `Fail` | the job is `dead` after 1 attempt |
| C05 | The handler returns `RetryIn(2s)` | the next attempt starts no sooner than 2 seconds later |
| C06 | The handler returns `Busy(1s)` | the job runs again without using an attempt |
| C07 | The handler returns `Pending(handle, 1s)`; the poller completes it on the third poll | the job is `completed`, and the poller saw the same handle each time |
| C08 | Cancel a waiting job | it is `cancelled` and no worker receives it |
| C09 | A job with a deadline 2 seconds away, the handler runs longer | the job is `expired`, and the handler's cancellation signal fired before the deadline on the server's clock |
| C10 | The worker holds a job longer than its lease, sending heartbeats | the job stays `running` and completes; no other worker received it |
| C11 | The worker stops heartbeats and the lease runs out | the SDK reports the lease as lost, and the job runs again |
| C12 | Shut the worker down while it holds jobs it has not started | those jobs are released untouched, with their attempt count unchanged |
| C13 | A queue with `max_queued: 1` gets a second job | the SDK raises its queue-full error, with the server's retry delay |
| C14 | Enqueue 5 jobs with the same unique key | one job is made, and four answers say `duplicate` |
| C15 | A flow `a`, then `b` and `c` after `a`, then `d` after both | the steps run in that order, and `d` reads the results of `b` and `c` |
| C16 | A worker with concurrency 2 and 6 jobs | never more than 2 run at once, and all 6 complete |
| C17 | A namespace with a callback secret, a job with a callback URL served by the test | the callback arrives once, and the SDK's verify call accepts it |
| C18 | A key without the `producer` role enqueues | the SDK raises its permission error |
| C19 | A worker clock 30 seconds off the server's | deadlines are still measured on the server's clock (C09 passes) |

SDKs with a streaming worker run C10 to C12 and C16 both over the `Work` stream and over unary `Claim`.
