# gRPC and Connect API

`runnel serve` also speaks gRPC, gRPC-Web and the [Connect protocol](https://connectrpc.com/docs/protocol) on its HTTP port. It is the same server, the same store and the same rules as the [HTTP API](http-api.md): pick whichever fits your language. The SDKs for other languages are generated from it.

The schema lives in [`proto/runnel/v1`](../proto/runnel/v1). Generated Go code is in [`gen/runnel/v1`](../gen/runnel/v1).

## Connecting

- Over TLS, gRPC uses HTTP/2 as usual.
- Without TLS, the server accepts HTTP/2 in clear text (h2c), so gRPC works on a private network too.
- Connect and gRPC-Web also work over HTTP/1.1, except the `Work` stream, which needs HTTP/2.
- Credentials are the same as for HTTP: send `Authorization: Bearer <token>` (or Basic auth) as request metadata. Keys need the same roles. See [logging in](http-api.md#logging-in).
- `server.max_body` caps the size of one message.

## Services

### JobService

| Call | Role | What it does |
|---|---|---|
| `Enqueue` | producer | enqueue one job or a batch. With one job, a refusal comes back as an error; with a batch, each job has its own `id`, `duplicate` or `error` |
| `EnqueueFlow` | producer | enqueue a workflow of dependent steps |
| `GetJob` | reader | read one job |
| `WaitJob` | reader | read a job, waiting up to `wait` (at most `server.max_result_wait`) for it to finish; `finished` tells whether it did |
| `ListJobs` | reader | list jobs by queue, kind, group or state, a page at a time with `cursor` |
| `CancelJob` | producer | cancel a job that has not finished |
| `RetryJob` | admin | run a dead, cancelled or expired job again from attempt one |
| `GetStats` | reader | job counts per queue and state |
| `ListEvents` | reader | jobs that reached a final state, in order, from `after` |

### WorkerService

| Call | Role | What it does |
|---|---|---|
| `Claim` | worker | claim up to `limit` jobs under a lease, waiting up to `wait` for some |
| `Extend` | worker | renew leases; answers the ids the worker no longer holds as `lost` |
| `Finish` | worker | report an attempt's outcome |
| `Dependencies` | worker | read the jobs a job depends on, with their results |
| `Work` | worker | a two-way stream: the worker asks for jobs and the server pushes them (see below) |

`Claim` and `Extend` answers carry `now`, the server's clock, which workers use to measure deadlines.

### ConfigService

Queues, pools and schedules each have five calls, for example for pools:

| Call | Role | What it does |
|---|---|---|
| `ListPools` | reader | every pool in the namespace |
| `GetPool` | reader | one pool and its `etag` |
| `PutPool` | admin | create or replace a pool |
| `PatchPool` | admin | change only the fields named in `update_mask` |
| `DeletePool` | admin | delete a pool; `NotFound` if there is none |

The same five exist as `ListQueues`, `GetQueue`, `PutQueue`, `PatchQueue`, `DeleteQueue` and `ListSchedules`, `GetSchedule`, `PutSchedule`, `PatchSchedule`, `DeleteSchedule`.

Etags are shared with the HTTP API: an etag read over HTTP works in a `precondition` here and the other way round. Set `precondition.if_match` to refuse a write when the setting changed since you read it, or `precondition.if_none_match` to only create it. Both refusals are `Aborted`. A patch can never rename a setting.

### AdminService

Admin token only.

| Call | What it does |
|---|---|
| `ListNamespaces` | every namespace |
| `PutNamespace` | create a namespace, or change its callback secret; `created` tells which |
| `DeleteNamespace` | delete a namespace, its jobs, settings and keys |
| `CreateKey` | create a key; the `token` is shown only once |
| `ListKeys` | the keys of a namespace, without their secrets |
| `DeleteKey` | revoke a key |

## Payloads

A job's `payload` and `result` travel as `bytes` holding the job's JSON exactly as you sent it, so large numbers keep their precision. In the Connect JSON encoding, `bytes` fields are base64, as protobuf JSON requires.

## The Work stream

`Work` is a long-lived stream that replaces the claim, extend and finish loop:

1. The worker sends `start` once, first: namespace, worker name, queues and the lease for its jobs.
2. The worker sends `credit` with the number of jobs it can take. Credit adds up.
3. The server sends at most that many `job` messages, each already claimed under the lease, and keeps claiming while credit is left.
4. The worker sends `extend` to renew leases and gets `extended`, with the ids it lost.
5. The worker sends `finish` with an outcome and gets `finished`, with an `error` if the server refused it (for example, a stale version). A refused finish does not end the stream.
6. The worker closes its side to stop. Jobs the server claimed but had not sent yet are given back untouched.

Every message from the server carries `now`, the server's clock.

## Errors

Errors use the standard codes. The `Runnel-Error` metadata names the Runnel error, so clients can map it back:

| Code | `Runnel-Error` | Means |
|---|---|---|
| `InvalidArgument` | `invalid` | bad input |
| `Unauthenticated` | | no or wrong credentials |
| `PermissionDenied` | | the key lacks the role |
| `NotFound` | `not_found` | unknown job, setting, key or namespace |
| `FailedPrecondition` | `conflict`, `final`, `not_final`, `dependency` | the job changed, already finished, is not finished, or a dependency is missing or failed |
| `Aborted` | `stale` | a setting changed since it was read, or already exists |
| `ResourceExhausted` | `queue_full`, `group_full`, `no_space` | a queue or group is full, or the store is low on disk; wait `Retry-After` seconds |

## Trying it

With [grpcurl](https://github.com/fullstorydev/grpcurl) and the proto files, against a server without TLS:

```sh
grpcurl -plaintext -import-path proto -proto runnel/v1/job.proto \
  -H "Authorization: Bearer $KEY" \
  -d '{"namespace":"shop","jobs":[{"queue":"emails","kind":"welcome","payload":"eyJ0byI6ImFuYUBleGFtcGxlLmNvbSJ9"}]}' \
  localhost:8700 runnel.v1.JobService/Enqueue
```

With curl, over the Connect protocol:

```sh
curl -s localhost:8700/runnel.v1.JobService/GetStats \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"namespace":"shop"}'
```

In Go, use the generated clients in `gen/runnel/v1/runnelv1connect`, or keep using [`remote`](sdks.md#go), which speaks HTTP.
