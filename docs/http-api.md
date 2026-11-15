# HTTP API

`runnel serve` speaks JSON over HTTP. This page is a map of the API. The full schema of every request and answer is the OpenAPI 3.1 document at [`server/openapi.yaml`](../server/openapi.yaml), also served at `/openapi.yaml`. The same calls are offered over gRPC and Connect: see the [gRPC and Connect API](grpc-api.md).

## Logging in

Every call under `/v1` needs credentials, unless the server runs with `server.auth: none`:

| Who | How |
|---|---|
| the admin | Basic auth `admin:<admin token>`, or `Authorization: Bearer <admin token>`, or a client certificate signed by `server.tls.client_ca` |
| a namespace key `rnk_<id>_<secret>` | `Authorization: Bearer rnk_<id>_<secret>`, or Basic auth `<id>:<secret>` |

A key works only in its own namespace, and only for its roles: `producer`, `reader`, `worker` or `admin`. See [concepts](concepts.md#namespaces-and-keys). Over plain http, Basic auth sends the password in clear text: use TLS whenever the server is reachable beyond localhost.

## Errors

Errors are [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem documents (`application/problem+json`) with a `title` and a `detail`:

| Status | Means | Go error |
|---|---|---|
| `400` | bad input | `runnel.ErrInvalid` |
| `401`, `403` | no or wrong credentials, or a missing role | |
| `404` | unknown job, setting, key or namespace | `runnel.ErrNotFound` |
| `409` | a conflict: the job changed, already finished, or is not finished | `runnel.ErrConflict`, `ErrFinal`, `ErrNotFinal` |
| `412` | a setting changed since it was read (`If-Match`) | `runnel.ErrStale` |
| `413` | the body is too large | |
| `415` | a `PATCH` that is not `application/merge-patch+json` | |
| `422` | a dependency is missing or failed | `runnel.ErrDependency` |
| `429` | a queue or group is full; see `Retry-After` | `runnel.ErrQueueFull`, `ErrGroupFull` |
| `507` | the store is low on disk space; see `Retry-After` | `runnel.ErrNoSpace` |

Bodies are parsed strictly: an unknown field or trailing data is a `400`.

## Jobs

| Call | Role | What it does |
|---|---|---|
| `POST /v1/ns/{ns}/jobs` | producer | enqueue one job, or a batch with `{"jobs":[…]}` |
| `POST /v1/ns/{ns}/flows` | producer | enqueue a workflow of dependent steps |
| `GET /v1/ns/{ns}/jobs` | reader | list jobs, newest first, filtered by `queue`, `state`, `kind` or `group`, paged with `cursor` |
| `GET /v1/ns/{ns}/jobs/{id}` | reader | read a job; with `Prefer: wait=N`, wait up to N seconds for it to finish |
| `POST /v1/ns/{ns}/jobs/{id}/cancel` | producer | cancel a job that has not finished |
| `POST /v1/ns/{ns}/jobs/{id}/retry` | admin | run a dead, cancelled or expired job again from attempt one |
| `GET /v1/ns/{ns}/stats` | reader | job counts per queue and state |
| `GET /v1/ns/{ns}/events` | reader | jobs that reached a final state, in order, from `after` |

Enqueue and wait for the result:

```sh
curl -i localhost:8700/v1/ns/shop/jobs -H "Authorization: Bearer $KEY" -H "Prefer: wait=30" \
  -d '{"queue":"emails","kind":"welcome","payload":{"to":"ana@example.com"},"group":"customer-42","max_attempts":3}'
```

The answer is `200` with the finished job, `202` with a `Location` and `Retry-After` while it still runs, or `201` without `Prefer`. A queue can set a default `wait`.

## Workers

| Call | Role | What it does |
|---|---|---|
| `POST /v1/ns/{ns}/claim` | worker | claim up to `limit` jobs under a lease, waiting up to `wait` for some |
| `POST /v1/ns/{ns}/extend` | worker | renew leases; answers the ids the worker no longer holds as `lost` |
| `POST /v1/ns/{ns}/jobs/{id}/finish` | worker | report an attempt's outcome |
| `POST /v1/ns/{ns}/dependencies` | worker | read the jobs a job depends on, with their results |

```sh
curl localhost:8700/v1/ns/shop/claim -H "Authorization: Bearer $WORKER_KEY" \
  -d '{"worker":"w1","queues":["emails"],"limit":10,"lease":"30s","wait":"20s"}'

curl localhost:8700/v1/ns/shop/extend -H "Authorization: Bearer $WORKER_KEY" \
  -d '{"worker":"w1","ids":["01a1…"],"lease":"30s"}'

curl localhost:8700/v1/ns/shop/jobs/01a1…/finish -H "Authorization: Bearer $WORKER_KEY" \
  -d '{"worker":"w1","version":3,"outcome":{"kind":"done","result":{"sent":true}}}'
```

An outcome's `kind` is `done`, `retry`, `fail`, `busy`, `pending` or `release`, with `result`, `error`, `delay` or `handle` as it needs. Claim and extend answers carry `now`, the server's clock, which workers use to measure deadlines. A worker key can only finish jobs it claimed itself.

## Settings

Queues, pools and schedules share one shape:

| Call | Role | What it does |
|---|---|---|
| `GET /v1/ns/{ns}/queues` | reader | list queue settings |
| `GET /v1/ns/{ns}/queues/{name}` | reader | read one, with an `ETag` |
| `PUT /v1/ns/{ns}/queues/{name}` | admin | set it whole |
| `PATCH /v1/ns/{ns}/queues/{name}` | admin | change some fields, as a JSON merge patch |
| `DELETE /v1/ns/{ns}/queues/{name}` | admin | remove it |
| `GET /v1/ns/{ns}/pools`, `GET /v1/ns/{ns}/pools/{name}`, `PUT /v1/ns/{ns}/pools/{name}`, `PATCH /v1/ns/{ns}/pools/{name}`, `DELETE /v1/ns/{ns}/pools/{name}` | as above | the same for pools |
| `GET /v1/ns/{ns}/schedules`, `GET /v1/ns/{ns}/schedules/{name}`, `PUT /v1/ns/{ns}/schedules/{name}`, `PATCH /v1/ns/{ns}/schedules/{name}`, `DELETE /v1/ns/{ns}/schedules/{name}` | as above | the same for schedules |

Change one field without overwriting the rest:

```sh
curl -u admin:$TOKEN -X PATCH -H 'Content-Type: application/merge-patch+json' \
  localhost:8700/v1/ns/shop/pools/acme-api -d '{"paused_until":"2026-11-14T18:00:00Z"}'
```

`null` clears a field. Send `If-Match: <ETag>` to write only if nobody changed it since your read (`412` otherwise), or `If-None-Match: *` to create only.

## Namespaces and keys

Admin only.

| Call | What it does |
|---|---|
| `GET /v1/namespaces` | list namespaces |
| `PUT /v1/namespaces/{name}` | create a namespace, or set its `callback_secret` |
| `GET /v1/namespaces/{name}` | read one, with its keys and queue counts |
| `DELETE /v1/namespaces/{name}` | delete it and revoke its keys; its jobs stay until pruned |
| `POST /v1/namespaces/{name}/keys` | create a key with `name`, `roles` and an optional `expires_at`; the answer holds the key once |
| `DELETE /v1/namespaces/{name}/keys/{id}` | revoke a key |

## Operations

| Call | Auth | What it does |
|---|---|---|
| `GET /healthz` | none | `200` while the process and its store work, `503` if the store stopped |
| `GET /readyz` | none | `200` while the server takes traffic, `503` while draining |
| `GET /metrics` | none | Prometheus metrics |
| `GET /openapi.yaml` | none | the OpenAPI document |
| `GET /v1/system` | admin | memory, storage and version |
| `GET /v1/backup` | admin | a copy of a hybrid store, for `runnel restore` |

The [runbook](RUNBOOK.md) explains probes, metrics and backups.
