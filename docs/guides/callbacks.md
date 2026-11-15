# Callbacks

When a job ends, Runnel can post its summary to your URL, so your service learns the result without polling.

## Set it up

1. Give the namespace a callback secret. Runnel signs every callback with it.

    ```sh
    curl -u admin:$RUNNEL_ADMIN_TOKEN -X PUT localhost:8700/v1/namespaces/shop \
      -d '{"callback_secret":"'$(openssl rand -hex 16)'"}'
    ```

2. Name the URL on the job, or once on its queue so every job in it uses it:

    ```sh
    curl -u admin:$RUNNEL_ADMIN_TOKEN -X PUT localhost:8700/v1/ns/shop/queues/emails \
      -d '{"name":"emails","callback_url":"https://app.example.com/hooks/runnel"}'
    ```

    In Go, per job: `c.Enqueue(ctx, "emails", "welcome", in, runnel.Callback("https://app.example.com/hooks/runnel"))`.

3. Make sure a maintainer runs with callbacks on. `runnel serve` does, unless `callbacks.enabled` is false.

## What arrives

A `POST` with the body `{"event", "delivery", "job"}` and these headers:

```
Runnel-Event: job.completed
Runnel-Delivery: 1842
Runnel-Signature: t=1761900000,v1=<hex hmac_sha256(secret, "<t>.<body>")>
```

The event is `job.completed`, `job.dead`, `job.cancelled` or `job.expired`, and `job` is the job as it ended, with its result or errors. Without a namespace secret, the signature header is left out.

## Check it

Compute the HMAC-SHA256 of `"<t>.<raw body>"` with the secret, compare it in constant time with `v1`, and refuse a `t` older than a few minutes. In Go:

```go
cb, err := runnel.CallbackReader{Secrets: []string{secret}}.Read(r)
if err != nil {
	http.Error(w, err.Error(), http.StatusUnauthorized)
	return
}
```

While you rotate the secret, list both: `Secrets: []string{newSecret, oldSecret}`.

## Rules to know

- **Deliveries can repeat.** Answer `2xx` once you have stored it, and dedupe on `Runnel-Delivery`.
- **Some failures are retried.** `408`, `429`, `5xx` and timeouts are retried with backoff, 10 attempts in all by default, after `Retry-After` when given (up to an hour). Any other answer, a redirect included, fails the delivery.
- **Safe by default.** Callbacks use `https`, never follow redirects, and are never sent to private or loopback addresses. `callbacks.allow_private` and `server.allow_http_callbacks` lift that for private networks.
- **Tracing.** A `traceparent` in the job's meta is sent along, so the receiver's span joins the same trace.

The rules are H01 to H06 in [SPEC.md](../SPEC.md); the story is U01.
