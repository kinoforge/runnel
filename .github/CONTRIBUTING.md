# Contributing to Runnel

Thank you for helping. Bug reports, fixes, docs and new store backends are all welcome.

## Before you start

- For anything bigger than a small fix, open an issue first so we can agree on the shape.
- Security problems go through [SECURITY.md](SECURITY.md), not public issues.
- By contributing you agree your work is released under the Apache License 2.0. Add yourself to [AUTHORS](AUTHORS) in your first pull request.

## Development

You need Go (the version in `go.mod`) and Docker.

```sh
make up          # postgres, mysql, redis, valkey and dragonfly on local ports
make test        # unit tests and SQLite, with the race detector
make test-all    # every backend
make lint        # gofmt and go vet
make vuln        # govulncheck
make fuzz        # fuzz targets, 30s each
make down        # stop the services and drop their data
```

## What a pull request needs

- A rule in [SPEC.md](../docs/SPEC.md) for any behaviour change, and tests that name it with `spec.Rule(t, "C04")`. `go test ./internal/spec` fails when a rule has no test or a test names a rule that does not exist. Run one rule's tests with `RUNNEL_RULES=C04 make test-all`.
- Tests. A behaviour change in a store goes in the shared conformance suite (`store/storetest`), so every backend proves it.
- A note in [DESIGN.md](../docs/DESIGN.md) when the change is a decision someone could make differently.
- `make lint` and `make test-all` passing.
- Code that reads like the code around it. As few comments as possible: exported identifiers have a short doc comment, function bodies carry none. If a block needs explaining, give it a name.
- Tests that fail are fixed in the code, never bent to pass: no loosened assertions, skips, longer timeouts, retries or sleeps to hide a failure. Change a test only when the test itself is wrong, and say why in the pull request.
- No hardcoded rules. Runnel is a generic tool: every limit, timeout, size or behaviour a user could need differently is a setting with a sensible default. No business rules and no special cases for one user.
- No new dependency without a reason in the pull request.
- An entry in [CHANGELOG.md](CHANGELOG.md) under "Unreleased" for anything a user would notice.

## Adding a store

Implement `runnel.Store` and run `storetest.Run` against it. Optional interfaces add features:

- `runnel.Waker` lets workers wake as soon as a job is ready.
- `runnel.EndWatcher` lets waiters wake as soon as a job finishes.
- `runnel.TimeSource` makes every process share the store's clock.
- `runnel.TxInserter` enables enqueueing inside a caller's transaction.

## Releases

Maintainers tag `vX.Y.Z` on `main`. The release workflow builds the binaries and images, writes SBOMs and signs everything.
