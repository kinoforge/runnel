# Security policy

## Supported versions

Runnel is before 1.0. Security fixes land on the latest release only.

| Version | Supported |
|---|---|
| latest 0.x | yes |
| older | no |

## Reporting a vulnerability

Please do not open a public issue.

Report privately through [GitHub security advisories](https://github.com/kinoforge/runnel/security/advisories/new), or by email to oss@kinoforge.dev. Include the version, how to reproduce it and what an attacker gains.

We confirm receipt within 3 working days and send a first assessment within 10. Once a fix is released we publish an advisory and credit you, unless you ask us not to.

## What counts

In scope: the `runnel` server and CLI, the Go packages in this module, and the release artifacts.

Examples: authentication or role bypass, one namespace reading or changing another, callback delivery reaching private addresses, signature forgery, or a crafted request that crashes the server or exhausts its memory.

Out of scope: attacks that need the admin token or direct access to the database, and deployments that expose the server without TLS.

## Hardening notes for operators

- Run the server behind TLS. Keep the admin token out of shell history and logs.
- Give each service its own namespace and keys with the fewest roles it needs.
- Callbacks are signed (`Runnel-Signature`, HMAC-SHA256 with a timestamp). Verify them, and reject old timestamps.
- Callbacks need `https` and never follow redirects. Delivery refuses private, loopback and link-local addresses, checked on every connection, so DNS tricks cannot reach them. `--allow-http-callbacks` and `--allow-private-callbacks` lift these rules for local development only.
- Release archives come with a checksum file signed with Sigstore cosign, and images on `ghcr.io/kinoforge/runnel` are signed too. Verify them before you deploy.
