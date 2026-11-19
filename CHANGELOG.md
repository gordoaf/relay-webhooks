# Changelog

## 0.2.0 - 2026-11-19

- Route new events through endpoint subscriptions, with exact types, namespace wildcards, and immutable accepted fanout.
- Edit endpoint destinations, descriptions, and rate limits. Rotate project keys and endpoint signing secrets with bounded overlap; record actual destinations in new attempt history.
- Preview and recover groups of failed deliveries through durable paced jobs, with progress, idempotent starts, and cancellation.
- Add queue health, storage visibility, authenticated Prometheus metrics, persisted alerts, and independently delivered signed operator notifications.
- Add opt-in bounded retention, explicit post-purge idempotency protection, storage admission checks, and cleanup/status commands.
- Ship Python and TypeScript clients with stable retry idempotency, signature verification, and a runnable storefront with transactional outbox and receiver deduplication.
- Extend the console and browser simulation for the new workflows. Migrate schema 1/2 to 3; stop all old processes and back up before upgrading. Retention remains disabled until configured.

## 0.1.1 — 2026-11-19

- Isolate slow destinations with database-enforced endpoint and project concurrency limits, fair scheduling, and persistent timeout cooldowns. Use endpoint-scoped queue indexes to avoid scanning parked backlogs.
- Coalesce DNS lookups and bound negative caching. Explicitly reject special IPv4/IPv6 destination ranges and report permanent network-policy failures without retries.
- Bound connection/TLS time and optional response diagnostics. Preserve received success responses, safely parse oversized `Retry-After` values, and account for unexpected delivery failures within the retry budget.
- Add `RELAY_PUBLIC_URL` for reverse-proxy deployments. Share failed-authentication throttling across login and administrator bearer routes without blocking valid credentials, and persist revocable browser sessions.
- Reject malformed Unicode and oversized pagination consistently, reserve idempotency conflicts for HTTP 409, and offload blocking database operations from asynchronous handlers.
- Restrict POSIX database permissions, improve worker diagnostics, and migrate schema 1 to 2 while preserving delivery state. Stop all old processes before upgrading; existing browser sessions require a new login.
- Add regression coverage for the confirmed adversarial findings and deployment/container checks.

## 0.1.0 — 2026-11-19

Initial release with durable event acceptance, leased delivery workers, retry policy, HMAC signatures, project API keys, endpoint controls, delivery history and replay, a browser console, and a local failure-recovery lab.

Includes Python and Docker installation paths, a signed receiver example, a reproducible local benchmark, and tests for storage, authentication, networking, and recovery behavior.
