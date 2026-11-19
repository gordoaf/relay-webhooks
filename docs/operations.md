# Operations

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `RELAY_ADMIN_TOKEN` | Required | Random administrator token of at least 24 characters |
| `RELAY_DB_PATH` | `data/relay.db` | Local SQLite database |
| `RELAY_PUBLIC_URL` | Unset | Canonical browser-facing HTTP(S) origin, such as `https://relay.example.com` |
| `RELAY_ALLOW_PRIVATE_NETWORKS` | `false` | Permit private destination addresses only in a controlled environment |
| `RELAY_WORKER_ENABLED` | `true` | Run delivery workers with the API |
| `RELAY_WORKER_CONCURRENCY` | `4` | Delivery threads per API/worker process, 1-64 |
| `RELAY_METRICS_TOKEN` | Unset | Optional separate random token of at least 24 characters, accepted only for Prometheus scraping |

Normal operation is `relay serve --host 127.0.0.1 --port 8787`. The process exits if its administrator token is missing or too short. The CLI reads environment variables; it does not load `.env` files itself. Docker Compose loads `.env` for interpolation.

## Deployment

Use a TLS reverse proxy for public access. Set `RELAY_PUBLIC_URL` to the exact origin visitors use, including a nonstandard port if applicable. It must not contain credentials, a path other than `/`, a query, or a fragment. Relay uses this origin for CSRF validation and sets Secure session cookies for HTTPS even when the proxy connects to Relay over HTTP. This works with `relay serve` and the supplied Docker Compose file; add the setting to `.env` for Compose.

Keep the backend port private and reachable only by the proxy. Preserve the public Host header when possible. Relay does not trust client-supplied forwarding headers by default. If you also need the original client address for per-IP throttling, configure the proxy to overwrite forwarding headers and launch Uvicorn with an explicit allowlist of the proxy's actual source addresses:

```sh
uvicorn relay.app:create_app --factory --host 127.0.0.1 --port 8787 \
  --proxy-headers --forwarded-allow-ips=127.0.0.1
```

Never use `--forwarded-allow-ips='*'` on a backend reachable by untrusted clients. `RELAY_PUBLIC_URL` does not establish trust in forwarded client addresses.

Keep the console and administrative API behind your private network or access gateway. Allow public access to event ingestion only when required. Add connection limits and request-rate limits at the proxy. The built-in failed-authentication throttle covers login, administrator bearer authentication, and metrics bearer authentication, is bounded and per process, and does not reject valid credentials because another client used up the failure allowance. It is not distributed request-rate protection. Always generate tokens randomly; the length check cannot establish that a manually chosen token is unpredictable.

The application checks credentials before returning a failure-limit response, including on `/metrics`. HTTP 429 therefore does **not cap credential comparisons** or independently prevent token guessing. Enforce the request ceiling before requests reach Relay, including the metrics route. For example, with Nginx receiving client connections directly, declare a zone in the `http` context and apply it inside your existing TLS server:

```nginx
# In http {}:
limit_req_zone $binary_remote_addr zone=relay_admin:10m rate=5r/s;

# In the configured TLS server {}:
location /api/ {
    limit_req zone=relay_admin burst=20 nodelay;
    limit_req_status 429;
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_pass http://127.0.0.1:8787;
}

location = /metrics {
    limit_req zone=relay_admin burst=20 nodelay;
    limit_req_status 429;
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_pass http://127.0.0.1:8787;
}
```

Tune the ceiling for expected traffic. Visitors sharing an IP share this budget. If Nginx itself sits behind another proxy, configure trusted client-address handling first. See the [Nginx request-limit reference](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html). Keep the administrative access gateway in place; this snippet is an ingress-limit example, not a complete TLS deployment.

The destination network policy is rechecked on each attempt. Keep private-network mode disabled on an internet-accessible deployment. For trusted internal destinations, enforce an outbound network allowlist in the hosting environment as well.

The Docker image runs as UID 10001 with a persistent `/data` volume. Bind mounts must be writable by that UID. Allow at least 30–45 seconds for graceful shutdown; in-flight work can otherwise resume after its lease expires.

On POSIX systems, Relay creates its data directory with mode 0700 and restricts the database to 0600. It does not make arbitrary existing parent directories private. Restrict existing data directories and backups to the service account, and review their permissions when upgrading. On Windows, apply equivalent filesystem ACLs to the service account; POSIX mode bits do not replace Windows ACLs. SQLite's WAL and shared-memory files also contain sensitive state and must stay inside the protected directory.

## Separate workers

Set `RELAY_WORKER_ENABLED=false` for the API and run `relay worker` with the same database path, administrator configuration, and destination policy. Multiple processes on the same host can claim work safely. All must use a local filesystem and the same configuration. This is not a multi-host database setup.

The scheduler allows one active delivery per endpoint and two per project by default, counted from unexpired leases across processes. It alternates eligible projects and endpoints. An attempt taking at least five seconds marks an endpoint slow, even if the receiver closes without returning HTTP. An HTTP response under five seconds qualifies for responsive capacity. Classification expires after 15 minutes; new or unclassified endpoints use the untested lane. Slow, untested, and timing-out endpoints together may use at most one slot per project and `max(1, RELAY_WORKER_CONCURRENCY - 1)` slots across the database. This leaves a slot for recently responsive destinations when concurrency is at least two. All processes sharing a database must use the same concurrency setting.

The reserve is intentional: with four workers, an entirely slow or untested workload can initially use at most three slots, and each affected project can use only one. A single worker cannot reserve separate responsive capacity. A brand-new healthy destination must first pass through the bounded untested lane, so its first response can wait behind earlier claims. A previously responsive receiver can also fail on its next request. These limits reduce interference; they do not guarantee latency during every outage. URL changes require the new target to qualify again. A slow HTTP 2xx response remains delivered, with no retry or timeout penalty solely because it was slow.

After the first timeout, that endpoint's next claims additionally share a single instance-wide probe allowance with other timing-out endpoints. Three consecutive timeouts start a 30-second cooldown, then 60, 120, 240, 480, and at most 600 seconds on further timeouts. A non-timeout result resets the streak. Endpoint rate limits still apply.

DNS lookups for the same hostname are coalesced and failures have a short bounded cache. Each process allows eight outstanding operating-system lookups, with at most two owned by one delivery project and six owned by delivery work altogether. The remaining capacity is available to endpoint validation and operator notifications. Caller timeouts do not release slots until the underlying OS lookup finishes. Local capacity shortages defer deliveries without recording a failed attempt, consuming retry budget, or extending endpoint cooldowns. After a capacity deferral, admission backs off at the affected project or global scope before claiming more work that needs a new lookup. The delay grows from one to five seconds with jitter and clears when resolver capacity is released. Numeric destinations and reusable hostname lookups remain eligible; project pressure does not pause unrelated projects. This limits repeated claims and queue writes during sustained resolver pressure.

The actual destination's DNS timeout still counts as a failed attempt. Distinct stalled names across projects can exhaust delivery capacity; use a reliable local resolver and monitor capacity deferrals. These per-process bounds are not a host-wide DNS quota, and admission backoff cannot cancel an operating-system DNS lookup.

## Backups and retention

Use SQLite's online backup API or stop the service before taking a filesystem backup. Copying only the main database file while WAL writes are active can omit committed data.

```python
import sqlite3

with sqlite3.connect("data/relay.db") as source:
    with sqlite3.connect("relay-backup.db") as target:
        source.backup(target)
```

Backups include event data, receiver response excerpts, and signing secrets. Store them with restricted permissions. Restore into an isolated instance first; pending or expired processing rows can resume delivery when workers start.

Automatic retention is disabled by default. Set `RELAY_RETENTION_DAYS=30` to retain completed history for 30 days. An event becomes eligible only after its entire fanout is terminal and all of its deliveries have remained unchanged past the cutoff. Queued recovery members are protected. Cleanup removes bounded batches of attempts, deliveries, and then events; large histories may need several passes.

Automatic cleanup runs up to 50 batches or a 200 ms soft budget per burst, releasing the writer lock and yielding between batches. Remaining work schedules another burst after one second. A complete scan with no deletion progress returns to the ordinary maintenance interval. The budget is checked between batches: a database busy wait or one batch may take longer. Monitoring keeps its separate normal interval. Manual cleanup and previews still operate on one batch. Measure sustained cleanup against your workload; the age setting does not guarantee immediate deletion of every eligible record.

The service preserves a hashed idempotency-key tombstone for `RELAY_IDEMPOTENCY_DAYS` (default 90) after an event is purged. During that window an identical submission returns HTTP 410 with the original event ID, and conflicting content returns 409. After expiry the key can create a new event. Unfinished events retain their original idempotency records regardless of age.

Preview one batch before enabling automatic deletion on a running service:

```powershell
$env:RELAY_RETENTION_DAYS = '30'
$env:RELAY_IDEMPOTENCY_DAYS = '90'
relay cleanup
relay cleanup --apply
relay status
```

On macOS/Linux use `export RELAY_RETENTION_DAYS=30` and `export RELAY_IDEMPOTENCY_DAYS=90`, then the same commands. CLI commands use the process environment; `.env` is only loaded by Compose. Cleanup does not run VACUUM: SQLite reuses freed pages and the file need not shrink. Keep backups before deleting history. All API/worker processes sharing a database must use the same policy.

## Diagnose delivery failures

Use the attempt history to distinguish receiver status errors from network failures. Fix the destination, then replay the failed delivery. An enabled receiver returning 2xx completes the delivery; a paused endpoint keeps queued work parked.

The health endpoint checks that the process can read the database. It does not assert queue freshness, receiver health, or a delivery SLA. Check pending/retrying counts and oldest creation times when assessing backlog.

Application logs omit payloads and secrets. The console can show response excerpts and event data to the administrator, so receivers should avoid returning credentials in response bodies.

## Upgrade

For 0.2.2, back up the database and stop **all API and worker processes** before upgrading. The next startup atomically migrates schema 1, 2, 3, or 4 to schema 5, preserving projects, endpoints, events, and attempt history. This migration adds endpoint responsiveness, shared claim accounting, and observed alert-recovery state. Endpoints qualify for responsive capacity through new attempts after migration. Existing 0.1.1/0.2.x sessions remain valid. Destination URLs in pre-0.2 attempt records stay unknown; new attempts record the claimed URL. Existing 0.1.0 browser cookies require a new login. Processes refuse newer unknown schema versions; do not run mixed application versions against the same database. To roll back, stop all processes and restore the pre-upgrade backup with the old application version. Retention remains opt-in. Existing accepted events keep their idempotency semantics; serialized-size limits apply to new acceptance and do not rewrite historical payloads.

## Monitoring and alert delivery

The console's Health & storage page and `relay status` expose queue depth/age, failures and mean attempt latency over the last hour, cooldown counts, disk usage, and alerts. `/metrics` exposes Prometheus gauges and persistent DNS-capacity deferral counters. Set a separate random `RELAY_METRICS_TOKEN` for your collector; it must differ from the administrator token and permits only GET/HEAD requests to `/metrics`. Supply it as a Bearer token through the collector's secret mechanism. Administrator authentication remains supported. Keep the route behind your monitoring network/access gateway. Rotate the metrics token by updating the environment and restarting each API process; it grants no console, event-ingestion, or administrative API access.

Alerts cover old unfinished work at enabled destinations, excessive recent failures, timeout cooldowns, storage pressure, and recent DNS-capacity deferrals. The `resolver_capacity` alert uses instance-wide resource pressure; local deferrals do not inflate the failed-attempt rate. Scrape `relay_delivery_deferrals_total` and `relay_delivery_last_deferral_timestamp_seconds` to track the two fixed reasons (`dns_global_capacity` and `dns_project_capacity`). Paused endpoints contribute to queue age but do not trigger the delay alert. Alerts are persisted and visible without an outbound receiver. To receive notifications, configure both `RELAY_ALERT_WEBHOOK_URL` and `RELAY_ALERT_SIGNING_SECRET`. Generate a secret with:

```sh
python -c "import base64,secrets; print('whsec_' + base64.b64encode(secrets.token_bytes(32)).decode())"
```

The receiver gets a signed Standard Webhooks envelope of type `relay.alert.active` or `relay.alert.resolved`, containing an alert code, severity, message, and timestamps. Verify the raw body and deduplicate by webhook ID. The URL is used directly; services requiring proprietary message formats need an adapter. Neither event payloads nor credentials appear in alerts. Protect the receiver URL if it embeds an access token.

Notifications use separate threads from event delivery, shared database leases, a five-second network deadline, and at most three attempts per notification cycle. Rapid active/resolved transitions for each alert code coalesce to the latest state until its cooldown gate opens; the dashboard updates immediately. Continuing active alerts receive cooldown reminders, and recovery notices can wait for that same gate. Retries of the same notification keep their bounded 30/60-second backoff.

After continuously observed healthy conditions for `RELAY_ALERT_REARM_SECONDS` (default five minutes), a fresh active incident can notify immediately even if a recovery notice was just sent. The healthy interval is persisted across brief restarts. A monitoring gap longer than twice the maintenance interval resets it; Relay does not infer health while monitoring is absent. Migrated alerts establish a new observation interval. This rearming leaves rapid flaps coalesced and continuing-incident reminders on `RELAY_ALERT_COOLDOWN_SECONDS` (default one hour).

Network checks and redirect rejection match ordinary deliveries. Notifications have access to reserved resolver capacity but still share the host and database: use external monitoring for host failure, service downtime, or scrape failures. `/healthz` remains a basic process/database probe and does not promise queue freshness.

## Maintenance configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAY_RETENTION_DAYS` | `0` | Completed history age; 0 disables deletion |
| `RELAY_IDEMPOTENCY_DAYS` | `90` | Key protection after payload deletion; at least 1 day |
| `RELAY_CLEANUP_BATCH_SIZE` | `100` | Per-table deletion bound per pass; 1-1000 |
| `RELAY_MAINTENANCE_INTERVAL_SECONDS` | `30` | Monitoring and ordinary cleanup interval; outstanding cleanup uses shorter catch-up passes |
| `RELAY_MAINTENANCE_ENABLED` | `true` | Run maintenance and notification threads in API/worker processes |
| `RELAY_MIN_FREE_BYTES` | `0` | Optional disk-space reserve for accepting new events |
| `RELAY_MAX_STORAGE_BYTES` | `0` | Optional estimated used SQLite/WAL capacity ceiling |
| `RELAY_STORAGE_WARNING_PERCENT` | `90` | Storage-pressure warning threshold |
| `RELAY_QUEUE_WARNING_AGE_SECONDS` | `300` | Enabled destination queue-delay threshold |
| `RELAY_FAILURE_WARNING_PERCENT` | `25` | Failed attempts in the last hour threshold |
| `RELAY_FAILURE_MIN_SAMPLES` | `10` | Minimum recent attempts before failure-rate alerts |
| `RELAY_ALERT_WEBHOOK_URL` | Unset | Optional operator notification destination |
| `RELAY_ALERT_SIGNING_SECRET` | Unset | Paired `whsec_` secret for the notification receiver |
| `RELAY_ALERT_COOLDOWN_SECONDS` | `3600` | Per-code notification-cycle gate across transitions and active reminders |
| `RELAY_ALERT_REARM_SECONDS` | `300` | Continuously observed healthy time before a new incident can bypass the notification gate; at least 1 second |

Capacity checks are conservative admission estimates, not filesystem quotas: attempt logs, administrative writes, and WAL behavior can still grow storage. A rejected event receives 503 before commit; existing idempotency reads remain available. Monitor free space externally as well. In Compose, these environment variables are forwarded from `.env`.

## Endpoint maintenance and outage recovery

Subscriptions affect new events only. URL edits affect future claims, including retries of older deliveries. In-flight requests finish using their claimed destination. Completed attempts retain the destination used; migrated pre-0.2 attempts display an unknown URL. Update receivers before changing destinations and do not assume edits cancel existing network requests.

Use the console's rotation action to select an overlap window. Project-key rotation without a body stays immediate for older integrations. Only one previous credential is supported; a second rotation replaces it. A zero-second overlap revokes for future claims/authentication, while an already claimed delivery may still use its old signature.

After fixing a receiver, use Bulk recovery to preview a destination/time window, select a requeue pace, and start. The preview freezes at most 1,000 failures. Progress counts released/skipped/cancelled members; delivery outcomes still follow ordinary retry rules. Cancel stops future releases but does not recall already queued work. Delivery workers must be running for jobs to progress.
