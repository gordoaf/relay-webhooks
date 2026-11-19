# Operations

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `RELAY_ADMIN_TOKEN` | Required | Random administrator token of at least 24 characters |
| `RELAY_DB_PATH` | `data/relay.db` | Local SQLite database |
| `RELAY_PUBLIC_URL` | Unset | Canonical browser-facing HTTP(S) origin, such as `https://relay.example.com` |
| `RELAY_ALLOW_PRIVATE_NETWORKS` | `false` | Permit private destination addresses only in a controlled environment |
| `RELAY_WORKER_ENABLED` | `true` | Run four worker threads with the API |

Normal operation is `relay serve --host 127.0.0.1 --port 8787`. The process exits if its administrator token is missing or too short. The CLI reads environment variables; it does not load `.env` files itself. Docker Compose loads `.env` for interpolation.

## Deployment

Use a TLS reverse proxy for public access. Set `RELAY_PUBLIC_URL` to the exact origin visitors use, including a nonstandard port if applicable. It must not contain credentials, a path other than `/`, a query, or a fragment. Relay uses this origin for CSRF validation and sets Secure session cookies for HTTPS even when the proxy connects to Relay over HTTP. This works with `relay serve` and the supplied Docker Compose file; add the setting to `.env` for Compose.

Keep the backend port private and reachable only by the proxy. Preserve the public Host header when possible. Relay does not trust client-supplied forwarding headers by default. If you also need the original client address for per-IP throttling, configure the proxy to overwrite forwarding headers and launch Uvicorn with an explicit allowlist of the proxy's actual source addresses:

```sh
uvicorn relay.app:create_app --factory --host 127.0.0.1 --port 8787 \
  --proxy-headers --forwarded-allow-ips=127.0.0.1
```

Never use `--forwarded-allow-ips='*'` on a backend reachable by untrusted clients. `RELAY_PUBLIC_URL` does not establish trust in forwarded client addresses.

Keep the console and administrative API behind your private network or access gateway. Allow public access to event ingestion only when required. Add connection limits and request-rate limits at the proxy. The built-in failed-authentication throttle covers login and administrator bearer authentication, is bounded and per process, and does not reject valid credentials because another client used up the failure allowance. It is not distributed request-rate protection. Always generate the administrator token randomly; the length check cannot establish that a manually chosen token is unpredictable.

The application checks credentials before returning a failure-limit response. This prevents anonymous lockout but does **not cap credential comparisons**. Enforce the request ceiling before requests reach Relay. For example, with Nginx receiving client connections directly, declare a zone in the `http` context and apply it inside your existing TLS server:

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
```

Tune the ceiling for expected traffic. Visitors sharing an IP share this budget. If Nginx itself sits behind another proxy, configure trusted client-address handling first. See the [Nginx request-limit reference](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html). Keep the administrative access gateway in place; this snippet is an ingress-limit example, not a complete TLS deployment.

The destination network policy is rechecked on each attempt. Keep private-network mode disabled on an internet-accessible deployment. For trusted internal destinations, enforce an outbound network allowlist in the hosting environment as well.

The Docker image runs as UID 10001 with a persistent `/data` volume. Bind mounts must be writable by that UID. Allow at least 30–45 seconds for graceful shutdown; in-flight work can otherwise resume after its lease expires.

On POSIX systems, Relay creates its data directory with mode 0700 and restricts the database to 0600. It does not make arbitrary existing parent directories private. Restrict existing data directories and backups to the service account, and review their permissions when upgrading. On Windows, apply equivalent filesystem ACLs to the service account; POSIX mode bits do not replace Windows ACLs. SQLite's WAL and shared-memory files also contain sensitive state and must stay inside the protected directory.

## Separate workers

Set `RELAY_WORKER_ENABLED=false` for the API and run `relay worker` with the same database path, administrator configuration, and destination policy. Multiple processes on the same host can claim work safely. All must use a local filesystem and the same configuration. This is not a multi-host database setup.

The scheduler allows one active delivery per endpoint and two per project by default, counted from unexpired leases across processes. It alternates eligible projects and endpoints. Three consecutive attempt timeouts pause new work for that endpoint for 30 seconds; further timeouts renew that cooldown until a non-timeout result resets it. Rate limits still apply. These limits protect other projects during an outage and can reduce peak throughput to a single receiver.

DNS lookups for the same hostname are coalesced and failures have a short bounded cache. Outstanding operating-system lookups remain bounded even after a caller times out. Multiple distinct stalled hostnames can still consume resolver capacity; use a reliable local resolver and outbound network controls for sustained workloads.

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

For 0.2.0, back up the database and stop **all API and worker processes** before upgrading. The next startup atomically migrates schema 1 or 2 to schema 3, preserving projects, endpoints, events, and attempt history. It adds subscriptions, credential overlap, recovery jobs, retention and monitoring state. Existing 0.1.1 sessions remain valid. Destination URLs in old attempt records stay unknown; new attempts record the claimed URL. Existing 0.1.0 browser cookies require a new login. Processes refuse newer unknown schema versions; do not run mixed application versions against the same database. To roll back, stop all processes and restore the pre-upgrade backup with the old application version.

## Monitoring and alert delivery

The console's Health & storage page and `relay status` expose queue depth/age, failures and mean attempt latency over the last hour, cooldown counts, disk usage, and alerts. `/metrics` exposes Prometheus gauges and requires administrator authentication. Keep it behind your monitoring network/access gateway; supply the admin token through your collector's secret mechanism.

Alerts cover old unfinished work at enabled destinations, excessive recent failures, timeout cooldowns, and storage pressure. Paused endpoints contribute to queue age but do not trigger the delay alert. Alerts are persisted and visible without an outbound receiver. To receive notifications, configure both `RELAY_ALERT_WEBHOOK_URL` and `RELAY_ALERT_SIGNING_SECRET`. Generate a secret with:

```sh
python -c "import base64,secrets; print('whsec_' + base64.b64encode(secrets.token_bytes(32)).decode())"
```

The receiver gets a signed Standard Webhooks envelope of type `relay.alert.active` or `relay.alert.resolved`, containing an alert code, severity, message, and timestamps. Verify the raw body and deduplicate by webhook ID. The URL is used directly; services requiring proprietary message formats need an adapter. Neither event payloads nor credentials appear in alerts. Protect the receiver URL if it embeds an access token.

Notifications use separate threads from event delivery, shared database leases, a five-second network deadline, at most three attempts per notification cycle, and cooldown reminders for active alerts. Network checks and redirect rejection match ordinary deliveries. They still share the host, database, and bounded resolver: use external monitoring for host failure, service downtime, or scrape failures. `/healthz` remains a basic process/database probe and does not promise queue freshness.

## Maintenance configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAY_RETENTION_DAYS` | `0` | Completed history age; 0 disables deletion |
| `RELAY_IDEMPOTENCY_DAYS` | `90` | Key protection after payload deletion; at least 1 day |
| `RELAY_CLEANUP_BATCH_SIZE` | `100` | Per-table deletion bound per pass; 1-1000 |
| `RELAY_MAINTENANCE_INTERVAL_SECONDS` | `30` | Maintenance polling interval |
| `RELAY_MAINTENANCE_ENABLED` | `true` | Run maintenance and notification threads in API/worker processes |
| `RELAY_MIN_FREE_BYTES` | `0` | Optional disk-space reserve for accepting new events |
| `RELAY_MAX_STORAGE_BYTES` | `0` | Optional estimated used SQLite/WAL capacity ceiling |
| `RELAY_STORAGE_WARNING_PERCENT` | `90` | Storage-pressure warning threshold |
| `RELAY_QUEUE_WARNING_AGE_SECONDS` | `300` | Enabled destination queue-delay threshold |
| `RELAY_FAILURE_WARNING_PERCENT` | `25` | Failed attempts in the last hour threshold |
| `RELAY_FAILURE_MIN_SAMPLES` | `10` | Minimum recent attempts before failure-rate alerts |
| `RELAY_ALERT_WEBHOOK_URL` | Unset | Optional operator notification destination |
| `RELAY_ALERT_SIGNING_SECRET` | Unset | Paired `whsec_` secret for the notification receiver |
| `RELAY_ALERT_COOLDOWN_SECONDS` | `3600` | Reminder interval for continuing active alerts |

Capacity checks are conservative admission estimates, not filesystem quotas: attempt logs, administrative writes, and WAL behavior can still grow storage. A rejected event receives 503 before commit; existing idempotency reads remain available. Monitor free space externally as well. In Compose, these environment variables are forwarded from `.env`.

## Endpoint maintenance and outage recovery

Subscriptions affect new events only. URL edits affect future claims, including retries of older deliveries. In-flight requests finish using their claimed destination. Completed attempts retain the destination used; migrated pre-0.2 attempts display an unknown URL. Update receivers before changing destinations and do not assume edits cancel existing network requests.

Use the console's rotation action to select an overlap window. Project-key rotation without a body stays immediate for older integrations. Only one previous credential is supported; a second rotation replaces it. A zero-second overlap revokes for future claims/authentication, while an already claimed delivery may still use its old signature.

After fixing a receiver, use Bulk recovery to preview a destination/time window, select a requeue pace, and start. The preview freezes at most 1,000 failures. Progress counts released/skipped/cancelled members; delivery outcomes still follow ordinary retry rules. Cancel stops future releases but does not recall already queued work. Delivery workers must be running for jobs to progress.
