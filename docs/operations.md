# Operations

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `RELAY_ADMIN_TOKEN` | Required | Random administrator token of at least 24 characters |
| `RELAY_DB_PATH` | `data/relay.db` | Local SQLite database |
| `RELAY_ALLOW_PRIVATE_NETWORKS` | `false` | Permit private destination addresses only in a controlled environment |
| `RELAY_WORKER_ENABLED` | `true` | Run four worker threads with the API |

Normal operation is `relay serve --host 127.0.0.1 --port 8787`. The process exits if its administrator token is missing or too short. The CLI reads environment variables; it does not load `.env` files itself. Docker Compose loads `.env` for interpolation.

## Deployment

Use a TLS reverse proxy for public access. Configure it to overwrite, rather than pass through, client-supplied forwarding headers. The CLI disables proxy header trust by default. For a trusted local proxy, launch through Uvicorn with an explicit proxy allowlist:

```sh
uvicorn relay.app:create_app --factory --host 127.0.0.1 --port 8787 \
  --proxy-headers --forwarded-allow-ips=127.0.0.1
```

Keep the console and administrative API behind your private network or access gateway. Allow public access to event ingestion only when required. Add connection limits and request-rate limits at the proxy; the built-in login throttle is per process and is not a distributed protection.

The destination network policy is rechecked on each attempt. Keep private-network mode disabled on an internet-accessible deployment. For trusted internal destinations, enforce an outbound network allowlist in the hosting environment as well.

The Docker image runs as UID 10001 with a persistent `/data` volume. Bind mounts must be writable by that UID. Allow at least 30–45 seconds for graceful shutdown; in-flight work can otherwise resume after its lease expires.

## Separate workers

Set `RELAY_WORKER_ENABLED=false` for the API and run `relay worker` with the same database path, administrator configuration, and destination policy. Multiple processes on the same host can claim work safely. All must use a local filesystem and the same configuration. This is not a multi-host database setup.

## Backups and retention

Use SQLite's online backup API or stop the service before taking a filesystem backup. Copying only the main database file while WAL writes are active can omit committed data.

```python
import sqlite3

with sqlite3.connect("data/relay.db") as source:
    with sqlite3.connect("relay-backup.db") as target:
        source.backup(target)
```

Backups include event data, receiver response excerpts, and signing secrets. Store them with restricted permissions. Restore into an isolated instance first; pending or expired processing rows can resume delivery when workers start.

There is no automatic retention job in v0.1. Monitor disk growth and free space. Do not delete pending/processing records to reclaim space. A future retention command needs to preserve referential integrity and explicitly define idempotency expiration.

## Diagnose delivery failures

Use the attempt history to distinguish receiver status errors from network failures. Fix the destination, then replay the failed delivery. An enabled receiver returning 2xx completes the delivery; a paused endpoint keeps queued work parked.

The health endpoint checks that the process can read the database. It does not assert queue freshness, receiver health, or a delivery SLA. Check pending/retrying counts and oldest creation times when assessing backlog.

Application logs omit payloads and secrets. The console can show response excerpts and event data to the administrator, so receivers should avoid returning credentials in response bodies.

## Upgrade

Back up the database, stop the service, install the new version, and restart. Version 0.1 creates schema version 1 and refuses to open a newer schema. There are no cross-version migrations yet.
