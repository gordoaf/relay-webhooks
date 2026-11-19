# Relay

[![Tests](https://github.com/gordoaf/relay-webhooks/actions/workflows/ci.yml/badge.svg)](https://github.com/gordoaf/relay-webhooks/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-087b5c)](LICENSE)

Relay is a self-hosted webhook delivery service. Send an event once, inspect every completed delivery attempt, and replay failures after fixing a receiver.

It includes a delivery console, durable retries, event subscriptions, credential rotation, bulk recovery, storage maintenance, and health monitoring. Python and TypeScript clients and a working storefront example demonstrate complete integrations. Python and SQLite run the service; the browser console has no build step.

**[Try the interactive demo](https://gordoaf.github.io/relay-webhooks/)** · [Architecture](docs/architecture.md) · [API](docs/api.md) · [Operations](docs/operations.md)

The public demo simulates deliveries in your browser. The local lab below sends real HTTP requests through the delivery engine.

## Quickstart

Requires Python 3.11 or newer.

```sh
git clone https://github.com/gordoaf/relay-webhooks.git
cd relay-webhooks
python -m venv .venv
```

Activate the environment:

```sh
# macOS / Linux
source .venv/bin/activate
```

```powershell
# Windows PowerShell; activation is optional when using the full executable path.
.\.venv\Scripts\Activate.ps1
```

Then install and run:

```sh
python -m pip install -e .
relay demo
```

Open **http://127.0.0.1:8787** and enter the administrator token printed in the terminal.

1. Click **Set up local lab**. Save the project key if you want to send from your terminal.
2. Click **Receiver online** to take the warehouse offline.
3. Click **Send test event**. Its first attempt gets HTTP 503 and moves to *Retrying*.
4. Bring the receiver back online. The next scheduled attempt gets HTTP 204.
5. Inspect the delivery to see both attempts.

The lab uses `data/demo.db` and a loopback receiver on port 8790. Private destination access is enabled for that local process. Normal server mode blocks private destinations.

## Run your own instance

Generate a random administrator token:

```sh
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Set `RELAY_ADMIN_TOKEN` in your environment, then run `relay serve`. For example:

```sh
export RELAY_ADMIN_TOKEN='replace-with-your-generated-token'
relay serve
```

```powershell
$env:RELAY_ADMIN_TOKEN = 'replace-with-your-generated-token'
relay serve
```

The console lets you create projects and endpoints. Project keys and endpoint signing secrets are shown on creation; ordinary list responses omit them.

### Docker

Copy `.env.example` to `.env`, set a random `RELAY_ADMIN_TOKEN`, then run:

```sh
docker compose up --build -d
```

The default port binding is loopback. The named volume preserves delivery state across container restarts. See [operations](docs/operations.md) before exposing an instance to a network.

## Send an event

Use a **project key**, rather than the administrator token:

```sh
curl -X POST http://127.0.0.1:8787/v1/events \
  -H 'Authorization: Bearer YOUR_PROJECT_KEY' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1042' \
  -d '{"type":"order.created","payload":{"order_id":"order_1042","amount":2400,"currency":"USD"}}'
```

An accepted event returns HTTP 202 after the event and its delivery rows commit together. Repeating the same request with the same idempotency key returns the original event with HTTP 200. Reusing a key for different content returns HTTP 409.

Each matching, enabled endpoint receives a signed JSON envelope:

```json
{
  "id": "evt_...",
  "type": "order.created",
  "created_at": 1795100000.0,
  "data": {"order_id": "order_1042", "amount": 2400, "currency": "USD"}
}
```

Use `relay.delivery.verify_signature` against the **raw request bytes** before parsing the body. See [the example receiver](examples/receiver.py) for signature verification and durable deduplication.

## What this release supports

| Area | Behavior |
| --- | --- |
| Durable acceptance | Event and endpoint fanout commit in one SQLite transaction |
| Recovery | Expiring leases allow another worker to reclaim unfinished work |
| Retries | Six attempts per cycle; exponential backoff with jitter; bounded `Retry-After` support |
| Signing | Standard Webhooks header/signature format with HMAC-SHA256 |
| Isolation | Project keys ingest only into their own project; one instance administrator |
| Destination controls | Editable URLs and rate limits, pause/resume, exact or namespace event subscriptions |
| Credential lifecycle | Project key and signing-secret rotation with an optional overlap up to 24 hours |
| Failure isolation | Shared limits for slow and untested receivers, failing probes, escalating timeout cooldowns, project DNS allowances, and non-attempt capacity backoff |
| Inspection | Search, status filters, pagination, per-attempt destination history, and manual replay |
| Outage recovery | Frozen previews, paced bulk requeue, progress, and cancellation |
| Operations | Queue and resource-pressure metrics, optional read-only Prometheus token, persisted alerts with flapping protection |
| Storage | Opt-in retention with bounded catch-up, post-purge idempotency protection, serialized-payload and storage admission |
| Integration | Python and TypeScript clients, signature helpers, and a local storefront example |
| Network boundaries | Public addresses by default, pinned DNS answers, no redirects, bounded time and response size |
| Installation | Python package from this repository, or Docker Compose |

Delivery is **at least once**. A receiver can process an event and lose its response before Relay records success; the event can then be delivered again. Receivers must deduplicate by event ID. Ordering is not guaranteed.

Version 0.2 targets one host with local persistent storage. It does not provide multi-region availability, enterprise SSO, per-user permissions, or an uptime SLA. The administrator can access every project. Automatic retention is disabled until configured. Enable a policy and monitor disk space for sustained use. See [the design boundaries](docs/architecture.md#boundaries).

## Integrate and operate

Follow the [integration guide](docs/integration.md) for Python and TypeScript clients and a complete storefront with separate warehouse and accounting receivers. Both clients preserve an explicit idempotency key across retries.

In the console, **Endpoints** controls subscriptions and credential rotation; **Bulk recovery** previews failed deliveries before requeueing; **Health & storage** shows queue age, attempt failures, capacity, and alerts. The public browser demo simulates these workflows without sending real traffic.

For an existing installation, retention is opt-in. A typical policy keeps completed history for 30 days and protects purged idempotency keys for another 90 days:

```sh
export RELAY_RETENTION_DAYS=30
export RELAY_IDEMPOTENCY_DAYS=90
relay cleanup          # Preview one bounded batch
relay cleanup --apply  # Apply one batch
relay status           # Queue, storage, policy, and alerts as JSON
```

Set these variables in your service environment (or `.env` for Compose) to apply them to automatic maintenance. See [operations](docs/operations.md) for Windows commands, storage limits, alerts, and upgrade precautions.

## Development

```sh
python -m pip install -e '.[dev]'
python -m pytest -q
```

Tests cover competing claims, stale-lease rejection, restart recovery, atomic fanout, idempotency conflicts, authentication, project isolation, request limits, signature tampering, DNS pinning, redirects, slow receivers, and retry exhaustion. Hardening regressions exercise worker fairness, shared concurrency limits, DNS isolation, revocable sessions, proxy origins, malicious response headers, malformed Unicode, pagination bounds, migrations, and POSIX permissions. CI runs on Windows and Linux and checks the container's health, proxy login, and data permissions.

Upgrading from 0.1.x, 0.2.0, or 0.2.1 requires stopping all API and worker processes before the schema migration. Follow the [upgrade instructions](docs/operations.md#upgrade) and take a database backup first.

Check the full storefront flow against isolated real servers:

```sh
python scripts/check_storefront.py
```

For browser workflow checks, install `python -m pip install -e '.[ui]'`, run `python -m playwright install chromium`, then `python scripts/check_console.py`. These checks cover the live console and browser simulation, including mobile layout. The TypeScript client has a dependency-free suite: run `node --test` from `sdk/typescript`.

Run the reproducible local delivery benchmark:

```sh
python examples/benchmark.py --events 200 --workers 4
```

[Recorded results and methodology](docs/benchmark.md). These measurements describe a local workload, not a hosted service capacity guarantee.

## License and contact

MIT licensed. Use it, modify it, and run it on your own infrastructure.

For bugs and feature requests, [open an issue](https://github.com/gordoaf/relay-webhooks/issues). For integration and development inquiries, visit [GordoAF's GitHub profile](https://github.com/gordoaf).
