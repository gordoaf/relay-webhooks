# API

The service defaults to `http://127.0.0.1:8787`. JSON request bodies are limited to 256 KiB, including chunked requests. Authenticated OpenAPI output is available at `GET /api/openapi.json`.

## Authentication

- Administrative endpoints accept `Authorization: Bearer <RELAY_ADMIN_TOKEN>`.
- The console exchanges the administrator token at `POST /api/session` for a signed, eight-hour HttpOnly SameSite=Strict cookie backed by a hashed database session record. Cookies use Secure when `RELAY_PUBLIC_URL` is HTTPS, or when the request scheme is HTTPS if that setting is absent. Logout revokes the session across processes, including copied cookies; changing the administrator token also invalidates existing signatures.
- Cookie-authenticated mutations require a matching Origin or `X-Relay-Request: 1`. A mismatched Origin is rejected even with that header.
- Event ingestion accepts a project API key, which cannot access administrative endpoints.
- Repeated failed administrator authentication receives HTTP 429 with `Retry-After`. Successful logins do not consume the failure allowance, and valid administrator credentials remain usable after other failed attempts. Credentials are checked before that response, so it does not cap comparisons or prevent guessing on its own. Enforce the hard request ceiling at the ingress proxy and generate an unpredictable administrator token.

## Event ingestion

`POST /v1/events`

```json
{"type":"order.created","payload":{"order_id":"order_1042"}}
```

Include an optional `Idempotency-Key` header of 1–200 visible ASCII characters. Types start with a letter and contain letters, digits, underscores, periods, colons, or hyphens, up to 128 characters. Payloads must be JSON objects with finite numbers and valid Unicode. Malformed payloads receive HTTP 422; HTTP 409 is reserved for conflicting idempotency-key reuse.

The response has `event` and `created` fields. The event includes `id`, `project_id`, `event_type`, `payload`, `created_at`, and `delivery_count`. Timestamps are Unix seconds. HTTP 202 means newly committed; HTTP 200 means an identical idempotent replay; HTTP 409 means conflicting key reuse.

## Administrative endpoints

| Method | Path | Body or purpose |
| --- | --- | --- |
| GET | `/healthz` | Unauthenticated process/database health |
| POST | `/api/session` | `{"token":"..."}` |
| GET / DELETE | `/api/session` | Inspect or end the browser session |
| GET / POST | `/api/projects` | List, or create with `{"name":"Production"}` |
| POST | `/api/projects/{project}/rotate-key` | Rotate a project key; optional `overlap_seconds` (0-86400), no body defaults to immediate revocation |
| GET / POST | `/api/projects/{project}/endpoints` | List, or create with `url`, optional `description`, `rate_limit`, and `subscriptions` |
| PATCH | `/api/projects/{project}/endpoints/{endpoint}` | Any of `enabled`, `url`, `description`, `rate_limit`, `subscriptions` |
| POST | `/api/projects/{project}/events` | Submit an event from the console with administrator access |
| GET | `/api/projects/{project}/deliveries` | `status`, `search`, `limit` (1–200), `offset` |
| GET | `/api/projects/{project}/deliveries/{delivery}` | Event data and completed attempt history |
| POST | `/api/projects/{project}/deliveries/{delivery}/replay` | Queue another cycle; HTTP 409 for an active lease |
| GET | `/api/projects/{project}/stats` | Counts, percentage delivered, mean final successful attempt duration |

Project creation returns a one-time `api_key`. Endpoint creation returns a one-time `signing_secret`. List/detail responses do not return either secret. Project key rotation without a body is immediate for compatibility. A body such as `{"overlap_seconds":3600}` temporarily accepts the previous key. Rotation returns the new credential and `previous_valid_until` (or null for immediate revocation). At most one previous credential is kept; rotating again replaces the overlap.

Delivery statuses are `pending`, `processing`, `retrying`, `delivered`, and `failed`. Search matches delivery ID, event ID, event type, destination URL, and last error. List responses contain `items` and `total`.

`offset` must be between 0 and 9,223,372,036,854,775,807 inclusive. Out-of-range pagination is rejected with HTTP 422 before querying SQLite.

## Local lab endpoints

These require administrator access and only exist operationally when running `relay demo`:

- `POST /api/demo/setup`: create the runtime's demo project and warehouse endpoint.
- `GET /api/demo/receiver`: read receiver availability and unique event count.
- `POST /api/demo/receiver` with `{"online":false}`: simulate a temporary receiver failure.
- `POST /api/demo/send` with `{"project_id":"...","count":1}`: enqueue sample orders.

The lab receiver's deduplication is in memory and is reset with the process. Use the [durable example receiver](../examples/receiver.py) when exploring receiver-side transactions.

## Subscriptions and signing-secret rotation

Endpoint creation accepts `"subscriptions":["order.created","invoice.*"]`. Omitted subscriptions default to `["*"]`. An empty list receives no new events. Patterns are exact event types or a trailing namespace wildcard; at most 100 patterns, each at most 128 characters. Changes affect only future fanout. Pausing still excludes new events and parks existing work.

Endpoint PATCH accepts one or more non-null editable fields. URL changes undergo the same public-address validation as creation and are checked again on delivery. Each future claim uses the current URL; in-flight claims retain their original destination. Attempt details include `endpoint_url`; null identifies legacy history whose destination cannot be established.

`POST /api/projects/{project}/endpoints/{endpoint}/rotate-secret`

```json
{"overlap_seconds":3600}
```

The default is one hour; 0 revokes immediately, maximum 86400 seconds. The response contains a one-time `signing_secret` and `previous_valid_until`. During overlap, both signatures are sent in the Standard Webhooks header so either receiver secret can verify the message. Update the receiver before the overlap expires. An already claimed request can still carry its claim-time credentials.

## Bulk recovery

1. `POST /api/projects/{project}/recoveries/preview` with optional `endpoint_id`, `since`, `until`, and `limit` (1-1000). Time filters are inclusive Unix seconds of the **last failure**, not event creation. The result includes `preview_id`, `total`, `selected_count`, `expires_at`, `sample`, and `filters`. Membership is frozen for 15 minutes.
2. `POST /api/projects/{project}/recoveries` with `{"preview_id":"rpv_...","pace_per_second":10}`. Pace is an integer from 1 to 1000 and is a ceiling, not a throughput guarantee. The same preview and pace return the same job on repeat. Changed or no-longer-failed members are skipped.
3. `GET /api/projects/{project}/recoveries` returns the latest 50 jobs in `recoveries`. `GET /api/projects/{project}/recoveries/{job}` returns progress: `queued`, `requeued`, `skipped`, `cancelled`, `total`, and current `outcomes` of released deliveries.
4. `POST /api/projects/{project}/recoveries/{job}/cancel` cancels remaining members. Already released deliveries continue through normal scheduling.

Job statuses are `running`, `completed`, and `cancelled`. Completion describes requeue progress; check delivery outcomes for receiver acknowledgements. Replays retain event IDs and completed attempts. There are at most 10 running jobs per project and 100 per instance. Unused previews are limited to 10 per project; a fresh preview is required after expiration. Completed jobs are retained for 30 days.

## Monitoring and storage

All of these routes require administrator authentication:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/operations` | Instance queue, recent attempt outcomes, endpoint cooldowns, storage, policy, and alerts |
| GET | `/api/projects/{project}/metrics` | Project queue/outcomes/endpoints plus instance storage/policy/alerts |
| GET | `/metrics` | Prometheus text format, fixed low-cardinality labels |
| POST | `/api/maintenance/cleanup` | `{"dry_run":true}` previews; false applies one bounded batch |

Attempt metrics use the last hour; queue counts describe current retained state. Cleanup policy is supplied through server configuration, not the request. The response reports eligible events in the next bounded scan (not the entire history), `scanned_events`, `scan_limit`, deleted event/delivery/attempt counts, and `has_more`. Because cleanup is incremental and automatic maintenance may run concurrently, a preview is informative rather than a reserved deletion plan.

Configured capacity limits reject a new event with HTTP **503** and `Retry-After: 30` before acceptance. After retention purges a payload, an identical request using its protected key returns HTTP **410** with its original `event_id`; changed content still returns **409**. Do not automatically invent another key on 410: the original event was already accepted. After the configured post-purge idempotency window expires, the key may be used again.
