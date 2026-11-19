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
| POST | `/api/projects/{project}/rotate-key` | Immediately revoke the previous project key and return a new one |
| GET / POST | `/api/projects/{project}/endpoints` | List, or create with `url`, optional `description`, and `rate_limit` |
| PATCH | `/api/projects/{project}/endpoints/{endpoint}` | `{"enabled":false}` or `true` |
| POST | `/api/projects/{project}/events` | Submit an event from the console with administrator access |
| GET | `/api/projects/{project}/deliveries` | `status`, `search`, `limit` (1–200), `offset` |
| GET | `/api/projects/{project}/deliveries/{delivery}` | Event data and completed attempt history |
| POST | `/api/projects/{project}/deliveries/{delivery}/replay` | Queue another cycle; HTTP 409 for an active lease |
| GET | `/api/projects/{project}/stats` | Counts, percentage delivered, mean final successful attempt duration |

Project creation returns a one-time `api_key`. Endpoint creation returns a one-time `signing_secret`. List/detail responses do not return either secret. Project key rotation is immediate; it does not support a dual-key overlap period.

Delivery statuses are `pending`, `processing`, `retrying`, `delivered`, and `failed`. Search matches delivery ID, event ID, event type, destination URL, and last error. List responses contain `items` and `total`.

`offset` must be between 0 and 9,223,372,036,854,775,807 inclusive. Out-of-range pagination is rejected with HTTP 422 before querying SQLite.

## Local lab endpoints

These require administrator access and only exist operationally when running `relay demo`:

- `POST /api/demo/setup`: create the runtime's demo project and warehouse endpoint.
- `GET /api/demo/receiver`: read receiver availability and unique event count.
- `POST /api/demo/receiver` with `{"online":false}`: simulate a temporary receiver failure.
- `POST /api/demo/send` with `{"project_id":"...","count":1}`: enqueue sample orders.

The lab receiver's deduplication is in memory and is reset with the process. Use the [durable example receiver](../examples/receiver.py) when exploring receiver-side transactions.
