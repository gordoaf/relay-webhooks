# Integrating an application

Relay runs on your infrastructure. Your application submits events with a
project key, and Relay delivers signed requests to your subscribers. The Python
and Node clients below submit events; the receiver helpers verify deliveries.

For a complete, runnable integration, follow the
[storefront walkthrough](../examples/storefront/README.md). It includes a local
checkout, a durable outbox, independent warehouse and accounting subscriptions,
outage switches, and persistent receiver deduplication.

## Python

Install Relay from this checkout with `python -m pip install -e .`. The client
itself uses only the Python standard library and ships in the Relay package.

```python
import os
from relay.client import RelayClient, RelayError, RelayEventExpired

relay = RelayClient(
    os.environ.get("RELAY_URL", "http://127.0.0.1:8787"),
    os.environ["RELAY_PROJECT_KEY"],
)
receipt = relay.emit(
    "order.created",
    {"order_id": "order_1042", "total_cents": 3600},
    idempotency_key="order_1042:created",
)
print(receipt["event"]["id"])
```

Use a stable key identifying the business operation. Save it with that operation
and reuse it after retries or process restarts. The required explicit key avoids
silently creating a new event when an earlier request may already have committed.
The example storefront demonstrates an outbox that saves the order and its
pending events in the same transaction.

The Python client retries transient network errors, invalid success receipts, HTTP 408,
429, and 5xx twice by default. It preserves the serialized request and key.
Certificate-verification failures are nonretryable and preserve a diagnostic cause;
fix the certificate or trust configuration. Other errors, including a 409 conflict,
are returned immediately as `RelayError`. HTTP 410 has a distinct subtype:

```python
try:
    receipt = relay.emit("order.created", {"order_id": "order_1042", "total_cents": 3600},
                         idempotency_key="order_1042:created")
except RelayEventExpired as exc:
    # Relay accepted this event earlier, but its retained details have expired.
    # Record acceptance with exc.event_id; this does not prove delivery success.
    print("Previously accepted:", exc.event_id)
except RelayError as exc:
    # exc.status is None when no HTTP receipt was available.
    # Retain exc.idempotency_key; an uncertain response can still mean committed.
    print(exc.status, exc.retryable, exc.idempotency_key)
```

Configure `timeout` (default 10 seconds, at most 120) and `max_retries` (0–5).
The timeout bounds connection/socket operations and a response-body deadline;
an operating-system DNS lookup can outlast a socket timeout. Backoff and numeric
`Retry-After` delays are capped at five seconds. The synchronous client blocks
its calling thread; call it from a job/outbox worker or `asyncio.to_thread` in an
asynchronous application. It does not follow redirects or read proxy settings.

## JavaScript and TypeScript

The dependency-free ESM client supports Node.js 20 or later and includes type
declarations. Install it from this repository; it is not published to npm:

```sh
npm install /absolute/path/to/relay-webhooks/sdk/typescript
```

```ts
import { RelayClient } from '@relay-webhooks/client';

const relay = new RelayClient({
  baseUrl: process.env.RELAY_URL ?? 'http://127.0.0.1:8787',
  apiKey: process.env.RELAY_PROJECT_KEY!,
});
const receipt = await relay.emit('order.created', { order_id: 'order_1042' }, {
  idempotencyKey: 'order_1042:created',
});
console.log(receipt.event.id);
```

Configure `timeoutMs` (default 10000, per attempt including the response body)
and `maxRetries` (default 2). Retry and idempotency behavior match the Python
client. The client is for server-side Node applications; never embed a project
key in browser JavaScript. See the [package guide](../sdk/typescript/README.md)
for raw-body verification and errors.

## Receiving safely

Verify the exact raw request bytes before parsing JSON. Python:

```python
from relay.client import verify_signature

if not verify_signature(signing_secret, raw_body_bytes, request_headers):
    # Return HTTP 401 before applying any business effect.
    ...
```

Both verification helpers check the timestamp (five-minute tolerance by
default), use constant-time digest comparison, and accept matching signatures
from a rotation overlap. Keep receiver clocks synchronized. For a complete
Python HTTP receiver, see [receiver.py](../examples/receiver.py).

Relay delivers an envelope with `id`, `type`, `created_at`, and `data`. Store a
unique event ID and the business effect in the **same database transaction**.
Return 2xx only after that transaction commits. Retrying and replaying an event
retain its ID; signature verification alone does not prevent duplicate work.
For effects in another service, use that service's idempotency mechanism or a
second outbox rather than assuming two separate database commits are atomic.

Idempotency at ingestion and receiver deduplication solve different problems:
the former prevents repeated submission from creating multiple Relay events;
the latter prevents repeated delivery from applying the same effect twice.
Retention limits how long Relay remembers submitted keys. Configure retention
to cover the maximum age of outbox retries and maintain longer business-level
deduplication where necessary; the client does not extend the server's window.

Both clients preserve expired acceptance as `RelayEventExpired`, with `event_id`
in Python or `eventId` in JavaScript and `accepted` set to true. Keep the original
key and event ID; do not turn this response into a new submission with another key.
The storefront records this acceptance separately from receiver fulfillment.
Its recovery controls let you update credentials and explicitly resume blocked
outbox entries while preserving their original keys. Credential updates require
explicit field selectors such as `--warehouse-secret-env RELAY_WAREHOUSE_SIGNING_SECRET`;
unselected environment variables are ignored. See the walkthrough for combined
updates and project-key rotation.
