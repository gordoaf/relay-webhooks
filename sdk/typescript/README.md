# Relay client for JavaScript and TypeScript

Node.js 20 or later; no runtime dependencies or build step. Install from a local
checkout (the package is not published to npm):

```sh
npm install /absolute/path/to/relay-webhooks/sdk/typescript
```

```ts
import { RelayClient, RelayError, RelayEventExpired, verifySignature } from '@relay-webhooks/client';

const relay = new RelayClient({
  baseUrl: process.env.RELAY_URL ?? 'http://127.0.0.1:8787',
  apiKey: process.env.RELAY_PROJECT_KEY!,
});

const receipt = await relay.emit('order.created', { order_id: 'order_1042' }, {
  idempotencyKey: 'order_1042:created',
});
console.log(receipt.event.id);
```

Store the idempotency key with the business operation and reuse it on retries,
including after a restart. A network error can occur after Relay commits the
event. The client retries network errors, invalid success receipts, HTTP 408,
429, and 5xx twice by default, preserving the original bytes and key. Other HTTP
errors, including 409, are returned immediately as `RelayError` with `status`,
`retryable`, and `idempotencyKey`. Do not retry a 409 with a new key: inspect the
conflicting operation. Configure `timeoutMs` (default 10000, per attempt) and
`maxRetries` (0–5). Backoff and numeric `Retry-After` delays are capped at five
seconds. Redirects are not followed. API keys belong only on the server.

Certificate verification failures are nonretryable: fix the certificate or
hostname instead of repeatedly submitting the request. Transport failures retain
their original exception in `error.cause` for diagnosis. No certificate checks
are disabled.

HTTP 410 has a specific meaning: Relay accepted this key and payload earlier,
but retention removed the event's details. It raises `RelayEventExpired`, a
`RelayError` subclass with `accepted: true`, the original `eventId`, and the
original `idempotencyKey`. Treat it as confirmed prior **acceptance**, not proof
that any receiver successfully processed the event. Do not create another key.

```ts
try {
  const receipt = await relay.emit('order.created', { order_id: 'order_1042' }, {
    idempotencyKey: 'order_1042:created',
  });
  // Save receipt.event.id to complete this outbox submission.
} catch (error) {
  if (error instanceof RelayEventExpired) {
    // Save error.eventId as previously accepted; delivery outcome is unknown.
  } else {
    throw error;
  }
}
```

A malformed 410 response does not invent an event ID; it returns a nonretryable
`RelayError` telling the caller to retain the original key for investigation.

Verification must use the exact raw bytes, before JSON parsing:

```ts
const rawBody = Buffer.from(await request.arrayBuffer()); // Web Request example
if (!verifySignature(process.env.RELAY_SIGNING_SECRET!, rawBody, request.headers)) {
  return new Response('Invalid signature', { status: 401 });
}
const event = JSON.parse(rawBody.toString('utf8'));
// Commit a unique event.id and the business effect in the same database transaction.
return new Response(null, { status: 204 });
```

Limit body size at your server before buffering. Verification rejects signatures
more than five minutes old or in the future by default. Keep system clocks
synchronized. During signing-secret rotation it accepts any valid `v1` token in
Relay's signature header. Signature verification does not replace durable
deduplication: retries and manual replays retain the same event ID.
Invalid verification input, including `null` options, returns `false`.

Run tests with `npm test` or `node --test` from this directory. The runnable
[storefront example](../../examples/storefront/README.md) demonstrates the whole
flow with persistent orders, an outbox, and two deduplicating receivers.
