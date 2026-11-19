import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import { RelayClient, RelayError, RelayEventExpired, verifySignature } from '../index.js';

async function server(handler, run) {
  const instance = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    handler(request, response, Buffer.concat(chunks));
  });
  instance.listen(0, '127.0.0.1');
  await once(instance, 'listening');
  try { await run(`http://127.0.0.1:${instance.address().port}`); }
  finally { instance.closeAllConnections(); await new Promise((resolve) => instance.close(resolve)); }
}

test('ambiguous committed request retries with identical body and key', async () => {
  const requests = [];
  await server((request, response, body) => {
    requests.push({ body: body.toString(), key: request.headers['idempotency-key'] });
    if (requests.length === 1) { request.socket.destroy(); return; }
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ created: false, event: { id: 'evt_saved' } }));
  }, async (baseUrl) => {
    const receipt = await new RelayClient({ baseUrl, apiKey: 'project_key' }).emit(
      'order.created', { order_id: 'order_1' }, { idempotencyKey: 'order_1:created' },
    );
    assert.deepEqual(receipt, { created: false, event: { id: 'evt_saved' } });
  });
  assert.equal(requests.length, 2);
  assert.deepEqual(requests[0], requests[1]);
});

test('a conflicting key is never retried or replaced', async () => {
  let calls = 0;
  await server((_, response) => { calls++; response.writeHead(409); response.end('{}'); }, async (baseUrl) => {
    await assert.rejects(new RelayClient({ baseUrl, apiKey: 'project_key' }).emit(
      'order.created', {}, { idempotencyKey: 'persistent-key' },
    ), (error) => error instanceof RelayError && error.status === 409 &&
      !error.retryable && error.idempotencyKey === 'persistent-key');
  });
  assert.equal(calls, 1);
});

test('expired acceptance retains the original event ID without claiming delivery', async () => {
  let calls = 0;
  await server((_, response) => {
    calls++;
    response.writeHead(410);
    response.end('{"event_id":"evt_original","detail":"Previously accepted"}');
  }, async (baseUrl) => {
    await assert.rejects(new RelayClient({ baseUrl, apiKey: 'project_key' }).emit(
      'order.created', {}, { idempotencyKey: 'original_key' },
    ), (error) => error instanceof RelayEventExpired && error.eventId === 'evt_original' &&
      error.accepted === true && error.status === 410 && !error.retryable &&
      error.idempotencyKey === 'original_key' && error.message.includes('do not resubmit'));
  });
  assert.equal(calls, 1);
});

test('malformed expired receipt preserves the key without inventing an event ID', async () => {
  await server((_, response) => {
    response.writeHead(410);
    response.end('{"event_id":null}');
  }, async (baseUrl) => {
    await assert.rejects(new RelayClient({ baseUrl, apiKey: 'project_key' }).emit(
      'order.created', {}, { idempotencyKey: 'original_key' },
    ), (error) => error instanceof RelayError && !(error instanceof RelayEventExpired) &&
      !error.retryable && error.message.includes('retain the original'));
  });
});

test('TLS certificate failures are not retried and retain their cause', async (context) => {
  const original = new TypeError('fetch failed', {
    cause: Object.assign(new Error('certificate expired'), { code: 'CERT_HAS_EXPIRED' }),
  });
  let calls = 0;
  context.mock.method(globalThis, 'fetch', async () => { calls++; throw original; });
  await assert.rejects(new RelayClient({ baseUrl: 'https://relay.example', apiKey: 'project_key' }).emit(
    'order.created', {}, { idempotencyKey: 'key' },
  ), (error) => !error.retryable && error.cause === original &&
    error.message.includes('certificate verification failed'));
  assert.equal(calls, 1);
});

test('transient transport failures retain their cause and retry ambiguity', async (context) => {
  const original = new TypeError('fetch failed', {
    cause: Object.assign(new Error('connection reset'), { code: 'ECONNRESET' }),
  });
  let calls = 0;
  context.mock.method(globalThis, 'fetch', async () => { calls++; throw original; });
  await assert.rejects(new RelayClient({ baseUrl: 'https://relay.example', apiKey: 'project_key', maxRetries: 1 }).emit(
    'order.created', {}, { idempotencyKey: 'key' },
  ), (error) => error.retryable && error.cause === original);
  assert.equal(calls, 2);
});

test('transient HTTP errors retry the same logical event', async () => {
  const keys = [];
  await server((request, response) => {
    keys.push(request.headers['idempotency-key']);
    response.writeHead(keys.length === 1 ? 503 : 202);
    response.end(keys.length === 1 ? '{}' : '{"created":true,"event":{"id":"evt_1"}}');
  }, async (baseUrl) => {
    const receipt = await new RelayClient({ baseUrl, apiKey: 'project_key' }).emit(
      'order.created', {}, { idempotencyKey: 'stable-key' },
    );
    assert.equal(receipt.event.id, 'evt_1');
  });
  assert.deepEqual(keys, ['stable-key', 'stable-key']);
});

test('redirects cannot forward project credentials elsewhere', async () => {
  let calls = 0;
  await server((_, response) => {
    calls++;
    response.writeHead(307, { location: 'http://127.0.0.1:1/steal' });
    response.end();
  }, async (baseUrl) => {
    await assert.rejects(new RelayClient({ baseUrl, apiKey: 'secret_project_key' }).emit(
      'order.created', {}, { idempotencyKey: 'key' },
    ), (error) => error.status === 307 && !String(error).includes('secret_project_key'));
  });
  assert.equal(calls, 1);
});

test('timeout bounds a receiver that never sends a response', async () => {
  await server(() => {}, async (baseUrl) => {
    const start = Date.now();
    await assert.rejects(new RelayClient({ baseUrl, apiKey: 'project_key', timeoutMs: 40, maxRetries: 0 }).emit(
      'order.created', {}, { idempotencyKey: 'key' },
    ), (error) => error instanceof RelayError && error.retryable && error.status === undefined);
    assert.ok(Date.now() - start < 2000);
  });
});

test('reject invalid JSON and missing idempotency key before sending', async () => {
  const client = new RelayClient({ baseUrl: 'http://127.0.0.1:1', apiKey: 'project_key' });
  for (const payload of [{ amount: NaN }, { amount: Infinity }, { a: undefined }, { a: '\ud800' }, []]) {
    await assert.rejects(client.emit('order.created', payload, { idempotencyKey: 'key' }), TypeError);
  }
  await assert.rejects(client.emit('order.created', {}), TypeError);
  assert.ok(!JSON.stringify(client).includes('project_key'));
});

test('verify exact raw bytes, timestamps, and multiple rotation signatures', () => {
  const key = Buffer.alloc(32, 120), next = Buffer.alloc(32, 121);
  const secret = `whsec_${key.toString('base64')}`, nextSecret = `whsec_${next.toString('base64')}`;
  const body = Buffer.from('{"id":"evt_1","data":{"count":1}}');
  const sign = (value) => `v1,${createHmac('sha256', value).update('evt_1.1000.').update(body).digest('base64')}`;
  const headers = { 'Webhook-Id': 'evt_1', 'Webhook-Timestamp': '1000',
    'Webhook-Signature': `${sign(key)} ${sign(next)}` };
  assert.equal(verifySignature(secret, body, headers, { now: 1000 }), true);
  assert.equal(verifySignature(nextSecret, body, headers, { now: 1000 }), true);
  assert.equal(verifySignature(secret, Buffer.concat([body, Buffer.from(' ')]), headers, { now: 1000 }), false);
  assert.equal(verifySignature(secret, body, headers, { now: 1301 }), false);
  assert.equal(verifySignature(secret, body, headers, { now: 699 }), false);
  assert.equal(verifySignature(secret, body, headers, { now: NaN }), false);
  assert.equal(verifySignature(secret, body, { ...headers, 'Webhook-Signature': 'v1,!' }, { now: 1000 }), false);
  assert.equal(verifySignature(secret, body, headers, null), false);
  assert.equal(verifySignature(secret, body, null), false);
  assert.equal(verifySignature(secret, body, headers, []), false);
});
