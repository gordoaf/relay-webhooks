import { createHmac, timingSafeEqual } from "node:crypto";

export class RelayError extends Error {
  constructor(message, { idempotencyKey, status, retryable = false }) {
    super(message);
    this.name = "RelayError";
    this.idempotencyKey = idempotencyKey;
    this.status = status;
    this.retryable = retryable;
  }
}

function validJSON(value, parents = new Set()) {
  if (value === null || typeof value === "boolean") return true;
  if (typeof value === "string") {
    // JSON can represent lone surrogates, but Relay requires valid Unicode.
    for (let i = 0; i < value.length; i++) {
      const c = value.charCodeAt(i);
      if (c >= 0xd800 && c <= 0xdbff) {
        const next = value.charCodeAt(++i);
        if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      } else if (c >= 0xdc00 && c <= 0xdfff) return false;
    }
    return true;
  }
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "object" || parents.has(value)) return false;
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null) return false;
  parents.add(value);
  const entries = Array.isArray(value) ? Array.from(value.entries()) : Object.entries(value);
  const result = entries.every(([key, child]) => validJSON(String(key)) && validJSON(child, parents));
  parents.delete(value);
  return result;
}

export class RelayClient {
  constructor({ baseUrl, apiKey, timeoutMs = 10000, maxRetries = 2 }) {
    const url = new URL(baseUrl);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password ||
        url.search || url.hash || url.pathname !== "/") {
      throw new TypeError("baseUrl must be an HTTP(S) origin without credentials or a path");
    }
    if (typeof apiKey !== "string" || !/^[\x21-\x7e]+$/.test(apiKey)) {
      throw new TypeError("apiKey must be a nonempty ASCII token");
    }
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 120000) {
      throw new TypeError("timeoutMs must be an integer between 1 and 120000");
    }
    if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 5) {
      throw new TypeError("maxRetries must be an integer between 0 and 5");
    }
    this.baseUrl = url.origin;
    this.timeoutMs = timeoutMs;
    this.maxRetries = maxRetries;
    Object.defineProperty(this, "apiKey", { value: apiKey, enumerable: false });
  }

  async emit(type, payload, { idempotencyKey } = {}) {
    if (typeof type !== "string" || !/^[A-Za-z][A-Za-z0-9_.:-]{0,127}$/.test(type)) {
      throw new TypeError("Invalid event type");
    }
    if (typeof idempotencyKey !== "string" || !/^[\x21-\x7e]{1,200}$/.test(idempotencyKey)) {
      throw new TypeError("idempotencyKey must contain 1-200 visible ASCII characters");
    }
    if (!payload || Array.isArray(payload) || typeof payload !== "object" || !validJSON(payload)) {
      throw new TypeError("payload must be a JSON object with finite numbers and valid Unicode");
    }
    const body = JSON.stringify({ type, payload });
    if (Buffer.byteLength(body) > 256 * 1024) throw new TypeError("Event request exceeds 256 KiB");
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      let error, retryAfter;
      try {
        const response = await fetch(`${this.baseUrl}/v1/events`, {
          method: "POST", redirect: "manual", signal: AbortSignal.timeout(this.timeoutMs),
          headers: {
            authorization: `Bearer ${this.apiKey}`, "content-type": "application/json",
            "idempotency-key": idempotencyKey, "user-agent": "relay-node-client/0.2",
          },
          body,
        });
        retryAfter = response.headers.get("retry-after");
        if (response.status === 200 || response.status === 202) {
          try {
            const chunks = [];
            let size = 0;
            if (!response.body) throw new Error("Empty receipt");
            for await (const chunk of response.body) {
              size += chunk.length;
              if (size > 1024 * 1024) throw new Error("Receipt exceeds 1 MiB");
              chunks.push(chunk);
            }
            const receipt = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            if (!receipt || typeof receipt.created !== "boolean" ||
                !receipt.event || typeof receipt.event.id !== "string") throw new Error("Invalid receipt");
            return receipt;
          } catch {
            error = new RelayError("Relay returned an invalid receipt; reuse the same idempotency key", {
              idempotencyKey, status: response.status, retryable: true,
            });
          }
        } else {
          await response.body?.cancel();
          error = new RelayError(`Relay rejected the event (HTTP ${response.status})`, {
            idempotencyKey, status: response.status,
            retryable: [408, 429].includes(response.status) || (response.status >= 500 && response.status <= 599),
          });
        }
      } catch {
        error = new RelayError("Relay could not confirm acceptance; reuse the same idempotency key", {
          idempotencyKey, retryable: true,
        });
      }
      if (!error.retryable || attempt === this.maxRetries) throw error;
      let delay = Math.min(5000, 250 * 2 ** attempt);
      if (retryAfter && /^[0-9]+$/.test(retryAfter)) {
        delay = Math.min(5000, Math.max(delay, Number(retryAfter.slice(0, 8)) * 1000));
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

function decodeBase64(value) {
  if (typeof value !== "string" || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new TypeError("Invalid base64");
  }
  return Buffer.from(value, "base64");
}

/** Verify the exact bytes received, before parsing JSON. Returns false on invalid input. */
export function verifySignature(secret, rawBody, headers, { toleranceSeconds = 300, now = Date.now() / 1000 } = {}) {
  try {
    if (!(rawBody instanceof Uint8Array) || !Number.isFinite(toleranceSeconds) ||
        toleranceSeconds < 0 || !Number.isFinite(now)) return false;
    const values = headers instanceof Headers ? Object.fromEntries(headers) :
      Object.fromEntries(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]));
    const id = values["webhook-id"], stamp = values["webhook-timestamp"];
    if (typeof id !== "string" || !id || typeof stamp !== "string" || !/^[0-9]+$/.test(stamp) ||
        !Number.isSafeInteger(Number(stamp)) || Math.abs(now - Number(stamp)) > toleranceSeconds) return false;
    if (typeof secret !== "string" || !secret.startsWith("whsec_")) return false;
    const key = decodeBase64(secret.slice(6));
    if (key.length < 16) return false;
    const expected = createHmac("sha256", key).update(`${id}.${stamp}.`).update(rawBody).digest();
    let matched = false;
    for (const candidate of values["webhook-signature"].split(/\s+/)) {
      if (!candidate.startsWith("v1,")) continue;
      try {
        const supplied = decodeBase64(candidate.slice(3));
        if (supplied.length === expected.length) matched = timingSafeEqual(expected, supplied) || matched;
      } catch { /* Other signatures may still match during rotation. */ }
    }
    return matched;
  } catch {
    return false;
  }
}
