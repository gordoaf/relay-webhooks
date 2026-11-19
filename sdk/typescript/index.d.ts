export type JSONValue = string | number | boolean | null | JSONValue[] | { [key: string]: JSONValue };
export interface EventReceipt {
  created: boolean;
  event: {
    id: string;
    project_id: string;
    event_type: string;
    payload: { [key: string]: JSONValue };
    created_at: number;
    delivery_count: number;
  };
}
export class RelayError extends Error {
  idempotencyKey: string;
  status: number | undefined;
  retryable: boolean;
  cause: unknown;
  constructor(message: string, options: { idempotencyKey: string; status?: number; retryable?: boolean; cause?: unknown });
}
/** Previously accepted by Relay; this does not establish successful delivery. */
export class RelayEventExpired extends RelayError {
  accepted: true;
  eventId: string;
  constructor(options: { idempotencyKey: string; eventId: string });
}
export class RelayClient {
  constructor(options: { baseUrl: string; apiKey: string; timeoutMs?: number; maxRetries?: number });
  emit(type: string, payload: { [key: string]: JSONValue }, options: { idempotencyKey: string }): Promise<EventReceipt>;
}
export function verifySignature(
  secret: string,
  rawBody: Uint8Array,
  headers: Headers | Record<string, string | string[] | undefined>,
  options?: { toleranceSeconds?: number; now?: number },
): boolean;
