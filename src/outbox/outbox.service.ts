import { Inject, Injectable, Optional } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { OUTBOX_OPTIONS } from './outbox.constants';
import type { NewOutboxMessage, OutboxMessageInput, OutboxModuleOptions, OutboxStore } from './outbox.interfaces';
import { OutboxRelay } from './outbox.relay';

/**
 * Application-facing API of the outbox.
 *
 * @example
 * await dataSource.transaction(async (manager) => {
 *   await manager.save(order);
 *   await outbox.enqueue({ topic: 'orders.created', key: order.id, payload: order }, manager);
 * });
 * outbox.wake(); // optional: publish now instead of on the next poll
 */
@Injectable()
export class OutboxService {
  private readonly store: OutboxStore;

  constructor(
    @Inject(OUTBOX_OPTIONS) options: OutboxModuleOptions,
    @Optional() private readonly relay?: OutboxRelay,
  ) {
    this.store = options.store;
  }

  /**
   * Stores message(s) to be published. Pass your transaction handle as `tx` so the
   * messages are committed (or rolled back) atomically with your business data –
   * that is the whole point of the outbox. Returns the message ids.
   */
  async enqueue<TTx = unknown>(
    messages: OutboxMessageInput | OutboxMessageInput[],
    tx?: TTx,
  ): Promise<string[]> {
    const list = Array.isArray(messages) ? messages : [messages];
    const prepared = list.map(prepare);
    const ids = new Set<string>();
    for (const m of prepared) {
      if (ids.has(m.id)) throw new Error(`outbox: duplicate message id "${m.id}" in one enqueue call`);
      ids.add(m.id);
    }
    await this.store.add(prepared, tx);
    return prepared.map((m) => m.id);
  }

  /** Nudges the local relay to poll immediately. Call AFTER your transaction commits. */
  wake(): void {
    this.relay?.wake();
  }

  /** Puts dead-lettered messages back in the queue (all of them when `ids` is omitted). */
  requeueFailed(ids?: string[]): Promise<number> {
    return this.store.requeueFailed(ids);
  }
}

function prepare(input: OutboxMessageInput): NewOutboxMessage {
  if (!input || typeof input.topic !== 'string' || input.topic.trim() === '') {
    throw new TypeError('outbox: message.topic must be a non-empty string');
  }
  if (input.id !== undefined && (typeof input.id !== 'string' || input.id === '')) {
    throw new TypeError('outbox: message.id must be a non-empty string');
  }
  if (input.availableAt !== undefined && Number.isNaN(input.availableAt?.getTime?.())) {
    throw new TypeError('outbox: message.availableAt must be a valid Date');
  }
  return {
    id: input.id ?? randomUUID(),
    topic: input.topic,
    key: input.key ?? null,
    payload: assertSerializable(input.payload, 'payload'),
    headers: validateHeaders(input.headers),
    availableAt: input.availableAt ?? null,
  };
}

/**
 * Fail fast in the request (inside the transaction) instead of later in the relay:
 * catches circular structures, BigInt, and values JSON silently drops (undefined, functions).
 * Returns a normalized clone so later mutations by the caller can't change the message.
 */
function assertSerializable(value: unknown, field: string): unknown {
  let json: string | undefined;
  try {
    json = JSON.stringify(value);
  } catch (error) {
    throw new TypeError(`outbox: ${field} is not JSON-serializable: ${(error as Error).message}`);
  }
  if (json === undefined) {
    throw new TypeError(`outbox: ${field} is not JSON-serializable (got ${typeof value})`);
  }
  return JSON.parse(json);
}

function validateHeaders(headers: Record<string, string> | undefined): Record<string, string> {
  if (headers === undefined || headers === null) return {};
  if (typeof headers !== 'object' || Array.isArray(headers)) {
    throw new TypeError('outbox: headers must be a plain object of strings');
  }
  for (const [k, v] of Object.entries(headers)) {
    if (typeof v !== 'string') throw new TypeError(`outbox: header "${k}" must be a string`);
  }
  return { ...headers };
}
