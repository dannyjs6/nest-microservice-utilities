import type { InjectionToken, ModuleMetadata, OptionalFactoryDependency } from '@nestjs/common';

export type OutboxStatus = 'pending' | 'processing' | 'published' | 'failed';

/** What application code passes to `OutboxService.enqueue()`. */
export interface OutboxMessageInput<TPayload = unknown> {
  /** Destination: topic / exchange+routing key / queue name. Interpreted by your publisher. */
  topic: string;
  /** Must be JSON-serializable (validated on enqueue). */
  payload: TPayload;
  /**
   * Partition / ordering key, e.g. the aggregate id. Passed to the publisher (Kafka key)
   * and, with `orderByKey`, used to guarantee per-key ordering.
   */
  key?: string | null;
  headers?: Record<string, string>;
  /**
   * Message id. Defaults to a random UUID. Provide a deterministic id
   * (e.g. `order-42-created`) to make enqueueing idempotent: duplicates are ignored.
   * Consumers should also de-duplicate on this id – delivery is AT-LEAST-ONCE.
   */
  id?: string;
  /** Do not publish before this time (delayed messages). */
  availableAt?: Date;
}

/** A message as persisted, ready to be stored. */
export interface NewOutboxMessage {
  id: string;
  topic: string;
  key: string | null;
  payload: unknown;
  headers: Record<string, string>;
  availableAt: Date | null;
}

/** A claimed message handed to the publisher. */
export interface OutboxMessage {
  id: string;
  topic: string;
  key: string | null;
  payload: unknown;
  headers: Record<string, string>;
  /** Number of delivery attempts INCLUDING the current one (1 on first delivery). */
  attempts: number;
  createdAt: Date;
}

/**
 * Persistence for the outbox. Implementations MUST:
 *  - write `add()` inside the caller's transaction when `tx` is given,
 *  - make `claim()` safe for concurrent relays (no message claimed twice while its lease is valid),
 *  - re-offer messages whose lease expired (crashed relay),
 *  - only update messages still leased by `workerId` in markPublished / markFailed / release.
 */
export interface OutboxStore<TTx = unknown> {
  add(messages: NewOutboxMessage[], tx?: TTx): Promise<void>;
  /** Atomically lease up to `limit` due messages. Increments `attempts`. */
  claim(limit: number, leaseMs: number, workerId: string): Promise<OutboxMessage[]>;
  markPublished(ids: string[], workerId: string): Promise<void>;
  /**
   * Record a failed attempt. `retryInMs === null` means give up: status becomes `failed`
   * (a.k.a. dead letter) and it is not retried until requeued.
   */
  markFailed(id: string, workerId: string, error: string, retryInMs: number | null): Promise<void>;
  /** Give the lease back without counting an attempt (e.g. on shutdown). */
  release(ids: string[], workerId: string): Promise<void>;
  /** Move `failed` messages back to `pending` with attempts reset. Returns how many. */
  requeueFailed(ids?: string[]): Promise<number>;
  /** Delete published messages older than `before`. Returns how many. */
  deletePublishedBefore(before: Date, limit: number): Promise<number>;
}

/** Sends a message to the broker. Throw to fail the attempt; throw `NonRetryableError` to dead-letter it. */
export interface OutboxPublisher {
  publish(message: OutboxMessage, signal: AbortSignal): Promise<void>;
}

export interface OutboxRelayOptions {
  /** Start the polling relay in this process. Disable on instances that only enqueue. Default: true. */
  enabled?: boolean;
  /** Delay between polls when the outbox is empty. Default: 1_000. */
  pollIntervalMs?: number;
  /** Messages claimed per poll. Default: 50. */
  batchSize?: number;
  /** Messages published in parallel within a batch. Default: 1 (sequential). */
  concurrency?: number;
  /** How long a claim is valid. Must be > publishTimeoutMs. Default: 30_000. */
  leaseMs?: number;
  /** Timeout for a single `publisher.publish()` call. Default: 10_000. */
  publishTimeoutMs?: number;
  /** Attempts before a message is moved to `failed`. Default: 10. */
  maxAttempts?: number;
  /** Base delay for exponential retry backoff. Default: 1_000. */
  retryBaseDelayMs?: number;
  /** Max delay between retries of one message. Default: 300_000 (5 min). */
  retryMaxDelayMs?: number;
  /** Delete published messages older than this. 0 disables cleanup. Default: 7 days. */
  retainPublishedMs?: number;
  /** How often cleanup runs. Default: 1 hour. */
  cleanupIntervalMs?: number;
  /** Called when a message is moved to `failed` – alert on this. */
  onDeadLetter?: (message: OutboxMessage, error: unknown) => void | Promise<void>;
  /** Identifies this relay in `locked_by`. Default: hostname:pid:random. */
  workerId?: string;
}

export interface OutboxModuleOptions {
  store: OutboxStore<any>;
  publisher: OutboxPublisher;
  relay?: OutboxRelayOptions;
}

export interface OutboxModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  inject?: (InjectionToken | OptionalFactoryDependency)[];
  useFactory: (...args: any[]) => OutboxModuleOptions | Promise<OutboxModuleOptions>;
  /** Register the module globally. Default: false. */
  isGlobal?: boolean;
}
