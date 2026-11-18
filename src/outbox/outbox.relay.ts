import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { errorMessage, isNonRetryable } from '../common/errors';
import { computeDelay } from '../resilience/retry';
import { withTimeout } from '../resilience/timeout';
import { OUTBOX_OPTIONS } from './outbox.constants';
import type {
  OutboxMessage,
  OutboxModuleOptions,
  OutboxPublisher,
  OutboxRelayOptions,
  OutboxStore,
} from './outbox.interfaces';

type ResolvedRelayOptions = Required<Omit<OutboxRelayOptions, 'onDeadLetter'>> &
  Pick<OutboxRelayOptions, 'onDeadLetter'>;

const MAX_ERROR_BACKOFF_MS = 30_000;
const CLEANUP_CHUNK = 1_000;

export function resolveRelayOptions(options: OutboxRelayOptions = {}): ResolvedRelayOptions {
  const o: ResolvedRelayOptions = {
    enabled: options.enabled ?? true,
    pollIntervalMs: options.pollIntervalMs ?? 1_000,
    batchSize: options.batchSize ?? 50,
    concurrency: options.concurrency ?? 1,
    leaseMs: options.leaseMs ?? 30_000,
    publishTimeoutMs: options.publishTimeoutMs ?? 10_000,
    maxAttempts: options.maxAttempts ?? 10,
    retryBaseDelayMs: options.retryBaseDelayMs ?? 1_000,
    retryMaxDelayMs: options.retryMaxDelayMs ?? 300_000,
    retainPublishedMs: options.retainPublishedMs ?? 7 * 24 * 3_600_000,
    cleanupIntervalMs: options.cleanupIntervalMs ?? 3_600_000,
    workerId: options.workerId ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`,
    onDeadLetter: options.onDeadLetter,
  };
  for (const key of ['batchSize', 'concurrency', 'maxAttempts'] as const) {
    if (!Number.isInteger(o[key]) || o[key] < 1) throw new RangeError(`outbox: ${key} must be an integer >= 1`);
  }
  if (o.publishTimeoutMs <= 0 || o.pollIntervalMs <= 0) {
    throw new RangeError('outbox: publishTimeoutMs and pollIntervalMs must be > 0');
  }
  // Otherwise a slow publish outlives its lease and another relay publishes it too.
  if (o.leaseMs <= o.publishTimeoutMs) {
    throw new RangeError('outbox: leaseMs must be greater than publishTimeoutMs');
  }
  return o;
}

/**
 * Polls the outbox store and hands messages to the publisher.
 *
 * Guarantees / edge cases:
 *  - at-least-once delivery: a crash between publish and markPublished re-delivers after the lease expires,
 *  - safe to run on N instances at once (store-level leasing),
 *  - lease-aware: stops publishing from a batch whose lease is about to expire and hands the rest back,
 *  - poison messages: exponential backoff, then `failed` after `maxAttempts` (or immediately on NonRetryableError),
 *  - graceful shutdown: finishes in-flight publishes, releases unstarted ones, never starts a new poll,
 *  - DB outages: the loop survives errors with capped exponential backoff.
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelay.name);
  private readonly store: OutboxStore;
  private readonly publisher: OutboxPublisher;
  readonly options: ResolvedRelayOptions;

  private running = false;
  private stopping = false;
  private loopPromise?: Promise<void>;
  private wakeUp?: () => void;
  private wakeRequested = false;
  private batchChain: Promise<unknown> = Promise.resolve();
  private nextCleanupAt = 0;

  constructor(@Inject(OUTBOX_OPTIONS) moduleOptions: OutboxModuleOptions) {
    this.store = moduleOptions.store;
    this.publisher = moduleOptions.publisher;
    this.options = resolveRelayOptions(moduleOptions.relay);
  }

  get isRunning(): boolean {
    return this.running;
  }

  onApplicationBootstrap(): void {
    if (this.options.enabled) this.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.stopping = false;
    this.nextCleanupAt = Date.now() + this.options.cleanupIntervalMs;
    this.logger.log(`Outbox relay started (worker ${this.options.workerId})`);
    this.loopPromise = this.loop();
  }

  /** Stops polling and waits for the in-flight batch to finish. Idempotent. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.stopping = true;
    this.wakeUp?.();
    await this.loopPromise;
    await this.batchChain.catch(() => undefined);
    this.stopping = false;
    this.logger.log('Outbox relay stopped');
  }

  /**
   * Skip the remaining poll interval and look for messages now. Call it after the
   * transaction that enqueued messages COMMITS, to get near-zero latency.
   */
  wake(): void {
    this.wakeRequested = true;
    this.wakeUp?.();
  }

  /**
   * Claims and processes one batch. Returns how many messages were claimed.
   * Used by the polling loop; also handy for tests or for driving the outbox from
   * your own scheduler with `relay.enabled = false`. Calls are serialized.
   */
  processBatch(): Promise<number> {
    const run = this.batchChain.catch(() => undefined).then(() => this.doProcessBatch());
    this.batchChain = run;
    return run;
  }

  /** Deletes published messages older than `retainPublishedMs`. Returns how many. */
  async cleanup(): Promise<number> {
    if (this.options.retainPublishedMs <= 0) return 0;
    const before = new Date(Date.now() - this.options.retainPublishedMs);
    let total = 0;
    for (;;) {
      const deleted = await this.store.deletePublishedBefore(before, CLEANUP_CHUNK);
      total += deleted;
      if (deleted < CLEANUP_CHUNK || this.stopping) return total;
    }
  }

  private async loop(): Promise<void> {
    let consecutiveErrors = 0;
    while (this.running) {
      let claimed = 0;
      try {
        claimed = await this.processBatch();
        consecutiveErrors = 0;
      } catch (error) {
        consecutiveErrors++;
        this.logger.error(`Outbox poll failed (${consecutiveErrors}x): ${errorMessage(error)}`);
      }
      if (!this.running) break;

      await this.maybeCleanup();

      if (consecutiveErrors > 0) {
        await this.idle(
          Math.min(this.options.pollIntervalMs * 2 ** (consecutiveErrors - 1), MAX_ERROR_BACKOFF_MS),
        );
      } else if (claimed > 0) {
        // There may be more work: poll again right away, but yield to the event loop first.
        await new Promise((resolve) => setImmediate(resolve));
      } else {
        await this.idle(this.options.pollIntervalMs);
      }
    }
  }

  private idle(ms: number): Promise<void> {
    if (this.wakeRequested) {
      this.wakeRequested = false;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.wakeUp = undefined;
        this.wakeRequested = false;
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.wakeUp = done;
    });
  }

  private async maybeCleanup(): Promise<void> {
    if (this.options.retainPublishedMs <= 0 || Date.now() < this.nextCleanupAt) return;
    this.nextCleanupAt = Date.now() + this.options.cleanupIntervalMs;
    try {
      const deleted = await this.cleanup();
      if (deleted > 0) this.logger.log(`Outbox cleanup removed ${deleted} published message(s)`);
    } catch (error) {
      this.logger.warn(`Outbox cleanup failed: ${errorMessage(error)}`);
    }
  }

  private async doProcessBatch(): Promise<number> {
    const { batchSize, leaseMs, publishTimeoutMs, workerId } = this.options;
    const claimedAt = Date.now();
    const messages = await this.store.claim(batchSize, leaseMs, workerId);
    if (messages.length === 0) return 0;

    // Latest moment a publish may START and still finish (timeout) before the lease expires.
    // A 10% safety margin covers DB round-trips and timer drift.
    const startDeadline = claimedAt + leaseMs * 0.9 - publishTimeoutMs;
    const unstarted: string[] = [];
    let index = 0;

    const worker = async () => {
      while (index < messages.length) {
        const message = messages[index++];
        if (this.stopping || Date.now() > startDeadline) {
          unstarted.push(message.id);
          continue;
        }
        await this.deliver(message);
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(this.options.concurrency, messages.length) }, worker),
    );

    if (unstarted.length > 0) {
      try {
        await this.store.release(unstarted, workerId);
      } catch (error) {
        // Not fatal: the lease expires and the messages are reclaimed anyway.
        this.logger.warn(`Failed to release ${unstarted.length} message(s): ${errorMessage(error)}`);
      }
    }
    return messages.length;
  }

  private async deliver(message: OutboxMessage): Promise<void> {
    const { publishTimeoutMs, workerId } = this.options;
    try {
      await withTimeout((signal) => this.publisher.publish(message, signal), publishTimeoutMs, {
        name: `Publishing outbox message ${message.id}`,
      });
    } catch (error) {
      await this.handleFailure(message, error);
      return;
    }
    try {
      await this.store.markPublished([message.id], workerId);
    } catch (error) {
      // The broker has the message but we could not record it -> it will be re-published
      // after the lease expires. Consumers must be idempotent (dedupe on message.id).
      this.logger.error(`Published ${message.id} but failed to mark it: ${errorMessage(error)}`);
    }
  }

  private async handleFailure(message: OutboxMessage, error: unknown): Promise<void> {
    const { maxAttempts, retryBaseDelayMs, retryMaxDelayMs, workerId } = this.options;
    const dead = isNonRetryable(error) || message.attempts >= maxAttempts;
    const retryIn = dead
      ? null
      : computeDelay(message.attempts, {
          delayMs: retryBaseDelayMs,
          backoff: 'exponential',
          factor: 2,
          maxDelayMs: retryMaxDelayMs,
          jitter: 'equal',
        });

    if (dead) {
      this.logger.error(
        `Outbox message ${message.id} (${message.topic}) dead-lettered after ${message.attempts} attempt(s): ${errorMessage(error)}`,
      );
    } else {
      this.logger.warn(
        `Outbox message ${message.id} (${message.topic}) attempt ${message.attempts} failed, retry in ${retryIn}ms: ${errorMessage(error)}`,
      );
    }

    try {
      await this.store.markFailed(message.id, workerId, errorMessage(error), retryIn);
    } catch (storeError) {
      this.logger.error(`Failed to record failure of ${message.id}: ${errorMessage(storeError)}`);
      return;
    }

    if (dead && this.options.onDeadLetter) {
      try {
        await this.options.onDeadLetter(message, error);
      } catch (hookError) {
        this.logger.error(`onDeadLetter hook threw: ${errorMessage(hookError)}`);
      }
    }
  }
}
