import { BulkheadRejectedError } from '../common/errors';
import { invokeAsPromise, methodName, wrapMethod } from '../common/utils';

export interface BulkheadOptions {
  /** Bulkheads with the same name share slots. Default: `ClassName.methodName`. */
  name?: string;
  /** Max concurrent executions. Required. */
  maxConcurrent: number;
  /** Max callers waiting for a slot; extra callers are rejected immediately. Default: 0. */
  maxQueue?: number;
  /** Max time a caller may wait in the queue. Default: no limit. */
  queueTimeoutMs?: number;
}

interface Waiter {
  resolve: () => void;
  reject: (err: Error) => void;
  timer?: NodeJS.Timeout;
}

/**
 * Concurrency limiter (a.k.a. semaphore / bulkhead). Protects a downstream dependency
 * (or your own event loop) from being flooded, and fails fast when overloaded instead
 * of letting latency grow without bound.
 */
export class BulkheadPolicy {
  readonly name: string;
  private active = 0;
  private readonly queue: Waiter[] = [];
  private readonly maxConcurrent: number;
  private readonly maxQueue: number;
  private readonly queueTimeoutMs?: number;

  constructor(options: BulkheadOptions & { name: string }) {
    this.name = options.name;
    this.maxConcurrent = options.maxConcurrent;
    this.maxQueue = options.maxQueue ?? 0;
    this.queueTimeoutMs = options.queueTimeoutMs;
    if (!Number.isInteger(this.maxConcurrent) || this.maxConcurrent < 1) {
      throw new RangeError('bulkhead: maxConcurrent must be an integer >= 1');
    }
    if (!Number.isInteger(this.maxQueue) || this.maxQueue < 0) {
      throw new RangeError('bulkhead: maxQueue must be an integer >= 0');
    }
  }

  get stats(): { active: number; queued: number } {
    return { active: this.active, queued: this.queue.length };
  }

  async execute<T>(operation: () => T | Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await invokeAsPromise(operation);
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    if (this.active < this.maxConcurrent) {
      this.active++;
      return Promise.resolve();
    }
    if (this.queue.length >= this.maxQueue) {
      return Promise.reject(new BulkheadRejectedError(this.name, 'queue_full'));
    }
    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject };
      if (this.queueTimeoutMs !== undefined) {
        waiter.timer = setTimeout(() => {
          const idx = this.queue.indexOf(waiter);
          if (idx !== -1) this.queue.splice(idx, 1);
          reject(new BulkheadRejectedError(this.name, 'queue_timeout'));
        }, this.queueTimeoutMs);
      }
      this.queue.push(waiter);
    });
  }

  private release(): void {
    const next = this.queue.shift();
    if (next) {
      // Hand the slot directly to the next waiter: `active` stays the same, so a
      // new caller can't sneak in between release and wake-up.
      clearTimeout(next.timer);
      next.resolve();
    } else {
      this.active--;
    }
  }
}

const registry = new Map<string, BulkheadPolicy>();

export function getBulkhead(name: string): BulkheadPolicy | undefined {
  return registry.get(name);
}

/** Intended for tests. */
export function clearBulkheads(): void {
  registry.clear();
}

/**
 * Limits concurrent executions of the method (shared by all instances of the class).
 *
 * @example
 * @Bulkhead({ maxConcurrent: 10, maxQueue: 50, queueTimeoutMs: 1_000 })
 * async generateReport() { ... }
 */
export function Bulkhead(options: BulkheadOptions): MethodDecorator {
  // Validate at decoration time.
  new BulkheadPolicy({ ...options, name: options.name ?? 'validation' });
  return (target, propertyKey, descriptor: PropertyDescriptor) =>
    wrapMethod(descriptor, 'Bulkhead', (original) => {
      const name = options.name ?? methodName(target, propertyKey);
      return function (this: unknown, ...args: unknown[]) {
        let bulkhead = registry.get(name);
        if (!bulkhead) {
          bulkhead = new BulkheadPolicy({ ...options, name });
          registry.set(name, bulkhead);
        }
        return bulkhead.execute(() => original.apply(this, args));
      };
    });
}
