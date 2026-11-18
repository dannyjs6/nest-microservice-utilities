/**
 * Base class for every error thrown by this library, so callers can do a single
 * `instanceof MicroserviceUtilsError` check if they want to.
 */
export class MicroserviceUtilsError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = new.target.name;
    if (options && 'cause' in options) {
      // Assigned manually so it works regardless of the `lib` target.
      (this as { cause?: unknown }).cause = options.cause;
    }
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown by `@Timeout` / `withTimeout` when the operation does not settle in time. */
export class TimeoutError extends MicroserviceUtilsError {
  constructor(
    public readonly timeoutMs: number,
    operation?: string,
  ) {
    super(`${operation ?? 'Operation'} timed out after ${timeoutMs}ms`);
  }
}

/** Thrown by `@CircuitBreaker` while the circuit is open (calls are short-circuited). */
export class CircuitOpenError extends MicroserviceUtilsError {
  constructor(
    public readonly circuit: string,
    public readonly retryAfterMs: number,
  ) {
    super(`Circuit "${circuit}" is open; retry after ${retryAfterMs}ms`);
  }
}

/** Thrown by `@Bulkhead` when both the concurrency slots and the queue are full. */
export class BulkheadRejectedError extends MicroserviceUtilsError {
  constructor(
    public readonly bulkhead: string,
    reason: 'queue_full' | 'queue_timeout',
  ) {
    super(
      reason === 'queue_full'
        ? `Bulkhead "${bulkhead}" is full`
        : `Bulkhead "${bulkhead}": timed out waiting for a free slot`,
    );
  }
}

/**
 * Throw (or wrap an error in) this to signal "do not retry this, it will never
 * succeed" – e.g. validation errors or a 4xx from a downstream service.
 * Honoured by `@Retry` and by the outbox relay (message goes straight to `failed`).
 */
export class NonRetryableError extends MicroserviceUtilsError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}

/** Thrown when an operation is cancelled through an `AbortSignal`. */
export class AbortedError extends MicroserviceUtilsError {
  constructor(reason?: unknown) {
    super(
      reason instanceof Error ? `Aborted: ${reason.message}` : 'Operation aborted',
      { cause: reason },
    );
  }
}

export function isNonRetryable(error: unknown): boolean {
  return (
    error instanceof NonRetryableError ||
    // Allows errors from other packages/copies of this lib to opt out by duck typing.
    (typeof error === 'object' && error !== null && (error as { retryable?: unknown }).retryable === false)
  );
}

export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  try {
    return typeof error === 'string' ? error : JSON.stringify(error);
  } catch {
    return String(error);
  }
}
