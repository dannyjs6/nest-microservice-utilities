import { AbortedError, CircuitOpenError, isNonRetryable } from '../common/errors';
import { invokeAsPromise, sleep, wrapMethod } from '../common/utils';

export type BackoffStrategy = 'fixed' | 'linear' | 'exponential';

export interface RetryOptions {
  /** Total number of attempts INCLUDING the first call. Default: 3. */
  maxAttempts?: number;
  /** Base delay between attempts in ms. Default: 200. */
  delayMs?: number;
  /** How the delay grows. Default: 'exponential' (delay * factor^(attempt-1)). */
  backoff?: BackoffStrategy;
  /** Multiplier for exponential backoff. Default: 2. */
  factor?: number;
  /** Upper bound for a single delay. Default: 30_000. */
  maxDelayMs?: number;
  /**
   * Randomise delays to avoid thundering herds when many instances retry at once.
   * 'full'  -> random(0, delay)            (AWS "full jitter", best spread)
   * 'equal' -> delay/2 + random(0, delay/2)
   * 'none'  -> exact delay
   * Default: 'full'.
   */
  jitter?: 'full' | 'equal' | 'none';
  /**
   * Decide whether an error should be retried. Called AFTER the built-in checks:
   * `NonRetryableError`, `{ retryable: false }` errors, `CircuitOpenError` and
   * `AbortedError` are never retried.
   */
  retryIf?: (error: unknown, attempt: number) => boolean;
  /** Hook called before sleeping, e.g. for logging/metrics. Errors thrown here are ignored. */
  onRetry?: (error: unknown, attempt: number, delayMs: number) => void;
  /** Cancels pending retries (and the wait between them). */
  signal?: AbortSignal;
}

export interface RetryContext {
  /** 1-based attempt number. */
  attempt: number;
  signal?: AbortSignal;
}

type ResolvedRetryOptions = Required<Omit<RetryOptions, 'retryIf' | 'onRetry' | 'signal'>> &
  Pick<RetryOptions, 'retryIf' | 'onRetry' | 'signal'>;

function resolveOptions(options: RetryOptions = {}): ResolvedRetryOptions {
  const resolved: ResolvedRetryOptions = {
    maxAttempts: 3,
    delayMs: 200,
    backoff: 'exponential',
    factor: 2,
    maxDelayMs: 30_000,
    jitter: 'full',
    ...stripUndefined(options),
  };
  if (!Number.isInteger(resolved.maxAttempts) || resolved.maxAttempts < 1) {
    throw new RangeError(`retry: maxAttempts must be an integer >= 1 (got ${resolved.maxAttempts})`);
  }
  if (resolved.delayMs < 0 || resolved.maxDelayMs < 0) {
    throw new RangeError('retry: delays must be >= 0');
  }
  return resolved;
}

function stripUndefined<T extends object>(obj: T): Partial<T> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined)) as Partial<T>;
}

/** Computes the delay to wait after the given (1-based) failed attempt. Exported for testing. */
export function computeDelay(
  attempt: number,
  opts: Pick<ResolvedRetryOptions, 'delayMs' | 'backoff' | 'factor' | 'maxDelayMs' | 'jitter'>,
  random: () => number = Math.random,
): number {
  let delay: number;
  switch (opts.backoff) {
    case 'fixed':
      delay = opts.delayMs;
      break;
    case 'linear':
      delay = opts.delayMs * attempt;
      break;
    case 'exponential':
    default:
      delay = opts.delayMs * Math.pow(opts.factor, attempt - 1);
  }
  // Guard against Infinity/NaN from huge exponents.
  delay = Number.isFinite(delay) ? Math.min(delay, opts.maxDelayMs) : opts.maxDelayMs;

  switch (opts.jitter) {
    case 'none':
      return Math.round(delay);
    case 'equal':
      return Math.round(delay / 2 + random() * (delay / 2));
    case 'full':
    default:
      return Math.round(random() * delay);
  }
}

function shouldRetry(error: unknown, attempt: number, opts: ResolvedRetryOptions): boolean {
  if (attempt >= opts.maxAttempts) return false;
  if (isNonRetryable(error)) return false;
  if (error instanceof CircuitOpenError || error instanceof AbortedError) return false;
  if (opts.signal?.aborted) return false;
  return opts.retryIf ? opts.retryIf(error, attempt) : true;
}

/**
 * Functional retry. The operation receives the attempt number and the abort signal.
 * After the last attempt the ORIGINAL error is rethrown (so `instanceof` checks keep working).
 *
 * @example
 * const user = await retry(() => http.get(`/users/${id}`), { maxAttempts: 5 });
 */
export async function retry<T>(
  operation: (ctx: RetryContext) => T | Promise<T>,
  options?: RetryOptions,
): Promise<T> {
  const opts = resolveOptions(options);
  for (let attempt = 1; ; attempt++) {
    if (opts.signal?.aborted) throw new AbortedError(opts.signal.reason);
    try {
      return await invokeAsPromise(() => operation({ attempt, signal: opts.signal }));
    } catch (error) {
      if (!shouldRetry(error, attempt, opts)) throw error;
      const delay = computeDelay(attempt, opts);
      try {
        opts.onRetry?.(error, attempt, delay);
      } catch {
        /* a broken logger must never break the business call */
      }
      await sleep(delay, opts.signal);
    }
  }
}

/**
 * Retries the decorated method on failure.
 *
 * Decorators run bottom-up, so put `@Timeout` BELOW `@Retry` to get a timeout per attempt:
 *
 * @example
 * @Retry({ maxAttempts: 4, delayMs: 100 })
 * @Timeout(2_000)
 * async fetchPrice(sku: string) { ... }
 *
 * Note: the decorated method always returns a Promise.
 */
export function Retry(options?: RetryOptions): MethodDecorator {
  // Validate eagerly so misconfiguration fails at boot, not on the first error.
  resolveOptions(options);
  return (target, propertyKey, descriptor: PropertyDescriptor) =>
    wrapMethod(descriptor, 'Retry', (original) =>
      function (this: unknown, ...args: unknown[]) {
        return retry(() => original.apply(this, args), options);
      },
    );
}
