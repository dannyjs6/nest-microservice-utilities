import { TimeoutError } from '../common/errors';
import { invokeAsPromise, methodName, wrapMethod } from '../common/utils';

/**
 * Runs `operation` and rejects with `TimeoutError` if it does not settle within `ms`.
 *
 * The operation receives an AbortSignal that fires on timeout – pass it to `fetch`,
 * `pg` cancellation, etc. so the underlying work is actually cancelled instead of
 * leaking in the background.
 *
 * Edge cases handled:
 *  - the timer is always cleared (no keeping the event loop alive / leaking timers),
 *  - a rejection of the original promise AFTER the timeout won't become an
 *    `unhandledRejection` that crashes the process,
 *  - an outer `signal` (e.g. request cancellation) is respected too.
 */
export function withTimeout<T>(
  operation: (signal: AbortSignal) => T | Promise<T>,
  ms: number,
  options: { name?: string; signal?: AbortSignal } = {},
): Promise<T> {
  validateTimeout(ms);
  const controller = new AbortController();
  const onOuterAbort = () => controller.abort(options.signal?.reason);
  if (options.signal?.aborted) controller.abort(options.signal.reason);
  else options.signal?.addEventListener('abort', onOuterAbort, { once: true });

  const work = invokeAsPromise(() => operation(controller.signal));

  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const error = new TimeoutError(ms, options.name);
      controller.abort(error);
      reject(error);
    }, ms);
  });

  // Swallow late rejections of the losing promise.
  work.catch(() => undefined);

  return Promise.race([work, timeout]).finally(() => {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', onOuterAbort);
  });
}

function validateTimeout(ms: number): void {
  if (!Number.isFinite(ms) || ms <= 0) {
    throw new RangeError(`timeout must be a positive finite number of ms (got ${ms})`);
  }
}

/**
 * Fails the decorated method with `TimeoutError` after `ms` milliseconds.
 *
 * A decorator cannot inject an AbortSignal into your method, so the underlying work
 * keeps running in the background after the timeout. If you need real cancellation,
 * use `withTimeout(signal => ...)` inside the method instead.
 */
export function Timeout(ms: number): MethodDecorator {
  validateTimeout(ms);
  return (target, propertyKey, descriptor: PropertyDescriptor) =>
    wrapMethod(descriptor, 'Timeout', (original) => {
      const name = methodName(target, propertyKey);
      return function (this: unknown, ...args: unknown[]) {
        return withTimeout(() => original.apply(this, args), ms, { name });
      };
    });
}
