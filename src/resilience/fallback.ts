import { invokeAsPromise, wrapMethod } from '../common/utils';

export type FallbackHandler<TThis = any> = (
  this: TThis,
  error: unknown,
  ...args: any[]
) => unknown | Promise<unknown>;

export interface FallbackOptions {
  /** Only fall back for matching errors; others are rethrown. Default: all errors. */
  when?: (error: unknown) => boolean;
}

/**
 * Returns an alternative result when the method fails (cached value, default, degraded
 * response…).
 *
 * The handler is either a function `(error, ...originalArgs)` or the NAME of another
 * method on the same class, which is called with `this` bound to the instance – handy
 * when the fallback needs injected dependencies:
 *
 * @example
 * @Fallback('fromCache')
 * @Retry()
 * async getRates(currency: string) { return this.api.rates(currency); }
 *
 * private fromCache(error: unknown, currency: string) { return this.cache.get(currency); }
 */
export function Fallback(handler: FallbackHandler | string, options: FallbackOptions = {}): MethodDecorator {
  return (_target, propertyKey, descriptor: PropertyDescriptor) =>
    wrapMethod(descriptor, 'Fallback', (original) =>
      async function (this: any, ...args: unknown[]) {
        try {
          return await invokeAsPromise(() => original.apply(this, args));
        } catch (error) {
          if (options.when && !options.when(error)) throw error;
          const fn: unknown = typeof handler === 'string' ? this?.[handler] : handler;
          if (typeof fn !== 'function') {
            throw new TypeError(
              `@Fallback on ${String(propertyKey)}: "${String(handler)}" is not a method`,
            );
          }
          return fn.call(this, error, ...args);
        }
      },
    );
}
