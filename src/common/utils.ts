import { AbortedError } from './errors';

/** Sleep that can be cancelled with an AbortSignal (rejects with AbortedError). */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new AbortedError(signal.reason));
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(new AbortedError(signal?.reason));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

interface SubscribableLike<T> {
  subscribe(observer: {
    next?: (value: T) => void;
    error?: (err: unknown) => void;
    complete?: () => void;
  }): { unsubscribe(): void };
}

function isSubscribable(value: unknown): value is SubscribableLike<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { subscribe?: unknown }).subscribe === 'function' &&
    typeof (value as { then?: unknown }).then !== 'function'
  );
}

/**
 * Turns any return value into a Promise:
 *  - sync throws become rejections,
 *  - Observables (e.g. from a ClientProxy-like client) resolve with their LAST value
 *    (an Observable that completes without emitting resolves `undefined`).
 */
export function invokeAsPromise<T>(fn: () => T | Promise<T>): Promise<T> {
  let result: unknown;
  try {
    result = fn();
  } catch (err) {
    return Promise.reject(err);
  }
  if (isSubscribable(result)) {
    const source = result;
    return new Promise<T>((resolve, reject) => {
      let last: unknown;
      source.subscribe({
        next: (v) => (last = v),
        error: reject,
        complete: () => resolve(last as T),
      });
    });
  }
  return Promise.resolve(result as T);
}

type AnyFn = (...args: any[]) => any;

/**
 * Replaces a method with a wrapper while keeping everything NestJS relies on:
 *  - `reflect-metadata` keys on the original function (e.g. @MessagePattern, @EventPattern,
 *    @UseGuards put their metadata on `descriptor.value`) are copied to the wrapper,
 *  - the function `name` is preserved (useful for logs and stack traces).
 *
 * This makes the decorators safe to put ABOVE or BELOW Nest decorators.
 */
export function wrapMethod(
  descriptor: PropertyDescriptor,
  decoratorName: string,
  createWrapper: (original: AnyFn) => AnyFn,
): PropertyDescriptor {
  const original = descriptor.value;
  if (typeof original !== 'function') {
    throw new TypeError(`@${decoratorName} can only be applied to methods`);
  }
  const wrapper = createWrapper(original);
  copyMetadata(original, wrapper);
  Object.defineProperty(wrapper, 'name', { value: original.name, configurable: true });
  descriptor.value = wrapper;
  return descriptor;
}

function copyMetadata(source: object, target: object): void {
  const reflect = Reflect as unknown as {
    getMetadataKeys?: (t: object) => unknown[];
    getMetadata?: (k: unknown, t: object) => unknown;
    defineMetadata?: (k: unknown, v: unknown, t: object) => void;
  };
  if (!reflect.getMetadataKeys || !reflect.getMetadata || !reflect.defineMetadata) return;
  for (const key of reflect.getMetadataKeys(source)) {
    reflect.defineMetadata(key, reflect.getMetadata(key, source), target);
  }
}

/** Builds a stable, human-readable name for a decorated method: `ClassName.method`. */
export function methodName(target: object, propertyKey: string | symbol): string {
  const ctor = typeof target === 'function' ? target : target.constructor;
  return `${ctor?.name ?? 'Anonymous'}.${String(propertyKey)}`;
}
