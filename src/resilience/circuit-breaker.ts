import { CircuitOpenError } from '../common/errors';
import { invokeAsPromise, methodName, wrapMethod } from '../common/utils';

export type CircuitState = 'closed' | 'open' | 'half_open';

export interface CircuitBreakerOptions {
  /**
   * Circuits with the same name share state (the options of the first one to run win).
   * Use it when several methods call the same
   * downstream dependency. Default: `ClassName.methodName` (one circuit per method,
   * shared by all instances of the class – including request-scoped ones).
   */
  name?: string;
  /** Consecutive failures that open the circuit. Default: 5. */
  failureThreshold?: number;
  /** How long the circuit stays open before letting a probe through. Default: 30_000. */
  resetTimeoutMs?: number;
  /** Concurrent probe calls allowed while half-open. Default: 1. */
  halfOpenMaxCalls?: number;
  /** Successful probes needed to close the circuit again. Default: 1. */
  successThreshold?: number;
  /**
   * Which errors count as a failure of the dependency. Errors that return `false`
   * (e.g. validation / 404) are passed through and treated as a "healthy" response.
   * Default: every error counts.
   */
  isFailure?: (error: unknown) => boolean;
  /** Called on every state transition – hook your logger/metrics here. */
  onStateChange?: (state: CircuitState, previous: CircuitState, name: string) => void;
  /** Injected clock, for tests. */
  now?: () => number;
}

/**
 * A small consecutive-failures circuit breaker.
 *
 *   closed --(N consecutive failures)--> open --(resetTimeout)--> half_open
 *   half_open --(success x successThreshold)--> closed
 *   half_open --(any failure)--> open
 *
 * Tip: always combine with a timeout, otherwise a hanging probe keeps the
 * circuit half-open (and rejecting) until it settles.
 */
export class CircuitBreakerPolicy {
  readonly name: string;
  private _state: CircuitState = 'closed';
  private failures = 0;
  private halfOpenSuccesses = 0;
  private halfOpenInFlight = 0;
  private openedAt = 0;
  private readonly opts: Required<Omit<CircuitBreakerOptions, 'onStateChange' | 'name'>> &
    Pick<CircuitBreakerOptions, 'onStateChange'>;

  constructor(options: CircuitBreakerOptions & { name: string }) {
    this.name = options.name;
    this.opts = {
      failureThreshold: options.failureThreshold ?? 5,
      resetTimeoutMs: options.resetTimeoutMs ?? 30_000,
      halfOpenMaxCalls: options.halfOpenMaxCalls ?? 1,
      successThreshold: options.successThreshold ?? 1,
      isFailure: options.isFailure ?? (() => true),
      onStateChange: options.onStateChange,
      now: options.now ?? Date.now,
    };
    for (const key of ['failureThreshold', 'halfOpenMaxCalls', 'successThreshold'] as const) {
      if (!Number.isInteger(this.opts[key]) || this.opts[key] < 1) {
        throw new RangeError(`circuit breaker: ${key} must be an integer >= 1`);
      }
    }
    if (this.opts.resetTimeoutMs < 0) throw new RangeError('circuit breaker: resetTimeoutMs must be >= 0');
  }

  /** Current state (lazily moves open -> half_open when the reset timeout has elapsed). */
  get state(): CircuitState {
    if (this._state === 'open' && this.opts.now() - this.openedAt >= this.opts.resetTimeoutMs) {
      this.transition('half_open');
    }
    return this._state;
  }

  async execute<T>(operation: () => T | Promise<T>): Promise<T> {
    const state = this.state;
    if (state === 'open') {
      throw new CircuitOpenError(this.name, this.remainingOpenMs());
    }
    const isProbe = state === 'half_open';
    if (isProbe) {
      if (this.halfOpenInFlight >= this.opts.halfOpenMaxCalls) {
        throw new CircuitOpenError(this.name, 0);
      }
      this.halfOpenInFlight++;
    }

    try {
      const result = await invokeAsPromise(operation);
      this.onSuccess(isProbe);
      return result;
    } catch (error) {
      let counts = true;
      try {
        counts = this.opts.isFailure(error);
      } catch {
        /* a throwing predicate counts as failure */
      }
      if (counts) this.onFailure(isProbe);
      else this.onSuccess(isProbe);
      throw error;
    } finally {
      if (isProbe) this.halfOpenInFlight--;
    }
  }

  /** Force the circuit closed (e.g. from an admin endpoint or in tests). */
  reset(): void {
    this.failures = 0;
    this.halfOpenSuccesses = 0;
    this.transition('closed');
  }

  private onSuccess(isProbe: boolean): void {
    if (isProbe && this._state === 'half_open') {
      if (++this.halfOpenSuccesses >= this.opts.successThreshold) this.reset();
      return;
    }
    // A slow call that started before the circuit opened must not close it.
    if (this._state === 'closed') this.failures = 0;
  }

  private onFailure(isProbe: boolean): void {
    if (isProbe && this._state === 'half_open') {
      this.open();
      return;
    }
    if (this._state === 'closed' && ++this.failures >= this.opts.failureThreshold) {
      this.open();
    }
  }

  private open(): void {
    this.openedAt = this.opts.now();
    this.failures = 0;
    this.halfOpenSuccesses = 0;
    this.transition('open');
  }

  private remainingOpenMs(): number {
    return Math.max(0, this.opts.resetTimeoutMs - (this.opts.now() - this.openedAt));
  }

  private transition(next: CircuitState): void {
    const previous = this._state;
    if (previous === next) return;
    this._state = next;
    if (next === 'half_open') this.halfOpenSuccesses = 0;
    try {
      this.opts.onStateChange?.(next, previous, this.name);
    } catch {
      /* never let a listener break the call path */
    }
  }
}

const registry = new Map<string, CircuitBreakerPolicy>();

/** Returns the named circuit once it has been used at least once. Useful for health checks. */
export function getCircuitBreaker(name: string): CircuitBreakerPolicy | undefined {
  return registry.get(name);
}

/** All registered circuits – e.g. to expose their state on a /health endpoint. */
export function listCircuitBreakers(): CircuitBreakerPolicy[] {
  return [...registry.values()];
}

/** Removes every registered circuit. Intended for tests. */
export function clearCircuitBreakers(): void {
  registry.clear();
}

function obtainBreaker(options: CircuitBreakerOptions & { name: string }): CircuitBreakerPolicy {
  let breaker = registry.get(options.name);
  if (!breaker) {
    breaker = new CircuitBreakerPolicy(options);
    registry.set(options.name, breaker);
  }
  return breaker;
}

/**
 * Short-circuits calls with `CircuitOpenError` after repeated failures.
 * `@Retry` does not retry `CircuitOpenError`, so put `@CircuitBreaker` BELOW `@Retry`
 * if you want every attempt to count, or ABOVE it to count a whole retry sequence as one.
 */
export function CircuitBreaker(options: CircuitBreakerOptions = {}): MethodDecorator {
  return (target, propertyKey, descriptor: PropertyDescriptor) =>
    wrapMethod(descriptor, 'CircuitBreaker', (original) => {
      const name = options.name ?? methodName(target, propertyKey);
      // Resolved lazily so `clearCircuitBreakers()` in tests gives a fresh circuit.
      return function (this: unknown, ...args: unknown[]) {
        return obtainBreaker({ ...options, name }).execute(() => original.apply(this, args));
      };
    });
}
