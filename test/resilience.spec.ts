import { Controller, Injectable, SetMetadata } from '@nestjs/common';
import {
  AbortedError,
  Bulkhead,
  BulkheadRejectedError,
  CircuitBreaker,
  CircuitBreakerPolicy,
  CircuitOpenError,
  clearBulkheads,
  clearCircuitBreakers,
  computeDelay,
  Fallback,
  getCircuitBreaker,
  NonRetryableError,
  Retry,
  retry,
  Timeout,
  TimeoutError,
  withTimeout,
} from '../src';

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));

afterEach(() => {
  clearCircuitBreakers();
  clearBulkheads();
});

describe('retry', () => {
  it('retries until success and passes the attempt number', async () => {
    const seen: number[] = [];
    const result = await retry(
      ({ attempt }) => {
        seen.push(attempt);
        if (attempt < 3) throw new Error('boom');
        return 'ok';
      },
      { maxAttempts: 5, delayMs: 1, jitter: 'none' },
    );
    expect(result).toBe('ok');
    expect(seen).toEqual([1, 2, 3]);
  });

  it('rethrows the ORIGINAL error after maxAttempts', async () => {
    class MyError extends Error {}
    let calls = 0;
    await expect(
      retry(() => {
        calls++;
        throw new MyError('nope');
      }, { maxAttempts: 3, delayMs: 1 }),
    ).rejects.toBeInstanceOf(MyError);
    expect(calls).toBe(3);
  });

  it.each([
    ['NonRetryableError', new NonRetryableError('bad input')],
    ['retryable:false', Object.assign(new Error('4xx'), { retryable: false })],
    ['CircuitOpenError', new CircuitOpenError('x', 100)],
  ])('does not retry %s', async (_name, error) => {
    let calls = 0;
    await expect(
      retry(() => {
        calls++;
        throw error;
      }, { maxAttempts: 5, delayMs: 1 }),
    ).rejects.toBe(error);
    expect(calls).toBe(1);
  });

  it('respects retryIf', async () => {
    let calls = 0;
    await expect(
      retry(() => {
        calls++;
        throw new Error('validation');
      }, { maxAttempts: 5, delayMs: 1, retryIf: (e) => (e as Error).message !== 'validation' }),
    ).rejects.toThrow('validation');
    expect(calls).toBe(1);
  });

  it('can be aborted while waiting between attempts', async () => {
    const controller = new AbortController();
    const p = retry(() => Promise.reject(new Error('x')), {
      maxAttempts: 10,
      delayMs: 10_000,
      jitter: 'none',
      signal: controller.signal,
    });
    await tick(5);
    controller.abort();
    await expect(p).rejects.toBeInstanceOf(AbortedError);
  });

  it('a throwing onRetry hook does not break retries', async () => {
    let calls = 0;
    const res = await retry(
      () => (++calls < 2 ? Promise.reject(new Error('x')) : 'ok'),
      { delayMs: 1, onRetry: () => { throw new Error('logger down'); } },
    );
    expect(res).toBe('ok');
  });

  it('computes capped backoff with jitter', () => {
    const base = { delayMs: 100, factor: 2, maxDelayMs: 1_000 } as const;
    expect(computeDelay(1, { ...base, backoff: 'exponential', jitter: 'none' })).toBe(100);
    expect(computeDelay(3, { ...base, backoff: 'exponential', jitter: 'none' })).toBe(400);
    expect(computeDelay(50, { ...base, backoff: 'exponential', jitter: 'none' })).toBe(1_000);
    expect(computeDelay(5000, { ...base, backoff: 'exponential', jitter: 'none' })).toBe(1_000); // Infinity guard
    expect(computeDelay(3, { ...base, backoff: 'linear', jitter: 'none' })).toBe(300);
    expect(computeDelay(3, { ...base, backoff: 'fixed', jitter: 'none' })).toBe(100);
    expect(computeDelay(3, { ...base, backoff: 'exponential', jitter: 'full' }, () => 0.5)).toBe(200);
    expect(computeDelay(3, { ...base, backoff: 'exponential', jitter: 'equal' }, () => 0)).toBe(200);
  });

  it('rejects invalid configuration at decoration time', () => {
    expect(() => Retry({ maxAttempts: 0 })).toThrow(RangeError);
  });
});

describe('withTimeout / @Timeout', () => {
  it('rejects with TimeoutError and aborts the signal', async () => {
    let signal!: AbortSignal;
    await expect(
      withTimeout((s) => {
        signal = s;
        return new Promise(() => undefined);
      }, 20),
    ).rejects.toBeInstanceOf(TimeoutError);
    expect(signal.aborted).toBe(true);
  });

  it('does not produce an unhandled rejection when the operation fails after the timeout', async () => {
    const unhandled = jest.fn();
    process.on('unhandledRejection', unhandled);
    try {
      await expect(
        withTimeout(() => tick(30).then(() => Promise.reject(new Error('late'))), 10),
      ).rejects.toBeInstanceOf(TimeoutError);
      await tick(50);
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
  });

  it('turns synchronous throws into rejections', async () => {
    await expect(withTimeout(() => { throw new Error('sync'); }, 100)).rejects.toThrow('sync');
  });

  it('respects an outer abort signal', async () => {
    const outer = new AbortController();
    let inner!: AbortSignal;
    const p = withTimeout((s) => {
      inner = s;
      return tick(50);
    }, 1_000, { signal: outer.signal });
    outer.abort();
    expect(inner.aborted).toBe(true);
    await p;
  });

  it('decorator names the operation in the error', async () => {
    class Svc {
      @Timeout(10)
      slow() {
        return tick(100);
      }
    }
    await expect(new Svc().slow()).rejects.toThrow('Svc.slow timed out after 10ms');
  });

  it('rejects invalid timeouts', () => {
    expect(() => Timeout(0)).toThrow(RangeError);
    expect(() => Timeout(Number.NaN)).toThrow(RangeError);
  });
});

describe('CircuitBreakerPolicy', () => {
  function make(now: { t: number }, extra = {}) {
    return new CircuitBreakerPolicy({
      name: 'test',
      failureThreshold: 2,
      resetTimeoutMs: 1_000,
      now: () => now.t,
      ...extra,
    });
  }
  const fail = () => Promise.reject(new Error('down'));

  it('opens after consecutive failures and short-circuits', async () => {
    const now = { t: 0 };
    const cb = make(now);
    await expect(cb.execute(fail)).rejects.toThrow('down');
    await expect(cb.execute(fail)).rejects.toThrow('down');
    expect(cb.state).toBe('open');
    const op = jest.fn();
    await expect(cb.execute(op)).rejects.toBeInstanceOf(CircuitOpenError);
    expect(op).not.toHaveBeenCalled();
  });

  it('a success resets the consecutive failure count', async () => {
    const cb = make({ t: 0 });
    await expect(cb.execute(fail)).rejects.toThrow();
    await cb.execute(() => 'ok');
    await expect(cb.execute(fail)).rejects.toThrow();
    expect(cb.state).toBe('closed');
  });

  it('half-open allows a single probe; success closes, failure reopens', async () => {
    const now = { t: 0 };
    const changes: string[] = [];
    const cb = make(now, { onStateChange: (s: string) => changes.push(s) });
    await expect(cb.execute(fail)).rejects.toThrow();
    await expect(cb.execute(fail)).rejects.toThrow();
    now.t = 1_000;
    expect(cb.state).toBe('half_open');

    // Probe in flight -> concurrent call rejected.
    let release!: () => void;
    const probe = cb.execute(() => new Promise<void>((r) => (release = r)));
    await expect(cb.execute(() => 'x')).rejects.toBeInstanceOf(CircuitOpenError);
    release();
    await probe;
    expect(cb.state).toBe('closed');

    await expect(cb.execute(fail)).rejects.toThrow();
    await expect(cb.execute(fail)).rejects.toThrow();
    now.t = 2_500;
    await expect(cb.execute(fail)).rejects.toThrow('down'); // failed probe
    expect(cb.state).toBe('open');
    expect(changes).toEqual(['open', 'half_open', 'closed', 'open', 'half_open', 'open']);
  });

  it('ignores errors rejected by isFailure', async () => {
    const cb = make({ t: 0 }, { isFailure: (e: Error) => e.message !== '404' });
    for (let i = 0; i < 5; i++) {
      await expect(cb.execute(() => Promise.reject(new Error('404')))).rejects.toThrow();
    }
    expect(cb.state).toBe('closed');
  });

  it('decorator shares state across instances (incl. request-scoped)', async () => {
    class Client {
      @CircuitBreaker({ failureThreshold: 1, resetTimeoutMs: 60_000 })
      call(): Promise<void> {
        return Promise.reject(new Error('down'));
      }
    }
    await expect(new Client().call()).rejects.toThrow('down');
    await expect(new Client().call()).rejects.toBeInstanceOf(CircuitOpenError);
    expect(getCircuitBreaker('Client.call')?.state).toBe('open');
  });
});

describe('@Bulkhead', () => {
  it('limits concurrency, queues, and rejects overflow', async () => {
    let active = 0;
    let peak = 0;
    class Svc {
      @Bulkhead({ maxConcurrent: 2, maxQueue: 1 })
      async work() {
        active++;
        peak = Math.max(peak, active);
        await tick(20);
        active--;
        return 'done';
      }
    }
    const svc = new Svc();
    const results = await Promise.allSettled([svc.work(), svc.work(), svc.work(), svc.work()]);
    expect(peak).toBe(2);
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(3);
    const rejected = results.find((r) => r.status === 'rejected') as PromiseRejectedResult;
    expect(rejected.reason).toBeInstanceOf(BulkheadRejectedError);
  });

  it('frees the slot when the method throws, and times out queued callers', async () => {
    class Svc {
      @Bulkhead({ name: 'bh-timeout', maxConcurrent: 1, maxQueue: 5, queueTimeoutMs: 10 })
      async work(ms: number, fail = false) {
        await tick(ms);
        if (fail) throw new Error('fail');
      }
    }
    const svc = new Svc();
    await expect(svc.work(1, true)).rejects.toThrow('fail');
    const slow = svc.work(50);
    await expect(svc.work(1)).rejects.toThrow(/timed out waiting/);
    await slow;
    await expect(svc.work(1)).resolves.toBeUndefined();
  });
});

describe('@Fallback', () => {
  it('calls a sibling method with `this` bound and original args', async () => {
    class Svc {
      cache = new Map([['EUR', 1.1]]);
      @Fallback('fromCache')
      async rate(_c: string): Promise<number> {
        throw new Error('api down');
      }
      fromCache(_e: unknown, currency: string) {
        return this.cache.get(currency);
      }
    }
    await expect(new Svc().rate('EUR')).resolves.toBe(1.1);
  });

  it('only falls back for matching errors', async () => {
    class Svc {
      @Fallback(() => 'default', { when: (e) => e instanceof TimeoutError })
      async a(): Promise<string> {
        throw new Error('other');
      }
    }
    await expect(new Svc().a()).rejects.toThrow('other');
  });
});

describe('composition and Nest compatibility', () => {
  it('keeps Nest metadata regardless of decorator order', () => {
    @Controller()
    class Ctrl {
      @Retry({ maxAttempts: 2 })
      @SetMetadata('custom:pattern', 'orders.created')
      handleAbove() {}

      @SetMetadata('custom:pattern', 'orders.updated')
      @Retry({ maxAttempts: 2 })
      handleBelow() {}
    }
    const proto = Ctrl.prototype as any;
    expect(Reflect.getMetadata('custom:pattern', proto.handleAbove)).toBe('orders.created');
    expect(Reflect.getMetadata('custom:pattern', proto.handleBelow)).toBe('orders.updated');
    expect(proto.handleAbove.name).toBe('handleAbove');
  });

  it('Retry + Timeout gives a timeout per attempt', async () => {
    let calls = 0;
    @Injectable()
    class Svc {
      @Retry({ maxAttempts: 3, delayMs: 1, jitter: 'none' })
      @Timeout(15)
      async flaky() {
        calls++;
        if (calls < 3) await tick(100); // hangs on first two attempts
        return 'ok';
      }
    }
    await expect(new Svc().flaky()).resolves.toBe('ok');
    expect(calls).toBe(3);
  });

  it('Fallback + Retry + CircuitBreaker work together', async () => {
    let calls = 0;
    class Svc {
      @Fallback(() => 'fallback')
      @Retry({ maxAttempts: 5, delayMs: 1 })
      @CircuitBreaker({ name: 'combo', failureThreshold: 2, resetTimeoutMs: 60_000 })
      async call(): Promise<string> {
        calls++;
        throw new Error('down');
      }
    }
    await expect(new Svc().call()).resolves.toBe('fallback');
    // Third attempt hit an open circuit -> Retry stopped early.
    expect(calls).toBe(2);
  });

  it('awaits the last value of an Observable-like return', async () => {
    const { of } = await import('rxjs');
    class Svc {
      @Retry()
      send() {
        return of(1, 2, 3);
      }
    }
    await expect(new Svc().send()).resolves.toBe(3);
  });
});
