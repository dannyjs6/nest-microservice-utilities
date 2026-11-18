import { Logger } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  InMemoryOutboxStore,
  NonRetryableError,
  OutboxMessage,
  OutboxModule,
  OutboxModuleOptions,
  OutboxPublisher,
  OutboxRelay,
  OutboxService,
} from '../src';

const tick = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

class FakePublisher implements OutboxPublisher {
  published: OutboxMessage[] = [];
  impl: (m: OutboxMessage, signal: AbortSignal) => Promise<void> = async () => undefined;
  async publish(m: OutboxMessage, signal: AbortSignal) {
    await this.impl(m, signal);
    this.published.push(m);
  }
}

function setup(relay: OutboxModuleOptions['relay'] = {}, storeOpts = {}) {
  const store = new InMemoryOutboxStore(storeOpts);
  const publisher = new FakePublisher();
  const options: OutboxModuleOptions = {
    store,
    publisher,
    relay: { enabled: false, retryBaseDelayMs: 0, workerId: 'w1', ...relay },
  };
  const relayInstance = new OutboxRelay(options);
  const service = new OutboxService(options, relayInstance);
  return { store, publisher, relay: relayInstance, service };
}

beforeAll(() => Logger.overrideLogger(false));

describe('OutboxService.enqueue', () => {
  it('stores messages and returns ids', async () => {
    const { service, store } = setup();
    const ids = await service.enqueue([
      { topic: 'a', payload: { x: 1 } },
      { topic: 'b', payload: [1, 2], key: 'k', headers: { trace: 't' } },
    ]);
    expect(ids).toHaveLength(2);
    expect(store.byStatus('pending').map((r) => r.topic)).toEqual(['a', 'b']);
  });

  it('is idempotent on explicit ids', async () => {
    const { service, store } = setup();
    await service.enqueue({ id: 'order-1-created', topic: 'a', payload: 1 });
    await service.enqueue({ id: 'order-1-created', topic: 'a', payload: 1 });
    expect(store.records.size).toBe(1);
  });

  it.each([
    ['circular', (() => { const o: any = {}; o.self = o; return o; })()],
    ['bigint', { n: BigInt(1) }],
    ['undefined', undefined],
    ['function', () => 1],
  ])('rejects non-serializable payload (%s) at enqueue time', async (_n, payload) => {
    const { service } = setup();
    await expect(service.enqueue({ topic: 'a', payload })).rejects.toThrow(/JSON-serializable/);
  });

  it('validates topic, headers and duplicate ids in one call', async () => {
    const { service } = setup();
    await expect(service.enqueue({ topic: '', payload: 1 })).rejects.toThrow(/topic/);
    await expect(service.enqueue({ topic: 'a', payload: 1, headers: { n: 1 as any } })).rejects.toThrow(/header/);
    await expect(
      service.enqueue([{ id: 'x', topic: 'a', payload: 1 }, { id: 'x', topic: 'a', payload: 2 }]),
    ).rejects.toThrow(/duplicate/);
  });

  it('snapshots the payload (later caller mutations are not published)', async () => {
    const { service, relay, publisher } = setup();
    const payload = { status: 'created' };
    await service.enqueue({ topic: 'a', payload });
    payload.status = 'mutated';
    await relay.processBatch();
    expect(publisher.published[0].payload).toEqual({ status: 'created' });
  });
});

describe('OutboxRelay', () => {
  it('publishes in order and marks messages published', async () => {
    const { service, relay, publisher, store } = setup();
    await service.enqueue([1, 2, 3].map((n) => ({ topic: 't', payload: n })));
    expect(await relay.processBatch()).toBe(3);
    expect(publisher.published.map((m) => m.payload)).toEqual([1, 2, 3]);
    expect(store.byStatus('published')).toHaveLength(3);
    expect(await relay.processBatch()).toBe(0);
  });

  it('respects delayed messages (availableAt)', async () => {
    const { service, relay, publisher } = setup();
    await service.enqueue({ topic: 't', payload: 1, availableAt: new Date(Date.now() + 60_000) });
    await relay.processBatch();
    expect(publisher.published).toHaveLength(0);
  });

  it('retries with backoff then dead-letters after maxAttempts', async () => {
    const onDeadLetter = jest.fn();
    const { service, relay, publisher, store } = setup({ maxAttempts: 3, onDeadLetter });
    publisher.impl = async () => { throw new Error('broker down'); };
    await service.enqueue({ topic: 't', payload: 1 });

    for (let i = 0; i < 5; i++) await relay.processBatch();

    const [dead] = store.byStatus('failed');
    expect(dead.attempts).toBe(3);
    expect(dead.lastError).toBe('broker down');
    expect(onDeadLetter).toHaveBeenCalledTimes(1);

    // Requeue brings it back with a fresh attempt budget.
    publisher.impl = async () => undefined;
    expect(await service.requeueFailed()).toBe(1);
    await relay.processBatch();
    expect(store.byStatus('published')).toHaveLength(1);
  });

  it('dead-letters immediately on NonRetryableError', async () => {
    const { service, relay, publisher, store } = setup({ maxAttempts: 10 });
    publisher.impl = async () => { throw new NonRetryableError('schema mismatch'); };
    await service.enqueue({ topic: 't', payload: 1 });
    await relay.processBatch();
    expect(store.byStatus('failed')).toHaveLength(1);
  });

  it('times out a hanging publish and aborts its signal', async () => {
    let signal!: AbortSignal;
    const { service, relay, publisher, store } = setup({ publishTimeoutMs: 20, leaseMs: 1_000 });
    publisher.impl = (_m, s) => { signal = s; return new Promise(() => undefined); };
    await service.enqueue({ topic: 't', payload: 1 });
    await relay.processBatch();
    expect(signal.aborted).toBe(true);
    const [r] = store.byStatus('pending');
    expect(r.lastError).toMatch(/timed out/);
  });

  it('re-delivers messages whose lease expired (crashed relay)', async () => {
    let now = 1_000_000;
    const store = new InMemoryOutboxStore({ now: () => now });
    await store.add([{ id: 'm1', topic: 't', key: null, payload: 1, headers: {}, availableAt: null }]);
    const [claimed] = await store.claim(10, 5_000, 'crashed-worker');
    expect(claimed.attempts).toBe(1);
    expect(await store.claim(10, 5_000, 'w2')).toHaveLength(0); // still leased
    now += 5_001;
    const [reclaimed] = await store.claim(10, 5_000, 'w2');
    expect(reclaimed.attempts).toBe(2);
    // The crashed worker coming back must not overwrite the new owner's state.
    await store.markPublished(['m1'], 'crashed-worker');
    expect(store.records.get('m1')!.status).toBe('processing');
  });

  it('releases messages it cannot start before the lease runs out', async () => {
    // lease 300ms, publish timeout 100ms -> publishes may only start in the first ~170ms.
    const { service, relay, publisher, store } = setup({ leaseMs: 300, publishTimeoutMs: 100 });
    publisher.impl = () => tick(100);
    await service.enqueue([1, 2, 3, 4].map((n) => ({ topic: 't', payload: n })));
    await relay.processBatch();
    expect(publisher.published.length).toBeGreaterThanOrEqual(1);
    expect(publisher.published.length).toBeLessThan(4);
    const pending = store.byStatus('pending');
    expect(pending.length).toBe(4 - publisher.published.length);
    expect(pending.every((r) => r.attempts === 0)).toBe(true); // release does not burn an attempt
  });

  it('orderByKey: a failing message blocks later messages with the same key only', async () => {
    const { service, relay, publisher, store } = setup({ maxAttempts: 2 }, { orderByKey: true });
    publisher.impl = async (m) => { if (m.payload === 'a1') throw new Error('fail'); };
    await service.enqueue([
      { topic: 't', key: 'A', payload: 'a1' },
      { topic: 't', key: 'A', payload: 'a2' },
      { topic: 't', key: 'B', payload: 'b1' },
      { topic: 't', key: null, payload: 'n1' },
    ]);
    await relay.processBatch();
    expect(publisher.published.map((m) => m.payload)).toEqual(['b1', 'n1']);
    await relay.processBatch(); // a1 fails a 2nd time -> dead-lettered
    expect(store.byStatus('failed').map((r) => r.payload)).toEqual(['a1']);
    await relay.processBatch(); // now a2 is unblocked
    expect(publisher.published.map((m) => m.payload)).toEqual(['b1', 'n1', 'a2']);
  });

  it('publishes concurrently when configured', async () => {
    let active = 0;
    let peak = 0;
    const { service, relay, publisher } = setup({ concurrency: 3 });
    publisher.impl = async () => { active++; peak = Math.max(peak, active); await tick(10); active--; };
    await service.enqueue(Array.from({ length: 9 }, (_, i) => ({ topic: 't', payload: i })));
    await relay.processBatch();
    expect(peak).toBe(3);
    expect(publisher.published).toHaveLength(9);
  });

  it('a markPublished failure does not count as a publish failure', async () => {
    const { service, relay, store } = setup();
    jest.spyOn(store, 'markPublished').mockRejectedValueOnce(new Error('db down'));
    const markFailed = jest.spyOn(store, 'markFailed');
    await service.enqueue({ topic: 't', payload: 1 });
    await relay.processBatch();
    expect(markFailed).not.toHaveBeenCalled();
  });

  it('loop: survives store errors, wakes up on demand, and stops gracefully', async () => {
    const { service, relay, publisher, store } = setup({ pollIntervalMs: 60_000 });
    const claim = jest.spyOn(store, 'claim').mockRejectedValueOnce(new Error('connection reset'));
    relay.start();
    await tick(10);
    expect(claim).toHaveBeenCalledTimes(1);

    // Error backoff is 60s, but wake() cuts it short.
    await service.enqueue({ topic: 't', payload: 'hello' });
    service.wake();
    await tick(20);
    expect(publisher.published.map((m) => m.payload)).toEqual(['hello']);

    // Graceful stop waits for the in-flight publish.
    let finished = false;
    publisher.impl = async () => { await tick(30); finished = true; };
    await service.enqueue({ topic: 't', payload: 'last' });
    service.wake();
    await tick(5);
    await relay.stop();
    expect(finished).toBe(true);
    expect(relay.isRunning).toBe(false);
  });

  it('stop releases messages that were claimed but not started', async () => {
    const { service, relay, publisher, store } = setup({ pollIntervalMs: 10 });
    publisher.impl = () => tick(30);
    await service.enqueue([1, 2, 3].map((n) => ({ topic: 't', payload: n })));
    relay.start();
    await tick(10);
    await relay.stop();
    expect(publisher.published).toHaveLength(1);
    expect(store.byStatus('pending')).toHaveLength(2);
    expect(store.byStatus('processing')).toHaveLength(0);
  });

  it('cleanup deletes old published messages only', async () => {
    const { service, relay, store } = setup({ retainPublishedMs: 1 });
    await service.enqueue([{ topic: 't', payload: 1 }, { topic: 't', payload: 2, availableAt: new Date(Date.now() + 60_000) }]);
    await relay.processBatch();
    await tick(5);
    expect(await relay.cleanup()).toBe(1);
    expect(store.records.size).toBe(1);
  });

  it('validates lease vs publish timeout', () => {
    expect(() => setup({ leaseMs: 1_000, publishTimeoutMs: 1_000 })).toThrow(/leaseMs/);
  });
});

describe('OutboxModule', () => {
  it('wires everything with forRootAsync and runs the relay with the app lifecycle', async () => {
    const store = new InMemoryOutboxStore();
    const publisher = new FakePublisher();
    const moduleRef = await Test.createTestingModule({
      imports: [
        OutboxModule.forRootAsync({
          useFactory: async () => ({ store, publisher, relay: { pollIntervalMs: 10 } }),
        }),
      ],
    }).compile();
    const app = await moduleRef.init();

    const outbox = app.get(OutboxService);
    const relay = app.get(OutboxRelay);
    expect(relay.isRunning).toBe(true);

    await outbox.enqueue({ topic: 'orders.created', payload: { id: 1 } });
    await tick(50);
    expect(publisher.published).toHaveLength(1);

    await app.close();
    expect(relay.isRunning).toBe(false);
  });

  it('does not start the relay when disabled', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        OutboxModule.forRoot({
          store: new InMemoryOutboxStore(),
          publisher: new FakePublisher(),
          relay: { enabled: false },
        }),
      ],
    }).compile();
    const app = await moduleRef.init();
    expect(app.get(OutboxRelay).isRunning).toBe(false);
    await app.close();
  });
});
