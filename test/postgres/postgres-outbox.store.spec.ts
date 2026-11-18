/**
 * Integration tests against a real PostgreSQL.
 * Run with: PG_INTEGRATION=1 PG_URL=postgres://postgres@localhost:5432/postgres npm run test:pg
 */
import { Logger } from '@nestjs/common';
import { Pool } from 'pg';
import {
  NewOutboxMessage,
  OutboxMessage,
  OutboxModuleOptions,
  OutboxRelay,
  OutboxService,
  PostgresOutboxStore,
  SqlExecutor,
} from '../../src';

const enabled = process.env.PG_INTEGRATION === '1';
const d = enabled ? describe : describe.skip;
const TABLE = 'outbox_it.outbox_messages';

const msg = (id: string, extra: Partial<NewOutboxMessage> = {}): NewOutboxMessage => ({
  id,
  topic: 'orders',
  key: null,
  payload: { id },
  headers: {},
  availableAt: null,
  ...extra,
});

d('PostgresOutboxStore (integration)', () => {
  let pool: Pool;
  let store: PostgresOutboxStore;

  beforeAll(async () => {
    Logger.overrideLogger(false);
    pool = new Pool({ connectionString: process.env.PG_URL ?? 'postgres://postgres@127.0.0.1:5432/postgres', max: 10 });
    await pool.query('DROP SCHEMA IF EXISTS outbox_it CASCADE; CREATE SCHEMA outbox_it');
    store = new PostgresOutboxStore({ executor: pool, table: TABLE });
    await store.ensureSchema();
    await store.ensureSchema(); // idempotent
  });

  beforeEach(() => pool.query(`TRUNCATE ${TABLE}`));
  afterAll(() => pool?.end());

  const status = async (id: string) =>
    (await pool.query(`SELECT * FROM ${TABLE} WHERE id = $1`, [id])).rows[0];

  it('round-trips payload, headers and key; insert is idempotent', async () => {
    await store.add([msg('a', { key: 'k1', headers: { trace: 'x' }, payload: { n: 1, nested: [true, null] } })]);
    await store.add([msg('a')]);
    const [m] = await store.claim(10, 30_000, 'w1');
    expect(m).toMatchObject({ id: 'a', key: 'k1', headers: { trace: 'x' }, payload: { n: 1, nested: [true, null] }, attempts: 1 });
    expect(m.createdAt).toBeInstanceOf(Date);
    expect((await pool.query(`SELECT count(*)::int c FROM ${TABLE}`)).rows[0].c).toBe(1);
  });

  it('writes inside the caller transaction (rollback discards messages)', async () => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await store.add([msg('tx1')], client);
      // Not visible outside the transaction yet.
      expect(await store.claim(10, 30_000, 'w1')).toHaveLength(0);
      await client.query('ROLLBACK');
      await client.query('BEGIN');
      await store.add([msg('tx2')], client);
      await client.query('COMMIT');
    } finally {
      client.release();
    }
    expect((await store.claim(10, 30_000, 'w1')).map((m) => m.id)).toEqual(['tx2']);
  });

  it('handles large batches in one statement', async () => {
    const many = Array.from({ length: 12_000 }, (_, i) => msg(`m${i}`));
    await store.add(many);
    expect((await pool.query(`SELECT count(*)::int c FROM ${TABLE}`)).rows[0].c).toBe(12_000);
  });

  it('concurrent relays never claim the same message (SKIP LOCKED)', async () => {
    await store.add(Array.from({ length: 200 }, (_, i) => msg(`c${i}`)));
    const results = await Promise.all(
      Array.from({ length: 8 }, (_, w) => store.claim(40, 30_000, `w${w}`)),
    );
    const ids = results.flat().map((m) => m.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids.length).toBe(200);
  });

  it('re-claims expired leases and protects against stale owners', async () => {
    await store.add([msg('l1')]);
    await store.claim(10, 1, 'old');
    await new Promise((r) => setTimeout(r, 20));
    const [m] = await store.claim(10, 30_000, 'new');
    expect(m.attempts).toBe(2);
    await store.markPublished(['l1'], 'old'); // stale owner: ignored
    expect((await status('l1')).status).toBe('processing');
    await store.markPublished(['l1'], 'new');
    expect((await status('l1')).status).toBe('published');
  });

  it('markFailed schedules a retry or dead-letters; requeue and release work', async () => {
    await store.add([msg('f1'), msg('f2'), msg('f3')]);
    await store.claim(10, 30_000, 'w');
    await store.markFailed('f1', 'w', 'boom', 60_000);
    await store.markFailed('f2', 'w', 'x'.repeat(10_000), null);
    await store.release(['f3'], 'w');

    const f1 = await status('f1');
    expect(f1.status).toBe('pending');
    expect(new Date(f1.available_at).getTime()).toBeGreaterThan(Date.now() + 50_000);
    const f2 = await status('f2');
    expect(f2.status).toBe('failed');
    expect(f2.last_error).toHaveLength(4000);
    expect(await status('f3')).toMatchObject({ status: 'pending', attempts: 0 });

    expect((await store.claim(10, 30_000, 'w')).map((m) => m.id)).toEqual(['f3']);
    expect(await store.requeueFailed()).toBe(1);
    expect(await status('f2')).toMatchObject({ status: 'pending', attempts: 0, last_error: null });
  });

  it('respects availableAt', async () => {
    await store.add([msg('later', { availableAt: new Date(Date.now() + 60_000) }), msg('now')]);
    expect((await store.claim(10, 30_000, 'w')).map((m) => m.id)).toEqual(['now']);
  });

  it('orderByKey only releases the head of each key', async () => {
    const ordered = new PostgresOutboxStore({ executor: pool, table: TABLE, orderByKey: true });
    await ordered.add([
      msg('a1', { key: 'A' }), msg('a2', { key: 'A' }), msg('b1', { key: 'B' }), msg('n1'), msg('n2'),
    ]);
    const first = await ordered.claim(10, 30_000, 'w');
    expect(first.map((m) => m.id)).toEqual(['a1', 'b1', 'n1', 'n2']);
    // a1 still processing -> a2 blocked even for another worker.
    expect(await ordered.claim(10, 30_000, 'w2')).toHaveLength(0);
    await ordered.markFailed('a1', 'w', 'x', 0);
    // a1 retryable again; a2 still waits behind it.
    expect((await ordered.claim(10, 30_000, 'w')).map((m) => m.id)).toEqual(['a1']);
    await ordered.markPublished(['a1'], 'w');
    expect((await ordered.claim(10, 30_000, 'w')).map((m) => m.id)).toEqual(['a2']);
  });

  it('deletes published messages in chunks', async () => {
    await store.add(Array.from({ length: 5 }, (_, i) => msg(`d${i}`)));
    const claimed = await store.claim(10, 30_000, 'w');
    await store.markPublished(claimed.map((m) => m.id), 'w');
    expect(await store.deletePublishedBefore(new Date(Date.now() + 1_000), 3)).toBe(3);
    expect(await store.deletePublishedBefore(new Date(Date.now() + 1_000), 3)).toBe(2);
  });

  it('works with a TypeORM-style executor that returns plain row arrays', async () => {
    const typeormLike: SqlExecutor = { query: async (sql, params) => (await pool.query(sql, params)).rows };
    const s = new PostgresOutboxStore({ executor: typeormLike, table: TABLE });
    await s.add([msg('t1')]);
    const [m] = await s.claim(10, 30_000, 'w');
    expect(m.id).toBe('t1');
    await s.markPublished(['t1'], 'w');
    expect(await s.deletePublishedBefore(new Date(Date.now() + 1_000), 10)).toBe(1);
  });

  it('rejects unsafe table names', () => {
    expect(() => new PostgresOutboxStore({ executor: pool, table: 'x; DROP TABLE y' })).toThrow();
  });

  it('end-to-end: two relays publish every message exactly once (happy path)', async () => {
    const published: string[] = [];
    const publisher = {
      async publish(m: OutboxMessage) {
        await new Promise((r) => setTimeout(r, Math.random() * 3));
        published.push(m.id);
      },
    };
    const mk = (id: string) => {
      const options: OutboxModuleOptions = { store, publisher, relay: { workerId: id, pollIntervalMs: 20, batchSize: 25 } };
      return new OutboxRelay(options);
    };
    const r1 = mk('r1');
    const r2 = mk('r2');
    const service = new OutboxService({ store, publisher }, r1);
    await service.enqueue(Array.from({ length: 300 }, (_, i) => ({ id: `e${i}`, topic: 't', payload: i })));
    r1.start();
    r2.start();
    const deadline = Date.now() + 10_000;
    while (published.length < 300 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
    await Promise.all([r1.stop(), r2.stop()]);
    expect(published).toHaveLength(300);
    expect(new Set(published).size).toBe(300);
  });
});
