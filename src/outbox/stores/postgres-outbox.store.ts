import type { NewOutboxMessage, OutboxMessage, OutboxStore } from '../outbox.interfaces';

/**
 * Anything with a `query(sql, params)` method:
 *  - `pg` Pool / PoolClient / Client           -> returns `{ rows }`
 *  - TypeORM DataSource / EntityManager / QueryRunner -> returns rows array
 *  - Knex (`knex.raw`) / Prisma need a tiny adapter: `{ query: (s, p) => prisma.$queryRawUnsafe(s, ...p) }`
 *
 * Only SELECT-shaped statements are used (writes are wrapped in CTEs) so the result
 * shape is the same for every driver.
 */
export interface SqlExecutor {
  query(sql: string, params?: unknown[]): Promise<unknown>;
}

export interface PostgresOutboxStoreOptions {
  /** Used when no transaction is passed (relay operations, non-transactional enqueue). */
  executor: SqlExecutor;
  /** Table name, optionally schema-qualified. Default: `outbox_messages`. */
  table?: string;
  /**
   * Guarantee per-key publish order: a message is only claimed when every earlier
   * message with the same `key` is published or dead-lettered. A failing message
   * therefore BLOCKS its key until it succeeds or reaches maxAttempts.
   * Messages with a null key are unaffected. Default: false.
   */
  orderByKey?: boolean;
}

const IDENT = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

function quoteTable(table: string): { qualified: string; base: string } {
  const parts = table.split('.');
  if (parts.length > 2 || !parts.every((p) => IDENT.test(p))) {
    throw new Error(`Invalid outbox table name: "${table}"`);
  }
  return { qualified: parts.map((p) => `"${p}"`).join('.'), base: parts[parts.length - 1] };
}

/** DDL for the outbox table. Also shipped as `sql/postgres-outbox.sql`. */
export function postgresOutboxSchema(table = 'outbox_messages'): string {
  const { qualified: t, base } = quoteTable(table);
  return `
CREATE TABLE IF NOT EXISTS ${t} (
  seq           BIGINT GENERATED ALWAYS AS IDENTITY,
  id            TEXT PRIMARY KEY,
  topic         TEXT NOT NULL,
  partition_key TEXT NULL,
  payload       JSONB NOT NULL,
  headers       JSONB NOT NULL DEFAULT '{}'::jsonb,
  status        TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'processing', 'published', 'failed')),
  attempts      INTEGER NOT NULL DEFAULT 0,
  available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_by     TEXT NULL,
  locked_until  TIMESTAMPTZ NULL,
  last_error    TEXT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "${base}_seq_idx" ON ${t} (seq);
CREATE INDEX IF NOT EXISTS "${base}_pending_idx" ON ${t} (available_at, seq) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS "${base}_processing_idx" ON ${t} (locked_until) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS "${base}_key_idx" ON ${t} (partition_key, seq)
  WHERE partition_key IS NOT NULL AND status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS "${base}_published_idx" ON ${t} (published_at) WHERE status = 'published';
`.trim();
}

function rowsOf(result: unknown): any[] {
  if (Array.isArray(result)) return result;
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? rows : [];
}

function toMessage(row: any): OutboxMessage {
  return {
    id: row.id,
    topic: row.topic,
    key: row.partition_key ?? null,
    payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
    headers: (typeof row.headers === 'string' ? JSON.parse(row.headers) : row.headers) ?? {},
    attempts: Number(row.attempts),
    createdAt: row.created_at instanceof Date ? row.created_at : new Date(row.created_at),
  };
}

/**
 * PostgreSQL outbox store.
 *
 * - Concurrent relays are safe: claims use `FOR UPDATE SKIP LOCKED` plus a lease.
 * - All time comparisons use the DATABASE clock, so app-server clock skew is irrelevant.
 * - Inserts are a single statement (`unnest`) regardless of batch size, and idempotent
 *   on `id` (`ON CONFLICT DO NOTHING`).
 */
export class PostgresOutboxStore implements OutboxStore<SqlExecutor> {
  private readonly t: string;
  private readonly executor: SqlExecutor;
  private readonly orderByKey: boolean;
  readonly schemaSql: string;

  constructor(options: PostgresOutboxStoreOptions) {
    const table = options.table ?? 'outbox_messages';
    this.t = quoteTable(table).qualified;
    this.executor = options.executor;
    this.orderByKey = options.orderByKey ?? false;
    this.schemaSql = postgresOutboxSchema(table);
  }

  /** Creates the table and indexes if missing. Prefer real migrations in production. */
  async ensureSchema(): Promise<void> {
    await this.executor.query(this.schemaSql);
  }

  async add(messages: NewOutboxMessage[], tx?: SqlExecutor): Promise<void> {
    if (messages.length === 0) return;
    const db = tx ?? this.executor;
    await db.query(
      `WITH ins AS (
         INSERT INTO ${this.t} (id, topic, partition_key, payload, headers, available_at)
         SELECT m.id, m.topic, m.partition_key, m.payload::jsonb, m.headers::jsonb,
                COALESCE(m.available_at, now())
         FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
           AS m(id, topic, partition_key, payload, headers, available_at)
         ON CONFLICT (id) DO NOTHING
         RETURNING 1
       ) SELECT count(*)::int AS count FROM ins`,
      [
        messages.map((m) => m.id),
        messages.map((m) => m.topic),
        messages.map((m) => m.key),
        messages.map((m) => JSON.stringify(m.payload)),
        messages.map((m) => JSON.stringify(m.headers)),
        messages.map((m) => m.availableAt),
      ],
    );
  }

  async claim(limit: number, leaseMs: number, workerId: string): Promise<OutboxMessage[]> {
    const ordering = this.orderByKey
      ? `AND (o.partition_key IS NULL OR NOT EXISTS (
           SELECT 1 FROM ${this.t} p
           WHERE p.partition_key = o.partition_key AND p.seq < o.seq
             AND p.status IN ('pending', 'processing')))`
      : '';
    const result = await this.executor.query(
      `WITH candidates AS (
         SELECT o.id FROM ${this.t} o
         WHERE ((o.status = 'pending' AND o.available_at <= now())
             OR (o.status = 'processing' AND o.locked_until < now()))
           ${ordering}
         ORDER BY o.seq
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       ), claimed AS (
         UPDATE ${this.t} o
         SET status = 'processing',
             locked_by = $2,
             locked_until = now() + make_interval(secs => $3::double precision / 1000),
             attempts = o.attempts + 1
         FROM candidates c
         WHERE o.id = c.id
         RETURNING o.*
       ) SELECT * FROM claimed ORDER BY seq`,
      [limit, workerId, leaseMs],
    );
    return rowsOf(result).map(toMessage);
  }

  async markPublished(ids: string[], workerId: string): Promise<void> {
    if (ids.length === 0) return;
    await this.executor.query(
      `WITH u AS (
         UPDATE ${this.t}
         SET status = 'published', published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
         WHERE id = ANY($1::text[]) AND locked_by = $2 AND status = 'processing'
         RETURNING 1
       ) SELECT count(*)::int AS count FROM u`,
      [ids, workerId],
    );
  }

  async markFailed(id: string, workerId: string, error: string, retryInMs: number | null): Promise<void> {
    await this.executor.query(
      `WITH u AS (
         UPDATE ${this.t}
         SET status = CASE WHEN $4::double precision IS NULL THEN 'failed' ELSE 'pending' END,
             available_at = CASE WHEN $4::double precision IS NULL THEN available_at
                                 ELSE now() + make_interval(secs => $4::double precision / 1000) END,
             last_error = $3, locked_by = NULL, locked_until = NULL
         WHERE id = $1 AND locked_by = $2 AND status = 'processing'
         RETURNING 1
       ) SELECT count(*)::int AS count FROM u`,
      [id, workerId, error.slice(0, 4000), retryInMs],
    );
  }

  async release(ids: string[], workerId: string): Promise<void> {
    if (ids.length === 0) return;
    await this.executor.query(
      `WITH u AS (
         UPDATE ${this.t}
         SET status = 'pending', locked_by = NULL, locked_until = NULL, attempts = GREATEST(attempts - 1, 0)
         WHERE id = ANY($1::text[]) AND locked_by = $2 AND status = 'processing'
         RETURNING 1
       ) SELECT count(*)::int AS count FROM u`,
      [ids, workerId],
    );
  }

  async requeueFailed(ids?: string[]): Promise<number> {
    const result = await this.executor.query(
      `WITH u AS (
         UPDATE ${this.t}
         SET status = 'pending', attempts = 0, available_at = now(), last_error = NULL
         WHERE status = 'failed' AND ($1::text[] IS NULL OR id = ANY($1::text[]))
         RETURNING 1
       ) SELECT count(*)::int AS count FROM u`,
      [ids ?? null],
    );
    return Number(rowsOf(result)[0]?.count ?? 0);
  }

  async deletePublishedBefore(before: Date, limit: number): Promise<number> {
    // Deleted in bounded chunks to avoid long locks / huge WAL bursts.
    const result = await this.executor.query(
      `WITH d AS (
         DELETE FROM ${this.t}
         WHERE id IN (
           SELECT id FROM ${this.t}
           WHERE status = 'published' AND published_at < $1
           LIMIT $2
         )
         RETURNING 1
       ) SELECT count(*)::int AS count FROM d`,
      [before, limit],
    );
    return Number(rowsOf(result)[0]?.count ?? 0);
  }
}
