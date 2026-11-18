-- Transactional outbox table for PostgresOutboxStore (default table name).
-- Generated from postgresOutboxSchema(); copy into your migrations.
CREATE TABLE IF NOT EXISTS "outbox_messages" (
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
CREATE UNIQUE INDEX IF NOT EXISTS "outbox_messages_seq_idx" ON "outbox_messages" (seq);
CREATE INDEX IF NOT EXISTS "outbox_messages_pending_idx" ON "outbox_messages" (available_at, seq) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS "outbox_messages_processing_idx" ON "outbox_messages" (locked_until) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS "outbox_messages_key_idx" ON "outbox_messages" (partition_key, seq)
  WHERE partition_key IS NOT NULL AND status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS "outbox_messages_published_idx" ON "outbox_messages" (published_at) WHERE status = 'published';
