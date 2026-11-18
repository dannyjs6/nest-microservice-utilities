import type { NewOutboxMessage, OutboxMessage, OutboxStatus, OutboxStore } from '../outbox.interfaces';

export interface InMemoryOutboxRecord extends OutboxMessage {
  seq: number;
  status: OutboxStatus;
  availableAt: number;
  lockedBy: string | null;
  lockedUntil: number | null;
  lastError: string | null;
  publishedAt: number | null;
}

/**
 * Non-durable store for unit tests and local development. It has the same semantics
 * as the Postgres store (leases, ordering, idempotent ids) but provides NO
 * transactional guarantees – do not use it in production.
 */
export class InMemoryOutboxStore implements OutboxStore<unknown> {
  readonly records = new Map<string, InMemoryOutboxRecord>();
  private seq = 0;

  constructor(
    private readonly options: { orderByKey?: boolean; now?: () => number } = {},
  ) {}

  private now(): number {
    return (this.options.now ?? Date.now)();
  }

  async add(messages: NewOutboxMessage[]): Promise<void> {
    const now = this.now();
    for (const m of messages) {
      if (this.records.has(m.id)) continue; // idempotent, like ON CONFLICT DO NOTHING
      this.records.set(m.id, {
        id: m.id,
        topic: m.topic,
        key: m.key,
        // Deep copy: mimics serialization so later mutation by the caller has no effect.
        payload: JSON.parse(JSON.stringify(m.payload)),
        headers: { ...m.headers },
        attempts: 0,
        createdAt: new Date(now),
        seq: ++this.seq,
        status: 'pending',
        availableAt: m.availableAt?.getTime() ?? now,
        lockedBy: null,
        lockedUntil: null,
        lastError: null,
        publishedAt: null,
      });
    }
  }

  async claim(limit: number, leaseMs: number, workerId: string): Promise<OutboxMessage[]> {
    const now = this.now();
    const all = [...this.records.values()].sort((a, b) => a.seq - b.seq);
    const blockedKeys = new Set<string>();
    const claimed: OutboxMessage[] = [];

    for (const r of all) {
      if (claimed.length >= limit) break;
      const due =
        (r.status === 'pending' && r.availableAt <= now) ||
        (r.status === 'processing' && (r.lockedUntil ?? 0) < now);
      const unfinished = r.status === 'pending' || r.status === 'processing';

      if (this.options.orderByKey && r.key !== null) {
        if (blockedKeys.has(r.key)) continue;
        if (unfinished) blockedKeys.add(r.key); // later messages with this key must wait
      }
      if (!due) continue;

      r.status = 'processing';
      r.lockedBy = workerId;
      r.lockedUntil = now + leaseMs;
      r.attempts += 1;
      claimed.push(this.snapshot(r));
    }
    return claimed;
  }

  async markPublished(ids: string[], workerId: string): Promise<void> {
    for (const id of ids) {
      const r = this.owned(id, workerId);
      if (!r) continue;
      r.status = 'published';
      r.publishedAt = this.now();
      r.lockedBy = r.lockedUntil = r.lastError = null;
    }
  }

  async markFailed(id: string, workerId: string, error: string, retryInMs: number | null): Promise<void> {
    const r = this.owned(id, workerId);
    if (!r) return;
    r.lastError = error;
    r.lockedBy = r.lockedUntil = null;
    if (retryInMs === null) {
      r.status = 'failed';
    } else {
      r.status = 'pending';
      r.availableAt = this.now() + retryInMs;
    }
  }

  async release(ids: string[], workerId: string): Promise<void> {
    for (const id of ids) {
      const r = this.owned(id, workerId);
      if (!r) continue;
      r.status = 'pending';
      r.lockedBy = r.lockedUntil = null;
      r.attempts = Math.max(0, r.attempts - 1);
    }
  }

  async requeueFailed(ids?: string[]): Promise<number> {
    let count = 0;
    for (const r of this.records.values()) {
      if (r.status !== 'failed' || (ids && !ids.includes(r.id))) continue;
      r.status = 'pending';
      r.attempts = 0;
      r.availableAt = this.now();
      r.lastError = null;
      count++;
    }
    return count;
  }

  async deletePublishedBefore(before: Date, limit: number): Promise<number> {
    let count = 0;
    for (const r of [...this.records.values()]) {
      if (count >= limit) break;
      if (r.status === 'published' && (r.publishedAt ?? 0) < before.getTime()) {
        this.records.delete(r.id);
        count++;
      }
    }
    return count;
  }

  /** Test helper. */
  byStatus(status: OutboxStatus): InMemoryOutboxRecord[] {
    return [...this.records.values()].filter((r) => r.status === status).sort((a, b) => a.seq - b.seq);
  }

  private owned(id: string, workerId: string): InMemoryOutboxRecord | undefined {
    const r = this.records.get(id);
    return r && r.status === 'processing' && r.lockedBy === workerId ? r : undefined;
  }

  private snapshot(r: InMemoryOutboxRecord): OutboxMessage {
    return {
      id: r.id,
      topic: r.topic,
      key: r.key,
      payload: JSON.parse(JSON.stringify(r.payload)),
      headers: { ...r.headers },
      attempts: r.attempts,
      createdAt: r.createdAt,
    };
  }
}
