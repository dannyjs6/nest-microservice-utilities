# @team/nest-microservice-utils

A small resilience + messaging toolkit for NestJS services:

- **Decorators:** `@Retry`, `@Timeout`, `@CircuitBreaker`, `@Bulkhead`, `@Fallback`. Each also has a plain-function form.
- **Transactional outbox:** `OutboxModule`, `OutboxService` and `OutboxRelay`, with a PostgreSQL store (works with `pg` or TypeORM) and an in-memory store for tests.

The only runtime dependencies are the peer dependencies `@nestjs/common` and `reflect-metadata`. The package doesn't depend on `@nestjs/microservices` or on any broker SDK. You plug in your own broker (Kafka, RabbitMQ, SQS, NATS…) by implementing one `publish()` method.

```bash
npm i @team/nest-microservice-utils
```

---

## 1. Resilience decorators

```ts
import { Injectable } from '@nestjs/common';
import { Retry, Timeout, CircuitBreaker, Bulkhead, Fallback, NonRetryableError } from '@team/nest-microservice-utils';

@Injectable()
export class PaymentsClient {
  @Fallback('cachedStatus')                                   // 5. degrade gracefully
  @Retry({ maxAttempts: 4, delayMs: 200 })                    // 4. retry the whole thing
  @CircuitBreaker({ name: 'payments-api', failureThreshold: 5, resetTimeoutMs: 30_000 }) // 3.
  @Bulkhead({ maxConcurrent: 20, maxQueue: 100 })             // 2. cap concurrency
  @Timeout(2_000)                                             // 1. per-attempt timeout
  async getStatus(paymentId: string): Promise<PaymentStatus> {
    const res = await this.http.get(`/payments/${paymentId}`);
    if (res.status === 404) throw new NonRetryableError('payment not found'); // never retried
    return res.data;
  }

  private cachedStatus(error: unknown, paymentId: string) {
    return this.cache.get(paymentId);
  }
}
```

### Decorator order matters

TypeScript applies decorators **bottom-up**: the decorator closest to the method wraps it first and runs innermost. Read the stack from the bottom up:

| You want | Put it |
|---|---|
| a timeout **per attempt** | `@Timeout` **below** `@Retry` |
| one timeout for **all attempts** | `@Timeout` **above** `@Retry` |
| every attempt counted by the breaker | `@CircuitBreaker` **below** `@Retry` (an open circuit stops the retries straight away) |
| a fallback that runs only after all retries fail | `@Fallback` at the **top** |

All decorators:

- keep NestJS metadata (`@MessagePattern`, `@EventPattern`, `SetMetadata`, guards…), whichever side of the Nest decorator you put them on;
- keep the method name and `this`;
- **always return a Promise**. A synchronous throw becomes a rejection. If the method returns an **Observable**, the decorator awaits its **last** emitted value.

### `@Retry(options)` / `retry(fn, options)`

| option | default | |
|---|---|---|
| `maxAttempts` | `3` | total attempts, **including** the first call |
| `delayMs` | `200` | base delay |
| `backoff` | `'exponential'` | `'fixed' \| 'linear' \| 'exponential'` |
| `factor` | `2` | exponential multiplier |
| `maxDelayMs` | `30_000` | cap for a single delay |
| `jitter` | `'full'` | `'full' \| 'equal' \| 'none'`; spreads retries so many instances don't retry at the same moment |
| `retryIf` | all | `(error, attempt) => boolean` |
| `onRetry` | – | logging/metrics hook. If it throws, the error is ignored. |
| `signal` | – | `AbortSignal` that cancels the remaining retries (function form only) |

These errors are never retried: `NonRetryableError`, any error with `retryable === false`, `CircuitOpenError` and `AbortedError`. When the last attempt fails, `retry` rethrows the **original** error, so `instanceof` checks still work.

```ts
const user = await retry(({ attempt, signal }) => fetch(url, { signal }).then((r) => r.json()), {
  maxAttempts: 5,
  retryIf: (e) => !(e instanceof HttpError && e.status < 500),
});
```

### `@Timeout(ms)` / `withTimeout(fn, ms, { signal? })`

Rejects with `TimeoutError`. The timer is always cleared. If the slow operation fails after the timeout has fired, that late rejection is swallowed, so it can't crash the process as an `unhandledRejection`.

A decorator **can't cancel** the work it wraps: after the timeout the call keeps running in the background. For real cancellation, use the function form and pass the signal down:

```ts
return withTimeout((signal) => fetch(url, { signal }), 2_000);
```

### `@CircuitBreaker(options)`

The breaker opens after `failureThreshold` failures in a row. It stays open for `resetTimeoutMs`, then lets `halfOpenMaxCalls` probe calls through. After `successThreshold` successful probes it closes again; any failed probe reopens it.

- State is **shared by every instance** of the class, including request-scoped ones, which would otherwise each get a fresh breaker that never opens. Use the same `name` on several methods to share one circuit for one downstream service. When circuits share a name, the options of whichever runs first apply.
- `isFailure(error)` decides which errors count against the circuit. For example, don't let 404s or validation errors open it.
- `onStateChange(state, prev, name)` is where to hook logs or metrics.
- To inspect circuits: `getCircuitBreaker(name)`, `listCircuitBreakers()` (e.g. for a `/health` endpoint), `breaker.reset()`.
- Always combine it with `@Timeout`. Otherwise a probe that hangs keeps the circuit half-open.

### `@Bulkhead({ maxConcurrent, maxQueue = 0, queueTimeoutMs?, name? })`

Caps how many calls run at once. Extra callers wait in a queue of up to `maxQueue`. Callers beyond that, or callers who wait longer than `queueTimeoutMs`, get a `BulkheadRejectedError`. The cap is shared by every instance of the class, or by every method with the same `name`.

### `@Fallback(handler | 'methodName', { when? })`

`handler(error, ...originalArgs)` runs with `this` bound to the instance, so it can use injected services. `when` limits the fallback to particular errors.

---

## 2. Transactional outbox

**The problem:** you save an order and then publish `orders.created`. If the process crashes between the two steps, the event is lost. If you publish first and the DB write then fails, you've announced an order that doesn't exist.

**The fix:** write the event to an `outbox_messages` table **in the same DB transaction** as the business data. A background relay reads that table and publishes to the broker. Delivery is **at-least-once**, so consumers must de-duplicate on `message.id`.

### Setup

1. Create the table. Copy [`sql/postgres-outbox.sql`](sql/postgres-outbox.sql) into a migration, or generate the SQL with `postgresOutboxSchema('my_schema.outbox')`. `store.ensureSchema()` also exists for dev and tests.

2. Implement a publisher for your broker:

```ts
import { Injectable } from '@nestjs/common';
import { OutboxMessage, OutboxPublisher, NonRetryableError } from '@team/nest-microservice-utils';

@Injectable()
export class KafkaOutboxPublisher implements OutboxPublisher {
  constructor(private readonly producer: Producer) {}

  async publish(message: OutboxMessage, signal: AbortSignal): Promise<void> {
    if (!KNOWN_TOPICS.has(message.topic)) throw new NonRetryableError(`unknown topic ${message.topic}`);
    await this.producer.send({
      topic: message.topic,
      messages: [{
        key: message.key ?? undefined,
        value: JSON.stringify(message.payload),
        headers: { ...message.headers, 'message-id': message.id }, // consumers dedupe on this
      }],
    });
  }
}
```

3. Register the module:

```ts
@Module({
  imports: [
    OutboxModule.forRootAsync({
      isGlobal: true,
      imports: [DatabaseModule, KafkaModule],
      inject: [PG_POOL, KafkaOutboxPublisher],
      useFactory: (pool: Pool, publisher: KafkaOutboxPublisher) => ({
        store: new PostgresOutboxStore({ executor: pool /*, orderByKey: true */ }),
        publisher,
        relay: {
          pollIntervalMs: 500,
          onDeadLetter: (msg, err) => alerts.warn('outbox dead letter', { id: msg.id, err }),
        },
      }),
    }),
  ],
})
export class AppModule {}
```

### Enqueue inside your transaction

```ts
// node-postgres
const client = await pool.connect();
try {
  await client.query('BEGIN');
  await client.query('INSERT INTO orders ...', [...]);
  await this.outbox.enqueue({ topic: 'orders.created', key: order.id, payload: order }, client);
  await client.query('COMMIT');
} catch (e) {
  await client.query('ROLLBACK');
  throw e;
} finally {
  client.release();
}
this.outbox.wake(); // optional: publish now instead of waiting for the next poll

// TypeORM: EntityManager / QueryRunner both have query(), so pass them directly
await this.dataSource.transaction(async (manager) => {
  await manager.save(order);
  await this.outbox.enqueue({ topic: 'orders.created', key: order.id, payload: order }, manager);
});
```

Any object with `query(sql, params)` works as the executor. That covers `pg` Pool, Client and PoolClient, and TypeORM DataSource, EntityManager and QueryRunner. For Prisma, write a one-line adapter: `{ query: (sql, params) => tx.$queryRawUnsafe(sql, ...params) }`.

### `enqueue(message | message[], tx?)`

| field | |
|---|---|
| `topic` | required; your publisher decides what it means |
| `payload` | must be JSON-serializable. This is checked **at enqueue time**, inside your transaction, so circular objects, BigInt or `undefined` fail fast. Dates become ISO strings. |
| `key` | partition key (e.g. the aggregate id). Used for Kafka keys and for `orderByKey`. |
| `headers` | `Record<string, string>` |
| `id` | defaults to a UUID. If you pass a deterministic id (e.g. `order-42-created`), enqueueing becomes idempotent: a duplicate id is ignored. |
| `availableAt` | delays delivery until this time |

### Relay options (`relay: {...}`)

| option | default | |
|---|---|---|
| `enabled` | `true` | set `false` on instances that only enqueue (e.g. run the relay only in a worker deployment) |
| `pollIntervalMs` | `1000` | how long to wait between polls when there was nothing to publish. While messages are flowing, the relay polls again immediately. |
| `batchSize` | `50` | |
| `concurrency` | `1` | how many messages from one batch are published in parallel |
| `leaseMs` | `30000` | how long a claim lasts; must be greater than `publishTimeoutMs` |
| `publishTimeoutMs` | `10000` | per `publish()` call; the `signal` passed to `publish()` aborts when it expires |
| `maxAttempts` | `10` | after this many attempts the message status becomes `failed` (dead letter) |
| `retryBaseDelayMs` / `retryMaxDelayMs` | `1000` / `300000` | exponential backoff with jitter between attempts |
| `retainPublishedMs` | 7 days | published rows older than this are deleted in 1000-row chunks; `0` disables cleanup |
| `cleanupIntervalMs` | 1 hour | |
| `onDeadLetter` | – | alerting hook |

### Guarantees and edge cases

| Situation | Behaviour |
|---|---|
| Transaction rolls back | Messages are rolled back too. |
| Several app instances run the relay | Safe. Claims use `FOR UPDATE SKIP LOCKED` plus a lease. |
| Relay crashes mid-publish | The lease expires and another relay re-delivers the message (at-least-once). |
| Relay process pauses (GC, CPU starvation) and comes back after its lease expired | Its late `markPublished`/`markFailed` calls are ignored, because every update checks `locked_by`. |
| A batch would outlive its lease | The relay stops starting new publishes and hands the rest of the batch back, without using up their attempts. |
| Broker down | Exponential backoff per message, then `failed` after `maxAttempts`. |
| Poison message | `NonRetryableError` sends it to `failed` straight away. A message that crashes the process still has its attempt counted, because attempts are incremented when the message is claimed. |
| Publish hangs | `publishTimeoutMs`, and the `AbortSignal` passed to `publish()` is aborted. |
| DB down while polling | The loop keeps running and backs off (up to 30s). |
| Published, but marking it published fails | The message is re-delivered later (at-least-once); this is logged. |
| App shutdown (`app.close()` / SIGTERM with `enableShutdownHooks()`) | Publishes already in flight finish. Messages claimed but not yet started are released. No new poll starts. |
| App and DB clocks differ | Doesn't matter: every lease and availability check uses the DB's `now()`. |
| Very large `enqueue` batch | A single `INSERT … SELECT unnest(...)` statement, so there's no Postgres parameter limit and the insert stays atomic. |
| Per-key ordering | Turn on `orderByKey: true` on the store. A message is claimed only after every earlier message with the same key is published or dead-lettered. A failing message **blocks its key** until it succeeds or is dead-lettered. Messages without a key are unaffected. |

Operations:

```ts
await outbox.requeueFailed();          // retry every dead-lettered message
await outbox.requeueFailed(['id-1']);  // or specific ones
```

```sql
-- what's stuck?
SELECT status, count(*), min(created_at) FROM outbox_messages GROUP BY status;
SELECT id, topic, attempts, last_error FROM outbox_messages WHERE status = 'failed';
```

### Testing your code

Use `InMemoryOutboxStore` with `relay: { enabled: false }`, then call `relay.processBatch()` to publish deterministically. Don't use the in-memory store in production: it has no transactions and loses everything on restart.

---

## Development

```bash
npm test                     # unit tests
npm run test:pg              # integration tests; needs Postgres at PG_URL
PG_URL=postgres://postgres@localhost:5432/postgres npm run test:pg
npm run build
```

### Known limitations (by design, to keep it small)

- Only PostgreSQL ships with the library. Another database means implementing `OutboxStore`, which is 7 methods; see `InMemoryOutboxStore` for the expected semantics.
- With `orderByKey`, a hot key publishes one message per poll round-trip.
- Ordering is by insert sequence. Two **concurrent** transactions writing the **same key** without locking the same row can commit in a different order from their sequence numbers. Lock the aggregate row (as you normally would) to make the order deterministic.
- Circuit breaker and bulkhead state lives in each process, not across the cluster.
