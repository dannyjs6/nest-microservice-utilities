import { DynamicModule, Module, Provider } from '@nestjs/common';
import { OUTBOX_OPTIONS } from './outbox.constants';
import type { OutboxModuleAsyncOptions, OutboxModuleOptions } from './outbox.interfaces';
import { OutboxRelay } from './outbox.relay';
import { OutboxService } from './outbox.service';

/**
 * @example
 * OutboxModule.forRootAsync({
 *   imports: [DatabaseModule, BrokerModule],
 *   inject: [PG_POOL, KafkaPublisher],
 *   useFactory: (pool: Pool, publisher: KafkaPublisher) => ({
 *     store: new PostgresOutboxStore({ executor: pool }),
 *     publisher,
 *     relay: { pollIntervalMs: 500, onDeadLetter: (m) => alerts.notify(m) },
 *   }),
 * })
 */
@Module({})
export class OutboxModule {
  static forRoot(options: OutboxModuleOptions & { isGlobal?: boolean }): DynamicModule {
    return this.build([{ provide: OUTBOX_OPTIONS, useValue: options }], [], options.isGlobal);
  }

  static forRootAsync(options: OutboxModuleAsyncOptions): DynamicModule {
    return this.build(
      [{ provide: OUTBOX_OPTIONS, useFactory: options.useFactory, inject: options.inject ?? [] }],
      options.imports ?? [],
      options.isGlobal,
    );
  }

  private static build(
    providers: Provider[],
    imports: DynamicModule['imports'],
    isGlobal = false,
  ): DynamicModule {
    return {
      module: OutboxModule,
      global: isGlobal,
      imports,
      providers: [...providers, OutboxRelay, OutboxService],
      exports: [OutboxService, OutboxRelay],
    };
  }
}
