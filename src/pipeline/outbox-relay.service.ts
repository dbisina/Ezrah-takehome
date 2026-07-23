import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { KafkaService } from '../kafka/kafka.service';
import { EnvConfig } from '../config/env.config';

interface OutboxRow {
  id: string;
  topic: string;
  message_key: string;
  payload: unknown;
  headers: Record<string, string> | null;
}

/**
 * Drains the transactional outbox to Kafka.
 *
 * Two phases, deliberately separated so no Kafka network I/O ever happens inside
 * a database transaction (holding a `FOR UPDATE` lock across a slow broker send
 * would trip the transaction timeout, abort the batch, and stall draining):
 *
 *  1. Claim: one short transaction `UPDATE ... WHERE id IN (SELECT ... FOR
 *     UPDATE SKIP LOCKED)` atomically grabs a batch of due rows and leases them
 *     by pushing `visible_after` into the future. `SKIP LOCKED` lets any number
 *     of relay instances cooperate with no double-claim and no single point of
 *     failure. If this relay then dies, the lease expires and another relay
 *     re-drives the rows.
 *  2. Publish: each claimed row is sent to Kafka OUTSIDE any transaction, then
 *     marked published in its own one-statement update. A failed send simply
 *     leaves the row for the lease to expire and be retried.
 *
 * With the idempotent producer and idempotent consumers, the occasional
 * re-publish this allows is harmless: effectively-once over at-least-once.
 *
 * `visible_after` also gates each row, which is how delayed retries are honored.
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(OutboxRelay.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafka: KafkaService,
    private readonly env: EnvConfig,
  ) {}

  onApplicationBootstrap(): void {
    this.schedule();
    this.logger.log(`Outbox relay started (poll ${this.env.outboxPollIntervalMs}ms)`);
  }

  onApplicationShutdown(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
  }

  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      void this.drain().finally(() => this.schedule());
    }, this.env.outboxPollIntervalMs);
  }

  /** Publish one batch of due outbox rows. Public so tests can drive it directly. */
  async drain(): Promise<number> {
    if (this.running) return 0;
    this.running = true;
    try {
      // Phase 1: claim + lease a batch in one short transaction. No Kafka I/O here.
      const rows = await this.prisma.$transaction((tx) =>
        tx.$queryRaw<OutboxRow[]>`
          UPDATE outbox_message
          SET visible_after = now() + (${this.env.outboxClaimLeaseMs} * interval '1 millisecond'),
              attempts = attempts + 1
          WHERE id IN (
            SELECT id FROM outbox_message
            WHERE published_at IS NULL AND visible_after <= now()
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT ${this.env.outboxBatchSize}
          )
          RETURNING id, topic, message_key, payload, headers`,
      );

      // Phase 2: publish outside any transaction, mark each published on its own.
      let published = 0;
      for (const row of rows) {
        try {
          await this.kafka.publish(row.topic, row.message_key, row.payload, row.headers ?? undefined);
          await this.prisma.$executeRaw`UPDATE outbox_message SET published_at = now() WHERE id = ${row.id}::uuid`;
          published += 1;
        } catch (err) {
          // Leave the row unpublished; its lease expires and a relay re-drives it.
          this.logger.warn(`Publish failed for outbox ${row.id} -> ${row.topic}: ${asMessage(err)}`);
        }
      }
      return published;
    } catch (err) {
      this.logger.error(`Outbox drain error: ${asMessage(err)}`);
      return 0;
    } finally {
      this.running = false;
    }
  }
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
