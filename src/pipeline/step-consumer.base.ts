import { Logger, OnApplicationBootstrap } from '@nestjs/common';
import { CredentialRequest, PipelineStep } from '@prisma/client';
import { KafkaService } from '../kafka/kafka.service';
import { PrismaService } from '../prisma/prisma.service';
import { EnvConfig } from '../config/env.config';
import { EventBus } from '../events/event-bus.service';
import { TransitionService, isUniqueViolation } from './transition.service';
import { SignedCredential } from '../domain/credential';
import { TOPICS } from '../common/topics';
import { PipelineEventDto } from '../events/event.types';
import { asMessage, backoffMs } from '../common/util';

export type StepOutcome =
  | { ok: true; credential?: SignedCredential }
  | { ok: false; transient: boolean; reason: string };

/**
 * Shared machinery for the three step consumers. The flow is identical for every
 * step; only `doWork` differs.
 *
 * Invariants enforced here:
 *  - The offset always advances. Poison messages go to the DLQ and are acked;
 *    only genuine infra errors rethrow (so Kafka redelivers).
 *  - A step is claimed atomically before any work; a non-claim is a no-op.
 *  - `doWork` is wrapped, so an unexpected throw becomes a transient failure
 *    (retried) rather than a stuck partition.
 */
export abstract class AbstractStepConsumer implements OnApplicationBootstrap {
  protected abstract readonly step: PipelineStep;
  protected abstract readonly topic: string;
  protected abstract readonly groupId: string;
  protected readonly logger = new Logger(this.constructor.name);

  protected constructor(
    protected readonly transition: TransitionService,
    protected readonly prisma: PrismaService,
    protected readonly kafka: KafkaService,
    protected readonly bus: EventBus,
    protected readonly env: EnvConfig,
  ) {}

  protected abstract doWork(request: CredentialRequest): Promise<StepOutcome>;

  async onApplicationBootstrap(): Promise<void> {
    const consumer = this.kafka.createConsumer(this.groupId);
    await consumer.connect();
    await consumer.subscribe({ topic: this.topic, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const requestId = parseRequestId(message.value?.toString());
        if (!requestId) {
          await this.parkPoison(message.value?.toString(), 'unparseable step command');
          return;
        }
        try {
          await this.handle(requestId);
        } catch (err) {
          if (isUniqueViolation(err)) return; // duplicate delivery already applied
          throw err; // infra error: let Kafka redeliver; the reaper is the backstop
        }
      },
    });
    this.logger.log(`Consuming ${this.topic} as ${this.groupId}`);
  }

  private async handle(requestId: string): Promise<void> {
    const claim = await this.transition.claimStep(requestId, this.step);
    this.emit(claim.events);
    if (!claim.claimed) return; // duplicate delivery, already succeeded, or terminal

    const request = await this.prisma.credentialRequest.findUnique({ where: { id: requestId } });
    if (!request) return;

    let outcome: StepOutcome;
    try {
      outcome = await this.doWork(request);
    } catch (err) {
      outcome = { ok: false, transient: true, reason: `unexpected error: ${asMessage(err)}` };
    }

    if (outcome.ok) {
      const r = await this.transition.succeedStep(requestId, this.step, claim.attempt, {
        signedCredential: outcome.credential,
      });
      this.emit(r.events);
      return;
    }

    if (outcome.transient && claim.attempt < this.env.maxStepAttempts) {
      const delay = backoffMs(claim.attempt, this.env.retryBaseDelayMs, this.env.retryMaxDelayMs);
      const r = await this.transition.scheduleRetry(requestId, this.step, claim.attempt, outcome.reason, delay);
      this.emit(r.events);
      this.logger.warn(`${this.step} attempt ${claim.attempt} failed, retry in ${delay}ms: ${outcome.reason}`);
      return;
    }

    // Deterministic (business) failures are not poison: no DLQ. Exhausted
    // transient failures are parked in the DLQ for inspection.
    const r = await this.transition.failRequest(requestId, this.step, claim.attempt, outcome.reason, {
      transient: outcome.transient,
      toDlq: outcome.transient,
    });
    this.emit(r.events);
    this.logger.warn(`${this.step} failed terminally: ${outcome.reason}`);
  }

  private async parkPoison(raw: string | undefined, reason: string): Promise<void> {
    try {
      await this.kafka.publish(TOPICS.DLQ, 'poison', { reason, topic: this.topic, raw: raw ?? null });
      this.logger.warn(`Parked poison message from ${this.topic}: ${reason}`);
    } catch (err) {
      this.logger.error(`Failed to DLQ poison message: ${asMessage(err)}`);
    }
  }

  protected emit(events: PipelineEventDto[]): void {
    for (const e of events) this.bus.publish(e);
  }
}

function parseRequestId(raw: string | undefined): string | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as { requestId?: unknown };
    return typeof parsed.requestId === 'string' && parsed.requestId ? parsed.requestId : null;
  } catch {
    return null;
  }
}
