import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { EventType, PipelineStep } from '@prisma/client';
import { KafkaService } from '../kafka/kafka.service';
import { EventBus } from '../events/event-bus.service';
import { EnvConfig } from '../config/env.config';
import { CONSUMER_GROUPS, TOPICS } from '../common/topics';
import { PipelineEventDto } from '../events/event.types';

/**
 * Per-node fan-out of the events topic into the local EventBus.
 *
 * The consumer group is unique per node (`cg.events-fanout.<instanceId>`), so
 * every node receives the FULL event stream; this is fan-out, not
 * load-balancing. That lets a Watch stream on any node see events produced by
 * consumers on any other node. Locally-produced events arrive both here and via
 * the fast local-commit path; the EventBus de-duplicates by eventId.
 */
@Injectable()
export class EventsFanoutConsumer implements OnApplicationBootstrap {
  private readonly logger = new Logger(EventsFanoutConsumer.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly bus: EventBus,
    private readonly env: EnvConfig,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    const groupId = `${CONSUMER_GROUPS.EVENTS_FANOUT_PREFIX}.${this.env.instanceId}`;
    const consumer = this.kafka.createConsumer(groupId);
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.EVENTS, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const dto = parseEvent(message.value?.toString());
        if (dto) this.bus.publish(dto);
      },
    });
    this.logger.log(`Fanning out ${TOPICS.EVENTS} as ${groupId}`);
  }
}

function parseEvent(raw: string | undefined): PipelineEventDto | null {
  if (!raw) return null;
  try {
    const p = JSON.parse(raw) as Record<string, unknown>;
    if (typeof p.eventId !== 'string' || typeof p.requestId !== 'string') return null;
    return {
      eventId: p.eventId,
      requestId: p.requestId,
      sequence: Number(p.sequence),
      type: p.type as EventType,
      step: (p.step as PipelineStep | null) ?? null,
      status: String(p.status),
      occurredAt: String(p.occurredAt),
      data: (p.data as Record<string, unknown>) ?? {},
    };
  } catch {
    return null;
  }
}
