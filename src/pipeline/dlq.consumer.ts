import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { KafkaService } from '../kafka/kafka.service';
import { CONSUMER_GROUPS, TOPICS } from '../common/topics';

/**
 * Dead-letter monitor. The DLQ is terminal: messages here are logged for
 * operator visibility and never auto-replayed into a step topic (that would
 * risk a poison-message loop). In a real deployment this would raise an alert
 * and expose the parked messages for manual inspection / re-drive.
 */
@Injectable()
export class DlqConsumer implements OnApplicationBootstrap {
  private readonly logger = new Logger(DlqConsumer.name);

  constructor(private readonly kafka: KafkaService) {}

  async onApplicationBootstrap(): Promise<void> {
    const consumer = this.kafka.createConsumer(CONSUMER_GROUPS.DLQ);
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.DLQ, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        this.logger.error(`DLQ [key=${message.key?.toString() ?? 'null'}]: ${message.value?.toString() ?? ''}`);
      },
    });
    this.logger.log(`Monitoring ${TOPICS.DLQ} as ${CONSUMER_GROUPS.DLQ}`);
  }
}
