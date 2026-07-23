import {
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  Consumer,
  Kafka,
  Partitioners,
  Producer,
  logLevel,
} from 'kafkajs';
import { EnvConfig } from '../config/env.config';

/**
 * Owns the shared Kafka client and the single idempotent producer.
 *
 * `idempotent: true` + `acks: all` mean the producer never writes a duplicate or
 * reorders records for a given key on retry; important because the outbox relay
 * republishes on failure and same-request ordering must survive that.
 */
@Injectable()
export class KafkaService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaService.name);
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumers: Consumer[] = [];
  private producerReady = false;

  constructor(private readonly env: EnvConfig) {
    this.kafka = new Kafka({
      clientId: this.env.kafkaClientId,
      brokers: this.env.kafkaBrokers,
      logLevel: logLevel.NOTHING,
      retry: { retries: 8, initialRetryTime: 300 },
    });
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.producerReady = true;
    this.logger.log('Kafka producer connected');
  }

  async onApplicationShutdown(): Promise<void> {
    await Promise.allSettled(this.consumers.map((c) => c.disconnect()));
    if (this.producerReady) await this.producer.disconnect();
  }

  async publish(
    topic: string,
    key: string,
    value: unknown,
    headers?: Record<string, string>,
  ): Promise<void> {
    await this.producer.send({
      topic,
      acks: -1,
      messages: [{ key, value: JSON.stringify(value), headers }],
    });
  }

  /** Create (but do not start) a consumer; it is registered for clean shutdown. */
  createConsumer(groupId: string): Consumer {
    const consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      // We never sleep inside a handler (retries are scheduled via the outbox),
      // so the default max poll interval is safe.
      allowAutoTopicCreation: false,
    });
    this.consumers.push(consumer);
    return consumer;
  }

  admin() {
    return this.kafka.admin();
  }
}
