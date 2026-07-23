import { Kafka, logLevel } from 'kafkajs';
import { EnvConfig } from '../config/env.config';
import { TOPICS } from '../common/topics';

/**
 * Provisions all Kafka topics with explicit partition counts. Run once by the
 * init container BEFORE the app starts, so topology is deterministic and
 * auto-creation (which would silently collapse a topic to one partition) can
 * stay disabled on the broker.
 *
 * Command topics are keyed by requestId and get `STEP_TOPIC_PARTITIONS`
 * partitions; their partition count is fixed for life (raising it would remap
 * hash(requestId) and break per-request ordering). Events likewise keyed by
 * requestId. Callback/DLQ carry independent messages and get fewer partitions.
 */
async function main(): Promise<void> {
  const env = new EnvConfig();
  const kafka = new Kafka({
    clientId: `${env.kafkaClientId}-provisioner`,
    brokers: env.kafkaBrokers,
    logLevel: logLevel.NOTHING,
    retry: { retries: 20, initialRetryTime: 500 },
  });
  const admin = kafka.admin();
  await admin.connect();

  const p = env.stepTopicPartitions;
  const topics = [
    { topic: TOPICS.IDENTITY_VERIFY, numPartitions: p },
    { topic: TOPICS.CLAIMS_VALIDATE, numPartitions: p },
    { topic: TOPICS.SIGN, numPartitions: p },
    { topic: TOPICS.EVENTS, numPartitions: p },
    { topic: TOPICS.CALLBACK, numPartitions: 3 },
    { topic: TOPICS.DLQ, numPartitions: 3 },
  ].map((t) => ({
    ...t,
    replicationFactor: 1,
    configEntries: [{ name: 'retention.ms', value: String(7 * 24 * 60 * 60 * 1000) }],
  }));

  const created = await admin.createTopics({ topics, waitForLeaders: true });
  await admin.disconnect();

  // eslint-disable-next-line no-console
  console.log(
    created
      ? `Provisioned topics: ${topics.map((t) => `${t.topic}(${t.numPartitions})`).join(', ')}`
      : 'Topics already existed; nothing to do',
  );
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Topic provisioning failed', err);
  process.exit(1);
});
