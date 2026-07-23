import { Injectable } from '@nestjs/common';

function str(key: string, fallback: string): string {
  const v = process.env[key];
  return v === undefined || v === '' ? fallback : v;
}

function num(key: string, fallback: number): number {
  const v = process.env[key];
  if (v === undefined || v === '') return fallback;
  const n = Number(v);
  if (Number.isNaN(n)) throw new Error(`Env ${key} must be a number, got "${v}"`);
  return n;
}

function rate(key: string, fallback: number): number {
  const n = num(key, fallback);
  if (n < 0 || n > 1) throw new Error(`Env ${key} must be between 0 and 1, got ${n}`);
  return n;
}

/**
 * Typed, validated view of the environment. Reading env once at construction
 * keeps configuration honest (fail fast on bad values) and the rest of the code
 * free of `process.env` lookups.
 */
@Injectable()
export class EnvConfig {
  readonly grpcUrl = str('GRPC_URL', '0.0.0.0:50051');

  readonly kafkaBrokers = str('KAFKA_BROKERS', 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
  readonly kafkaClientId = str('KAFKA_CLIENT_ID', 'ezrah-credential-pipeline');
  readonly instanceId = str('APP_INSTANCE_ID', 'node-1');
  readonly stepTopicPartitions = num('STEP_TOPIC_PARTITIONS', 6);

  readonly identityFailureRate = rate('IDENTITY_FAILURE_RATE', 0.25);
  readonly signingFailureRate = rate('SIGNING_FAILURE_RATE', 0.25);
  readonly identityDelayMs = num('IDENTITY_DELAY_MS', 400);
  readonly signingDelayMs = num('SIGNING_DELAY_MS', 600);

  readonly maxStepAttempts = num('MAX_STEP_ATTEMPTS', 4);
  readonly retryBaseDelayMs = num('RETRY_BASE_DELAY_MS', 1000);
  readonly retryMaxDelayMs = num('RETRY_MAX_DELAY_MS', 15000);

  // A step stuck in RUNNING longer than the lease is assumed to belong to a
  // crashed consumer and is re-driven by the reaper.
  readonly stepLeaseMs = num('STEP_LEASE_MS', 60000);
  readonly reaperIntervalMs = num('REAPER_INTERVAL_MS', 15000);

  readonly outboxPollIntervalMs = num('OUTBOX_POLL_INTERVAL_MS', 400);
  readonly outboxBatchSize = num('OUTBOX_BATCH_SIZE', 100);
  // When a relay claims a batch it leases the rows (pushes visible_after this far
  // out) so other relays skip them; if this relay dies before publishing, the
  // lease expires and another relay re-drives. Must exceed a batch's publish time.
  readonly outboxClaimLeaseMs = num('OUTBOX_CLAIM_LEASE_MS', 30000);

  readonly callbackMaxAttempts = num('CALLBACK_MAX_ATTEMPTS', 5);
  readonly callbackTimeoutMs = num('CALLBACK_TIMEOUT_MS', 5000);
}
