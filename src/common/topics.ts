import { PipelineStep } from '@prisma/client';

/**
 * Kafka topology.
 *
 * One topic per pipeline step. Every message is keyed by requestId, so all of a
 * request's messages hash to the same partition and are therefore processed in
 * order by a single consumer; that is what preserves per-request step ordering
 * without any global coordination. Each step has its own consumer group so the
 * steps scale independently and per-step lag is observable.
 *
 * `.v1` in every name is deliberate: keyed command topics have a fixed partition
 * count (raising it live would remap hash(requestId) and let two consumers work
 * the same request), so we version-and-migrate rather than repartition.
 */
export const TOPICS = {
  IDENTITY_VERIFY: 'credential.identity.verify.v1',
  CLAIMS_VALIDATE: 'credential.claims.validate.v1',
  SIGN: 'credential.sign.v1',
  // Fan-out of every pipeline event. Keyed by requestId so a request's events
  // stay ordered on one partition. External services and the per-node Watch
  // fan-out consumer subscribe here.
  EVENTS: 'credential.events.v1',
  // Terminal outcome delivery to a caller's callback URL.
  CALLBACK: 'credential.callback.v1',
  // Dead-letter park for poison / unrecoverable messages. Terminal: nothing
  // auto-replays out of it.
  DLQ: 'credential.dlq.v1',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

/** Consumer groups: one responsibility each. */
export const CONSUMER_GROUPS = {
  IDENTITY: 'cg.identity-verifier',
  CLAIMS: 'cg.claims-validator',
  SIGNING: 'cg.credential-signer',
  CALLBACK: 'cg.callback-dispatcher',
  DLQ: 'cg.dlq-monitor',
  // Watch fan-out is unique PER NODE so every node receives the full event
  // stream (fan-out, not load-balancing) and can feed its local Watch streams.
  EVENTS_FANOUT_PREFIX: 'cg.events-fanout',
} as const;

/** The topic that carries the command for a given step. */
export const STEP_TOPIC: Record<PipelineStep, TopicName> = {
  [PipelineStep.IDENTITY_VERIFICATION]: TOPICS.IDENTITY_VERIFY,
  [PipelineStep.CLAIMS_VALIDATION]: TOPICS.CLAIMS_VALIDATE,
  [PipelineStep.CREDENTIAL_SIGNING]: TOPICS.SIGN,
};

/** Fixed step order. `null` marks the end of the pipeline. */
export const STEP_ORDER: PipelineStep[] = [
  PipelineStep.IDENTITY_VERIFICATION,
  PipelineStep.CLAIMS_VALIDATION,
  PipelineStep.CREDENTIAL_SIGNING,
];

export function nextStep(step: PipelineStep): PipelineStep | null {
  const i = STEP_ORDER.indexOf(step);
  return i >= 0 && i < STEP_ORDER.length - 1 ? STEP_ORDER[i + 1] : null;
}

/** A step command message. All work is driven by requestId; state lives in the DB. */
export interface StepCommand {
  requestId: string;
}
