import { Injectable } from '@nestjs/common';
import { CredentialRequest, PipelineStep } from '@prisma/client';
import { AbstractStepConsumer, StepOutcome } from '../step-consumer.base';
import { CONSUMER_GROUPS, TOPICS } from '../../common/topics';
import { TransitionService } from '../transition.service';
import { PrismaService } from '../../prisma/prisma.service';
import { KafkaService } from '../../kafka/kafka.service';
import { EventBus } from '../../events/event-bus.service';
import { EnvConfig } from '../../config/env.config';
import { validateClaims } from '../../domain/credential';

/**
 * Step 2: claims validation. Deterministic: it passes or fails purely on the
 * data, with no delay and no randomness. A failure here is a business outcome
 * (bad data), not a transient error, so it fails the request terminally and is
 * never retried or sent to the DLQ.
 */
@Injectable()
export class ClaimsConsumer extends AbstractStepConsumer {
  protected readonly step = PipelineStep.CLAIMS_VALIDATION;
  protected readonly topic = TOPICS.CLAIMS_VALIDATE;
  protected readonly groupId = CONSUMER_GROUPS.CLAIMS;

  constructor(
    transition: TransitionService,
    prisma: PrismaService,
    kafka: KafkaService,
    bus: EventBus,
    env: EnvConfig,
  ) {
    super(transition, prisma, kafka, bus, env);
  }

  protected async doWork(request: CredentialRequest): Promise<StepOutcome> {
    const claims = (request.claims ?? {}) as Record<string, string>;
    const result = validateClaims(request.credentialType, claims);
    if (result.valid) return { ok: true };

    const parts: string[] = [];
    if (result.missing.length) parts.push(`missing [${result.missing.join(', ')}]`);
    if (result.blank.length) parts.push(`blank [${result.blank.join(', ')}]`);
    return {
      ok: false,
      transient: false,
      reason: `claims validation failed for ${request.credentialType}: ${parts.join('; ')}`,
    };
  }
}
