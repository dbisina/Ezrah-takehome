import { Injectable } from '@nestjs/common';
import { CredentialRequest, PipelineStep } from '@prisma/client';
import { AbstractStepConsumer, StepOutcome } from '../step-consumer.base';
import { CONSUMER_GROUPS, TOPICS } from '../../common/topics';
import { TransitionService } from '../transition.service';
import { PrismaService } from '../../prisma/prisma.service';
import { KafkaService } from '../../kafka/kafka.service';
import { EventBus } from '../../events/event-bus.service';
import { EnvConfig } from '../../config/env.config';
import { sleep } from '../../common/util';

/**
 * Step 1: identity verification. Simulated: an artificial delay and an
 * occasional transient failure (a DID that momentarily fails to resolve).
 */
@Injectable()
export class IdentityConsumer extends AbstractStepConsumer {
  protected readonly step = PipelineStep.IDENTITY_VERIFICATION;
  protected readonly topic = TOPICS.IDENTITY_VERIFY;
  protected readonly groupId = CONSUMER_GROUPS.IDENTITY;

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
    await sleep(this.env.identityDelayMs);
    if (Math.random() < this.env.identityFailureRate) {
      return {
        ok: false,
        transient: true,
        reason: `subject DID ${request.subjectDid} could not be resolved (transient)`,
      };
    }
    return { ok: true };
  }
}
