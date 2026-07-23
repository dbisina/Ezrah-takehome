import { Injectable } from '@nestjs/common';
import { CredentialRequest, PipelineStep } from '@prisma/client';
import { AbstractStepConsumer, StepOutcome } from '../step-consumer.base';
import { CONSUMER_GROUPS, TOPICS } from '../../common/topics';
import { TransitionService } from '../transition.service';
import { PrismaService } from '../../prisma/prisma.service';
import { KafkaService } from '../../kafka/kafka.service';
import { EventBus } from '../../events/event-bus.service';
import { EnvConfig } from '../../config/env.config';
import { signCredential } from '../../domain/credential';
import { sleep } from '../../common/util';

/**
 * Step 3: credential signing. Simulated: an artificial delay and an occasional
 * transient failure. The signed artifact is DETERMINISTIC over the request's
 * stable inputs, so a redelivery or forced re-sign reproduces byte-identical
 * output and never mints a second, different credential. It is persisted inside
 * the same transaction that marks the step succeeded.
 */
@Injectable()
export class SigningConsumer extends AbstractStepConsumer {
  protected readonly step = PipelineStep.CREDENTIAL_SIGNING;
  protected readonly topic = TOPICS.SIGN;
  protected readonly groupId = CONSUMER_GROUPS.SIGNING;

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
    await sleep(this.env.signingDelayMs);
    if (Math.random() < this.env.signingFailureRate) {
      return { ok: false, transient: true, reason: 'signing service unavailable (transient)' };
    }
    const credential = signCredential({
      requestId: request.id,
      subjectDid: request.subjectDid,
      credentialType: request.credentialType,
      claims: (request.claims ?? {}) as Record<string, string>,
      issuanceDate: request.createdAt.toISOString(),
    });
    return { ok: true, credential };
  }
}
