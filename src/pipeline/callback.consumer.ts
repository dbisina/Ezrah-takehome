import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { CallbackStatus, CredentialRequest, Prisma, RequestStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { KafkaService } from '../kafka/kafka.service';
import { EnvConfig } from '../config/env.config';
import { CONSUMER_GROUPS, TOPICS } from '../common/topics';
import { asMessage, backoffMs } from '../common/util';

/**
 * Delivers the terminal outcome to a caller's callback URL.
 *
 * Kept off the pipeline's hot path: a slow or broken webhook never blocks
 * credential processing. Delivery is idempotent end-to-end: every POST carries
 * a stable `X-Idempotency-Key` (requestId + terminal status) so the receiver can
 * de-duplicate, and a delivered outcome is recorded so we don't re-POST. Retries
 * are scheduled through the outbox (delayed re-publish), not by blocking the
 * consumer.
 */
@Injectable()
export class CallbackConsumer implements OnApplicationBootstrap {
  private readonly logger = new Logger(CallbackConsumer.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafka: KafkaService,
    private readonly env: EnvConfig,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    const consumer = this.kafka.createConsumer(CONSUMER_GROUPS.CALLBACK);
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.CALLBACK, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const requestId = parseRequestId(message.value?.toString());
        if (!requestId) return; // nothing actionable; advance offset
        await this.deliver(requestId);
      },
    });
    this.logger.log(`Consuming ${TOPICS.CALLBACK} as ${CONSUMER_GROUPS.CALLBACK}`);
  }

  private async deliver(requestId: string): Promise<void> {
    const request = await this.prisma.credentialRequest.findUnique({ where: { id: requestId } });
    if (!request || !request.callbackUrl) return;
    if (request.status !== RequestStatus.COMPLETED && request.status !== RequestStatus.FAILED) return;

    // One delivery record per (request, terminal outcome). A later, different
    // outcome (e.g. COMPLETED after a manual Retry of a FAILED request) gets its
    // own record and is delivered even though the earlier outcome was DELIVERED.
    const delivery = await this.prisma.callbackDelivery.upsert({
      where: { requestId_terminalStatus: { requestId, terminalStatus: request.status } },
      create: {
        requestId,
        terminalStatus: request.status,
        url: request.callbackUrl,
        status: CallbackStatus.PENDING,
      },
      update: {},
    });
    if (delivery.status === CallbackStatus.DELIVERED) return; // this outcome already delivered

    const idempotencyKey = `${requestId}:${request.status}`;
    const body = this.buildOutcome(request);

    try {
      const res = await this.post(request.callbackUrl, idempotencyKey, body);
      if (res.ok) {
        await this.prisma.callbackDelivery.update({
          where: { id: delivery.id },
          data: {
            status: CallbackStatus.DELIVERED,
            attempts: { increment: 1 },
            deliveredAt: new Date(),
            lastError: null,
          },
        });
        this.logger.log(`Delivered outcome for ${requestId} (${request.status}) -> ${request.callbackUrl}`);
        return;
      }
      await this.handleFailure(delivery.id, requestId, request.callbackUrl, delivery.attempts + 1, `HTTP ${res.status}`);
    } catch (err) {
      await this.handleFailure(delivery.id, requestId, request.callbackUrl, delivery.attempts + 1, asMessage(err));
    }
  }

  private async handleFailure(
    deliveryId: string,
    requestId: string,
    url: string,
    attempts: number,
    error: string,
  ): Promise<void> {
    if (attempts < this.env.callbackMaxAttempts) {
      const delay = backoffMs(attempts, this.env.retryBaseDelayMs, this.env.retryMaxDelayMs);
      await this.prisma.callbackDelivery.update({
        where: { id: deliveryId },
        data: { status: CallbackStatus.FAILED, attempts, lastError: error },
      });
      // Reschedule via the outbox rather than blocking the consumer.
      await this.prisma.outboxMessage.create({
        data: {
          aggregateId: requestId,
          topic: TOPICS.CALLBACK,
          messageKey: requestId,
          payload: { requestId } as unknown as Prisma.InputJsonValue,
          visibleAfter: new Date(Date.now() + delay),
        },
      });
      this.logger.warn(`Callback for ${requestId} failed (attempt ${attempts}), retry in ${delay}ms: ${error}`);
    } else {
      await this.prisma.callbackDelivery.update({
        where: { id: deliveryId },
        data: { status: CallbackStatus.EXHAUSTED, attempts, lastError: error },
      });
      this.logger.error(`Callback for ${requestId} exhausted after ${attempts} attempts -> ${url}: ${error}`);
    }
  }

  private buildOutcome(request: CredentialRequest): Record<string, unknown> {
    const base = {
      requestId: request.id,
      subjectDid: request.subjectDid,
      credentialType: request.credentialType,
      status: request.status,
      deliveredAt: new Date().toISOString(),
    };
    if (request.status === RequestStatus.COMPLETED) {
      return { ...base, credential: request.signedCredential };
    }
    return { ...base, failureStep: request.failureStep, failureReason: request.failureReason };
  }

  private async post(
    url: string,
    idempotencyKey: string,
    body: Record<string, unknown>,
  ): Promise<{ ok: boolean; status: number }> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.env.callbackTimeoutMs);
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-idempotency-key': idempotencyKey,
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      return { ok: res.ok, status: res.status };
    } finally {
      clearTimeout(timeout);
    }
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
