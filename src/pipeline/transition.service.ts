import { Injectable } from '@nestjs/common';
import {
  CredentialType,
  EventType,
  Prisma,
  PipelineStep,
  RequestStatus,
  StepState,
} from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { STEP_ORDER, STEP_TOPIC, TOPICS, nextStep } from '../common/topics';
import { PipelineEventDto } from '../events/event.types';
import { SignedCredential } from '../domain/credential';

type Tx = Prisma.TransactionClient;

interface DraftEvent {
  type: EventType;
  step: PipelineStep | null;
  status: string;
  data: Record<string, unknown>;
}

export interface AcceptResult {
  requestId: string;
  status: RequestStatus;
  duplicate: boolean;
  events: PipelineEventDto[];
}

export interface ClaimResult {
  claimed: boolean;
  attempt: number;
  alreadyTerminal: boolean;
  events: PipelineEventDto[];
}

export interface TransitionResult {
  applied: boolean;
  events: PipelineEventDto[];
}

export interface ResumeResult {
  resumed: boolean;
  step: PipelineStep | null;
  status: RequestStatus | null;
  reason?: string;
}

/**
 * All durable state transitions. Every method that changes state does so in ONE
 * Postgres transaction that also appends to the event log AND writes any Kafka
 * publish to the outbox, so state, observable history, and downstream work are
 * committed atomically. The Kafka publish itself happens later, in the relay.
 *
 * Each method returns the events it appended so the caller can hand them to the
 * in-process EventBus AFTER the transaction commits (never before; an
 * uncommitted event must not reach a watcher).
 */
@Injectable()
export class TransitionService {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Get-or-create by idempotency key. The winning insert emits REQUEST_ACCEPTED
   * and queues step 1; a concurrent duplicate loses the unique-index race, and
   * we return the existing request instead; it never double-starts the pipeline.
   */
  async accept(input: {
    idempotencyKey: string;
    subjectDid: string;
    credentialType: CredentialType;
    claims: Record<string, string>;
    callbackUrl: string | null;
  }): Promise<AcceptResult> {
    try {
      return await this.prisma.$transaction(async (tx) => {
        const created = await tx.credentialRequest.create({
          data: {
            idempotencyKey: input.idempotencyKey,
            subjectDid: input.subjectDid,
            credentialType: input.credentialType,
            claims: input.claims as unknown as Prisma.InputJsonValue,
            callbackUrl: input.callbackUrl,
            status: RequestStatus.ACCEPTED,
            currentStep: PipelineStep.IDENTITY_VERIFICATION,
            steps: { create: STEP_ORDER.map((step) => ({ step, state: StepState.PENDING })) },
          },
        });

        const events: PipelineEventDto[] = [];
        let seq = 0;
        events.push(
          await this.insertEvent(tx, created.id, ++seq, {
            type: EventType.REQUEST_ACCEPTED,
            step: null,
            status: RequestStatus.ACCEPTED,
            data: {
              subjectDid: input.subjectDid,
              credentialType: input.credentialType,
            },
          }),
        );
        await tx.credentialRequest.update({
          where: { id: created.id },
          data: { lastSequence: seq },
        });
        await this.enqueueOutbox(tx, {
          aggregateId: created.id,
          topic: STEP_TOPIC[PipelineStep.IDENTITY_VERIFICATION],
          key: created.id,
          payload: { requestId: created.id },
        });

        return {
          requestId: created.id,
          status: RequestStatus.ACCEPTED,
          duplicate: false,
          events,
        };
      });
    } catch (err) {
      if (isUniqueViolation(err)) {
        const existing = await this.prisma.credentialRequest.findUniqueOrThrow({
          where: { idempotencyKey: input.idempotencyKey },
        });
        return { requestId: existing.id, status: existing.status, duplicate: true, events: [] };
      }
      throw err;
    }
  }

  /**
   * Atomically claim a step for execution. Returns claimed=true for exactly one
   * caller even under concurrent/redelivered messages: the CAS UPDATE only
   * matches a PENDING/FAILED row, and the request row lock serializes claimers.
   */
  async claimStep(requestId: string, step: PipelineStep): Promise<ClaimResult> {
    return this.prisma.$transaction(async (tx) => {
      const req = await this.lockAndLoad(tx, requestId);
      if (!req) return emptyClaim();
      // A terminal request must never be re-claimed, whether COMPLETED or
      // FAILED. `status` is the discriminator between a terminal FAILED request
      // and a step that is merely FAILED-and-retriable: scheduleRetry() and
      // resume() both set status back to PROCESSING, so a legitimate re-drive is
      // never sitting in FAILED status here. Without this check, a redelivered
      // step command for an already-failed request could re-claim it via the
      // CAS below (which matches state IN (PENDING,FAILED)).
      if (req.status === RequestStatus.COMPLETED || req.status === RequestStatus.FAILED) {
        return { claimed: false, attempt: 0, alreadyTerminal: true, events: [] };
      }

      const claim = await tx.stepExecution.updateMany({
        where: { requestId, step, state: { in: [StepState.PENDING, StepState.FAILED] } },
        data: { state: StepState.RUNNING, attempts: { increment: 1 }, startedAt: new Date(), error: null },
      });
      if (claim.count === 0) return emptyClaim(); // already RUNNING or SUCCEEDED

      const claimed = await tx.stepExecution.findUniqueOrThrow({
        where: { requestId_step: { requestId, step } },
      });
      let seq = req.lastSequence;
      const event = await this.insertEvent(tx, requestId, ++seq, {
        type: EventType.STEP_STARTED,
        step,
        status: StepState.RUNNING,
        data: { attempt: claimed.attempts },
      });
      await tx.credentialRequest.update({
        where: { id: requestId },
        data: { status: RequestStatus.PROCESSING, currentStep: step, lastSequence: seq },
      });
      return { claimed: true, attempt: claimed.attempts, alreadyTerminal: false, events: [event] };
    });
  }

  /**
   * Mark a step succeeded and advance the pipeline. Optimistically guarded by the
   * claimed attempt number: if some other delivery already advanced this step,
   * applied=false and nothing happens (no double advance, no duplicate events).
   * For the final step, the signed credential is persisted in THIS transaction.
   */
  async succeedStep(
    requestId: string,
    step: PipelineStep,
    claimedAttempt: number,
    opts: { signedCredential?: SignedCredential } = {},
  ): Promise<TransitionResult> {
    return this.prisma.$transaction(async (tx) => {
      const req = await this.lockAndLoad(tx, requestId);
      if (!req) return notApplied();

      const upd = await tx.stepExecution.updateMany({
        where: { requestId, step, state: StepState.RUNNING, attempts: claimedAttempt },
        data: { state: StepState.SUCCEEDED, finishedAt: new Date(), error: null },
      });
      if (upd.count === 0) return notApplied();

      const events: PipelineEventDto[] = [];
      let seq = req.lastSequence;
      events.push(
        await this.insertEvent(tx, requestId, ++seq, {
          type: EventType.STEP_SUCCEEDED,
          step,
          status: StepState.SUCCEEDED,
          data: { attempt: claimedAttempt },
        }),
      );

      const next = nextStep(step);
      if (next) {
        await tx.credentialRequest.update({
          where: { id: requestId },
          data: { currentStep: next, lastSequence: seq },
        });
        await this.enqueueOutbox(tx, {
          aggregateId: requestId,
          topic: STEP_TOPIC[next],
          key: requestId,
          payload: { requestId },
        });
      } else {
        const credential = opts.signedCredential;
        events.push(
          await this.insertEvent(tx, requestId, ++seq, {
            type: EventType.REQUEST_COMPLETED,
            step: null,
            status: RequestStatus.COMPLETED,
            data: { credential: credential as unknown as Record<string, unknown> },
          }),
        );
        await tx.credentialRequest.update({
          where: { id: requestId },
          data: {
            status: RequestStatus.COMPLETED,
            currentStep: null,
            signedCredential: credential as unknown as Prisma.InputJsonValue,
            completedAt: new Date(),
            lastSequence: seq,
          },
        });
        if (req.callbackUrl) {
          await this.enqueueOutbox(tx, {
            aggregateId: requestId,
            topic: TOPICS.CALLBACK,
            key: requestId,
            payload: { requestId },
          });
        }
      }
      return { applied: true, events };
    });
  }

  /**
   * A transient step failure that will be retried. Sets the step back to FAILED
   * (claimable again) and schedules a delayed re-drive via the outbox
   * `visibleAfter`: no in-consumer sleep.
   */
  async scheduleRetry(
    requestId: string,
    step: PipelineStep,
    claimedAttempt: number,
    error: string,
    backoffMs: number,
  ): Promise<TransitionResult> {
    return this.prisma.$transaction(async (tx) => {
      const req = await this.lockAndLoad(tx, requestId);
      if (!req) return notApplied();

      const upd = await tx.stepExecution.updateMany({
        where: { requestId, step, state: StepState.RUNNING, attempts: claimedAttempt },
        data: { state: StepState.FAILED, error, finishedAt: new Date() },
      });
      if (upd.count === 0) return notApplied();

      let seq = req.lastSequence;
      const event = await this.insertEvent(tx, requestId, ++seq, {
        type: EventType.STEP_FAILED,
        step,
        status: StepState.FAILED,
        data: { attempt: claimedAttempt, error, transient: true, willRetry: true },
      });
      await tx.credentialRequest.update({ where: { id: requestId }, data: { lastSequence: seq } });
      await this.enqueueOutbox(tx, {
        aggregateId: requestId,
        topic: STEP_TOPIC[step],
        key: requestId,
        payload: { requestId },
        visibleAfter: new Date(Date.now() + backoffMs),
      });
      return { applied: true, events: [event] };
    });
  }

  /**
   * Terminal failure of the whole request at `step`. Emits STEP_FAILED +
   * REQUEST_FAILED, queues the callback, and (for exhausted transient failures)
   * parks a copy in the DLQ for inspection. `claimedAttempt` null means "force"
   * (used when failing without a live claim).
   */
  async failRequest(
    requestId: string,
    step: PipelineStep,
    claimedAttempt: number | null,
    reason: string,
    opts: { transient: boolean; toDlq: boolean },
  ): Promise<TransitionResult> {
    return this.prisma.$transaction(async (tx) => {
      const req = await this.lockAndLoad(tx, requestId);
      if (!req) return notApplied();
      if (req.status === RequestStatus.COMPLETED) return notApplied();

      const where =
        claimedAttempt === null
          ? { requestId, step }
          : { requestId, step, state: StepState.RUNNING, attempts: claimedAttempt };
      const upd = await tx.stepExecution.updateMany({
        where,
        data: { state: StepState.FAILED, error: reason, finishedAt: new Date() },
      });
      if (claimedAttempt !== null && upd.count === 0) return notApplied();

      const events: PipelineEventDto[] = [];
      let seq = req.lastSequence;
      events.push(
        await this.insertEvent(tx, requestId, ++seq, {
          type: EventType.STEP_FAILED,
          step,
          status: StepState.FAILED,
          data: { attempt: claimedAttempt ?? undefined, error: reason, transient: opts.transient, willRetry: false },
        }),
      );
      events.push(
        await this.insertEvent(tx, requestId, ++seq, {
          type: EventType.REQUEST_FAILED,
          step,
          status: RequestStatus.FAILED,
          data: { failureStep: step, reason },
        }),
      );
      await tx.credentialRequest.update({
        where: { id: requestId },
        data: {
          status: RequestStatus.FAILED,
          currentStep: step,
          failureStep: step,
          failureReason: reason,
          lastSequence: seq,
        },
      });
      if (req.callbackUrl) {
        await this.enqueueOutbox(tx, {
          aggregateId: requestId,
          topic: TOPICS.CALLBACK,
          key: requestId,
          payload: { requestId },
        });
      }
      if (opts.toDlq) {
        await this.enqueueOutbox(tx, {
          aggregateId: requestId,
          topic: TOPICS.DLQ,
          key: requestId,
          payload: { requestId, step, reason },
        });
      }
      return { applied: true, events };
    });
  }

  /**
   * Manual retry: resume a FAILED request from its first incomplete step.
   * Completed steps keep their SUCCEEDED state and are never re-run.
   */
  async resume(requestId: string): Promise<ResumeResult> {
    return this.prisma.$transaction(async (tx) => {
      const req = await this.lockAndLoad(tx, requestId, true);
      if (!req) return { resumed: false, step: null, status: null, reason: 'not found' };
      if (req.status !== RequestStatus.FAILED) {
        return { resumed: false, step: null, status: req.status, reason: `request is ${req.status}, not FAILED` };
      }

      const stepStates = new Map(req.steps.map((s) => [s.step, s.state]));
      const firstIncomplete = STEP_ORDER.find((s) => stepStates.get(s) !== StepState.SUCCEEDED) ?? null;
      if (!firstIncomplete) {
        return { resumed: false, step: null, status: req.status, reason: 'no incomplete step' };
      }

      await tx.stepExecution.updateMany({
        where: { requestId, step: firstIncomplete },
        data: { state: StepState.PENDING, error: null, finishedAt: null },
      });
      await tx.credentialRequest.update({
        where: { id: requestId },
        data: {
          status: RequestStatus.PROCESSING,
          currentStep: firstIncomplete,
          failureStep: null,
          failureReason: null,
        },
      });
      await this.enqueueOutbox(tx, {
        aggregateId: requestId,
        topic: STEP_TOPIC[firstIncomplete],
        key: requestId,
        payload: { requestId },
      });
      return { resumed: true, step: firstIncomplete, status: RequestStatus.PROCESSING };
    });
  }

  // --- internals ---------------------------------------------------------

  private async lockAndLoad(tx: Tx, requestId: string, withSteps = false) {
    // FOR UPDATE serializes all transitions for a request, so sequence
    // allocation and step CAS are race-free.
    await tx.$executeRaw`SELECT 1 FROM credential_request WHERE id = ${requestId}::uuid FOR UPDATE`;
    return tx.credentialRequest.findUnique({
      where: { id: requestId },
      include: { steps: withSteps },
    });
  }

  private async insertEvent(
    tx: Tx,
    requestId: string,
    sequence: number,
    draft: DraftEvent,
  ): Promise<PipelineEventDto> {
    const eventId = randomUUID();
    const occurredAt = new Date();
    await tx.pipelineEvent.create({
      data: {
        id: eventId,
        requestId,
        sequence,
        type: draft.type,
        step: draft.step,
        status: draft.status,
        data: draft.data as unknown as Prisma.InputJsonValue,
        occurredAt,
      },
    });
    const dto: PipelineEventDto = {
      eventId,
      requestId,
      sequence,
      type: draft.type,
      step: draft.step,
      status: draft.status,
      occurredAt: occurredAt.toISOString(),
      data: draft.data,
    };
    // Publish the event to the events topic via the outbox (same transaction),
    // for external subscribers and cross-node Watch fan-out. Keyed by requestId
    // so a request's events stay ordered on one partition.
    await this.enqueueOutbox(tx, {
      aggregateId: requestId,
      topic: TOPICS.EVENTS,
      key: requestId,
      payload: dto as unknown as Record<string, unknown>,
    });
    return dto;
  }

  private async enqueueOutbox(
    tx: Tx,
    msg: {
      aggregateId: string;
      topic: string;
      key: string;
      payload: Record<string, unknown>;
      headers?: Record<string, string>;
      visibleAfter?: Date;
    },
  ): Promise<void> {
    await tx.outboxMessage.create({
      data: {
        aggregateId: msg.aggregateId,
        topic: msg.topic,
        messageKey: msg.key,
        payload: msg.payload as unknown as Prisma.InputJsonValue,
        headers: (msg.headers as unknown as Prisma.InputJsonValue) ?? Prisma.JsonNull,
        visibleAfter: msg.visibleAfter ?? new Date(),
      },
    });
  }
}

function emptyClaim(): ClaimResult {
  return { claimed: false, attempt: 0, alreadyTerminal: false, events: [] };
}

function notApplied(): TransitionResult {
  return { applied: false, events: [] };
}

export function isUniqueViolation(err: unknown): boolean {
  return err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002';
}
