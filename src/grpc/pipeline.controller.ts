import { Controller } from '@nestjs/common';
import { GrpcMethod, RpcException } from '@nestjs/microservices';
import { status as GrpcStatus } from '@grpc/grpc-js';
import { PipelineStep, StepExecution } from '@prisma/client';
import { Observable, map } from 'rxjs';
import { IngestService } from '../pipeline/ingest.service';
import { WatchService } from '../pipeline/watch.service';
import { TransitionService } from '../pipeline/transition.service';
import { PrismaService } from '../prisma/prisma.service';
import { STEP_ORDER } from '../common/topics';
import { PipelineEventDto } from '../events/event.types';

const SERVICE = 'CredentialPipeline';

interface SubmitReq {
  subjectDid?: string;
  credentialType?: string;
  claims?: Record<string, string>;
  callbackUrl?: string;
  idempotencyKey?: string;
}
interface WatchReq {
  requestId?: string;
  fromSequence?: string | number;
}
interface IdReq {
  requestId?: string;
}

interface PipelineEventMsg {
  eventId: string;
  requestId: string;
  sequence: string;
  type: string;
  step: string;
  status: string;
  occurredAt: string;
  dataJson: string;
}

@Controller()
export class PipelineController {
  constructor(
    private readonly ingest: IngestService,
    private readonly watchService: WatchService,
    private readonly transition: TransitionService,
    private readonly prisma: PrismaService,
  ) {}

  @GrpcMethod(SERVICE, 'Submit')
  async submit(req: SubmitReq): Promise<{
    requestId: string;
    status: string;
    duplicate: boolean;
    message: string;
  }> {
    const out = await this.ingest.submit({
      subjectDid: req.subjectDid ?? '',
      credentialType: req.credentialType ?? '',
      claims: req.claims ?? {},
      callbackUrl: req.callbackUrl,
      idempotencyKey: req.idempotencyKey,
    });
    return {
      requestId: out.requestId,
      status: out.status,
      duplicate: out.duplicate,
      message: out.message,
    };
  }

  @GrpcMethod(SERVICE, 'Watch')
  watch(req: WatchReq): Observable<PipelineEventMsg> {
    const requestId = requireId(req.requestId);
    const fromSequence = Number(req.fromSequence ?? 0) || 0;
    return this.watchService.watch(requestId, fromSequence).pipe(map(toEventMsg));
  }

  @GrpcMethod(SERVICE, 'GetStatus')
  async getStatus(req: IdReq): Promise<Record<string, unknown>> {
    const requestId = requireId(req.requestId);
    const request = await this.prisma.credentialRequest.findUnique({
      where: { id: requestId },
      include: { steps: true },
    });
    if (!request) {
      throw new RpcException({ code: GrpcStatus.NOT_FOUND, message: `unknown request ${requestId}` });
    }

    const byStep = new Map<PipelineStep, StepExecution>(request.steps.map((s) => [s.step, s]));
    const steps = STEP_ORDER.map((step) => {
      const s = byStep.get(step);
      return {
        step,
        state: s?.state ?? 'PENDING',
        attempts: s?.attempts ?? 0,
        error: s?.error ?? '',
        startedAt: s?.startedAt?.toISOString() ?? '',
        finishedAt: s?.finishedAt?.toISOString() ?? '',
      };
    });

    return {
      requestId: request.id,
      subjectDid: request.subjectDid,
      credentialType: request.credentialType,
      status: request.status,
      currentStep: request.currentStep ?? '',
      steps,
      signedCredentialJson: request.signedCredential
        ? JSON.stringify(request.signedCredential)
        : '',
      failureStep: request.failureStep ?? '',
      failureReason: request.failureReason ?? '',
      lastSequence: String(request.lastSequence),
      createdAt: request.createdAt.toISOString(),
      updatedAt: request.updatedAt.toISOString(),
    };
  }

  @GrpcMethod(SERVICE, 'Retry')
  async retry(req: IdReq): Promise<{
    requestId: string;
    status: string;
    resumedStep: string;
    message: string;
  }> {
    const requestId = requireId(req.requestId);
    const result = await this.transition.resume(requestId);
    if (!result.resumed) {
      const code = result.reason === 'not found' ? GrpcStatus.NOT_FOUND : GrpcStatus.FAILED_PRECONDITION;
      throw new RpcException({ code, message: result.reason ?? 'cannot retry' });
    }
    return {
      requestId,
      status: result.status ?? '',
      resumedStep: result.step ?? '',
      message: `resumed from ${result.step}`,
    };
  }
}

function requireId(id: string | undefined): string {
  if (!id || !id.trim()) {
    throw new RpcException({ code: GrpcStatus.INVALID_ARGUMENT, message: 'request_id is required' });
  }
  return id;
}

function toEventMsg(e: PipelineEventDto): PipelineEventMsg {
  return {
    eventId: e.eventId,
    requestId: e.requestId,
    sequence: String(e.sequence),
    type: e.type,
    step: e.step ?? '',
    status: e.status,
    occurredAt: e.occurredAt,
    dataJson: JSON.stringify(e.data),
  };
}
