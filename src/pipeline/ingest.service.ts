import { Injectable } from '@nestjs/common';
import { RpcException } from '@nestjs/microservices';
import { status as GrpcStatus } from '@grpc/grpc-js';
import { CredentialType, RequestStatus } from '@prisma/client';
import { TransitionService } from './transition.service';
import { EventBus } from '../events/event-bus.service';
import {
  deriveIdempotencyKey,
  hasRequiredClaims,
  isCredentialType,
} from '../domain/credential';

export interface SubmitInput {
  subjectDid: string;
  credentialType: string;
  claims: Record<string, string>;
  callbackUrl?: string;
  idempotencyKey?: string;
}

export interface SubmitOutput {
  requestId: string;
  status: RequestStatus;
  duplicate: boolean;
  message: string;
}

/**
 * The accept path. Validates the request cheaply and synchronously, then hands
 * off to the durable get-or-create. It returns as soon as the request is
 * committed and step 1 is queued; the caller never waits for processing.
 */
@Injectable()
export class IngestService {
  constructor(
    private readonly transition: TransitionService,
    private readonly bus: EventBus,
  ) {}

  async submit(input: SubmitInput): Promise<SubmitOutput> {
    const subjectDid = (input.subjectDid ?? '').trim();
    if (!subjectDid) {
      throw invalid('subject_did is required');
    }
    if (!isCredentialType(input.credentialType)) {
      throw invalid(
        `credential_type must be one of EmploymentCredential, IdentityCredential, KYCCredential (got "${input.credentialType}")`,
      );
    }
    const credentialType = input.credentialType as CredentialType;
    const claims = input.claims ?? {};

    // Ingest gate: reject requests missing required claim keys before they enter
    // the pipeline. Deeper validation happens in the claims-validation step.
    const presence = hasRequiredClaims(credentialType, claims);
    if (!presence.valid) {
      throw invalid(
        `missing required claims for ${credentialType}: ${presence.missing.join(', ')}`,
      );
    }

    const idempotencyKey =
      input.idempotencyKey?.trim() ||
      deriveIdempotencyKey({ subjectDid, credentialType, claims });

    const result = await this.transition.accept({
      idempotencyKey,
      subjectDid,
      credentialType,
      claims,
      callbackUrl: input.callbackUrl?.trim() || null,
    });

    // Publish the REQUEST_ACCEPTED event to any watcher already listening.
    for (const e of result.events) this.bus.publish(e);

    return {
      requestId: result.requestId,
      status: result.status,
      duplicate: result.duplicate,
      message: result.duplicate
        ? 'duplicate submission; returning the already-accepted request'
        : 'request accepted and queued for processing',
    };
  }
}

function invalid(message: string): RpcException {
  return new RpcException({ code: GrpcStatus.INVALID_ARGUMENT, message });
}
