import { Injectable } from '@nestjs/common';
import { RpcException } from '@nestjs/microservices';
import { status as GrpcStatus } from '@grpc/grpc-js';
import { EventType, PipelineEvent, PipelineStep, RequestStatus } from '@prisma/client';
import { Observable } from 'rxjs';
import { PrismaService } from '../prisma/prisma.service';
import { EventBus } from '../events/event-bus.service';
import { PipelineEventDto, isTerminalEvent } from '../events/event.types';

const MAX_LIVE_BUFFER = 2000;

/**
 * Builds the Watch stream for one request: full history replayed from the event
 * log, then live events, then completion at a terminal event.
 *
 * The replay→live handoff is the subtle part. We subscribe to the live bus
 * FIRST (buffering), then read the log. Because an event is only published to
 * the bus AFTER its row is committed, every event is either already in the log
 * read or in the live buffer, never lost in the seam. We then flush the buffer
 * and de-duplicate strictly by `sequence`, so nothing is delivered twice either.
 */
@Injectable()
export class WatchService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly bus: EventBus,
  ) {}

  watch(requestId: string, fromSequence: number): Observable<PipelineEventDto> {
    return new Observable<PipelineEventDto>((subscriber) => {
      const emitted = new Set<number>();
      const liveBuffer: PipelineEventDto[] = [];
      let buffering = true;
      let closed = false;

      const cleanup = (): void => {
        if (closed) return;
        closed = true;
        unsubscribe();
      };

      const forward = (e: PipelineEventDto): void => {
        if (closed) return;
        if (e.sequence < fromSequence || emitted.has(e.sequence)) return;
        emitted.add(e.sequence);
        subscriber.next(e);
        if (isTerminalEvent(e.type)) {
          cleanup();
          subscriber.complete();
        }
      };

      const onLive = (e: PipelineEventDto): void => {
        if (buffering) {
          if (liveBuffer.length >= MAX_LIVE_BUFFER) {
            cleanup();
            subscriber.error(
              new RpcException({
                code: GrpcStatus.RESOURCE_EXHAUSTED,
                message: 'watch replay buffer overflow; re-issue Watch with from_sequence to resume',
              }),
            );
            return;
          }
          liveBuffer.push(e);
        } else {
          forward(e);
        }
      };

      // Subscribe to live events BEFORE reading the log (closes the seam).
      const unsubscribe = this.bus.subscribe(requestId, onLive);

      void (async () => {
        try {
          const exists = await this.prisma.credentialRequest.findUnique({
            where: { id: requestId },
            select: { id: true, status: true },
          });
          if (!exists) {
            cleanup();
            subscriber.error(
              new RpcException({ code: GrpcStatus.NOT_FOUND, message: `unknown request ${requestId}` }),
            );
            return;
          }

          const rows = await this.prisma.pipelineEvent.findMany({
            where: { requestId, sequence: { gte: fromSequence } },
            orderBy: { sequence: 'asc' },
          });
          for (const row of rows) forward(toDto(row));
          if (closed) return;

          // Flush anything that arrived live during the replay, then tail.
          buffering = false;
          const pending = liveBuffer.splice(0, liveBuffer.length);
          for (const e of pending) forward(e);
          if (closed) return;

          // If the request is already terminal but no terminal event was in range
          // (e.g. the caller resumed with a from_sequence past the last event),
          // there is nothing more to wait for: complete rather than hang.
          if (exists.status === RequestStatus.COMPLETED || exists.status === RequestStatus.FAILED) {
            cleanup();
            subscriber.complete();
          }
        } catch (err) {
          cleanup();
          subscriber.error(err);
        }
      })();

      // Teardown on client cancel / unsubscribe.
      return () => cleanup();
    });
  }
}

function toDto(row: PipelineEvent): PipelineEventDto {
  return {
    eventId: row.id,
    requestId: row.requestId,
    sequence: row.sequence,
    type: row.type as EventType,
    step: (row.step as PipelineStep | null) ?? null,
    status: row.status,
    occurredAt: row.occurredAt.toISOString(),
    data: (row.data as Record<string, unknown>) ?? {},
  };
}
