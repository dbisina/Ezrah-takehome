import { EventType, PipelineStep } from '@prisma/client';

/**
 * The canonical pipeline event envelope. Every producer emits this exact shape;
 * `data` carries the type-specific payload. A consumer that reads a request's
 * events in `sequence` order can reconstruct the request's entire state.
 */
export interface PipelineEventDto {
  eventId: string;
  requestId: string;
  sequence: number;
  type: EventType;
  step: PipelineStep | null;
  status: string;
  occurredAt: string; // ISO-8601 UTC
  data: Record<string, unknown>;
}

export const TERMINAL_EVENT_TYPES: ReadonlySet<EventType> = new Set([
  EventType.REQUEST_COMPLETED,
  EventType.REQUEST_FAILED,
]);

export function isTerminalEvent(type: EventType): boolean {
  return TERMINAL_EVENT_TYPES.has(type);
}
