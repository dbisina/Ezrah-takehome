import { EventType, PipelineEvent } from '@prisma/client';
import { EventBus } from '../src/events/event-bus.service';
import { WatchService } from '../src/pipeline/watch.service';
import { PipelineEventDto } from '../src/events/event.types';
import { PrismaService } from '../src/prisma/prisma.service';

function row(sequence: number, type: EventType): PipelineEvent {
  return {
    id: `evt-${sequence}`,
    requestId: 'r1',
    sequence,
    type,
    step: null,
    status: 'x',
    data: {},
    occurredAt: new Date('2026-01-01T00:00:00.000Z'),
  } as PipelineEvent;
}

function liveDto(sequence: number, type: EventType): PipelineEventDto {
  return {
    eventId: `evt-${sequence}`,
    requestId: 'r1',
    sequence,
    type,
    step: null,
    status: 'x',
    occurredAt: '2026-01-01T00:00:00.000Z',
    data: {},
  };
}

const tick = () => new Promise((r) => setImmediate(r));
async function waitFor(pred: () => boolean): Promise<void> {
  for (let i = 0; i < 100 && !pred(); i++) await tick();
}

describe('WatchService replay -> live seam', () => {
  it('replays the log then tails live, losing and duplicating nothing across the seam', async () => {
    const bus = new EventBus();
    const logRows = [row(1, EventType.REQUEST_ACCEPTED), row(2, EventType.STEP_STARTED), row(3, EventType.STEP_SUCCEEDED)];

    // Gate findMany so we can inject live events WHILE the log read is in
    // flight: this is the window where the replay and the live feed overlap.
    let releaseFindMany: (() => void) | undefined;
    const prisma = {
      credentialRequest: { findUnique: jest.fn().mockResolvedValue({ id: 'r1' }) },
      pipelineEvent: {
        findMany: jest.fn().mockImplementation(
          () => new Promise((res) => { releaseFindMany = () => res(logRows); }),
        ),
      },
    } as unknown as PrismaService;

    const svc = new WatchService(prisma, bus);
    const received: number[] = [];
    let completed = false;

    svc.watch('r1', 0).subscribe({
      next: (e) => received.push(e.sequence),
      complete: () => { completed = true; },
    });

    // Wait until the log read is in flight (findMany called, still pending).
    await waitFor(() => releaseFindMany !== undefined);

    // Inject live events into the seam: a brand-new event (4) and a duplicate of
    // one still sitting in the pending log read (2).
    bus.publish(liveDto(4, EventType.STEP_STARTED));
    bus.publish(liveDto(2, EventType.STEP_STARTED)); // duplicate of a log row

    // Now let the log read complete.
    releaseFindMany!();
    await waitFor(() => received.length >= 4);

    // A terminal live event ends the stream.
    bus.publish(liveDto(5, EventType.REQUEST_COMPLETED));
    await waitFor(() => completed);

    expect(received).toEqual([1, 2, 3, 4, 5]); // in order, no gaps, no duplicates
    expect(completed).toBe(true);
  });

  it('a late subscriber replays full history and completes immediately', async () => {
    const bus = new EventBus();
    const logRows = [
      row(1, EventType.REQUEST_ACCEPTED),
      row(2, EventType.STEP_SUCCEEDED),
      row(3, EventType.REQUEST_COMPLETED),
    ];
    const prisma = {
      credentialRequest: { findUnique: jest.fn().mockResolvedValue({ id: 'r1' }) },
      pipelineEvent: { findMany: jest.fn().mockResolvedValue(logRows) },
    } as unknown as PrismaService;

    const svc = new WatchService(prisma, bus);
    const received: number[] = [];
    let completed = false;
    svc.watch('r1', 0).subscribe({
      next: (e) => received.push(e.sequence),
      complete: () => { completed = true; },
    });
    await waitFor(() => completed);
    expect(received).toEqual([1, 2, 3]);
  });

  it('errors NOT_FOUND for an unknown request', async () => {
    const bus = new EventBus();
    const prisma = {
      credentialRequest: { findUnique: jest.fn().mockResolvedValue(null) },
      pipelineEvent: { findMany: jest.fn() },
    } as unknown as PrismaService;
    const svc = new WatchService(prisma, bus);
    const err = await new Promise<Error>((resolve) => {
      svc.watch('missing', 0).subscribe({ next: () => undefined, error: resolve });
    });
    expect(String((err as { message?: string }).message ?? err)).toContain('unknown request');
  });

  it('completes (does not hang) when from_sequence is past an already-terminal request', async () => {
    const bus = new EventBus();
    const prisma = {
      credentialRequest: { findUnique: jest.fn().mockResolvedValue({ id: 'r1', status: 'COMPLETED' }) },
      pipelineEvent: { findMany: jest.fn().mockResolvedValue([]) }, // nothing at/after from_sequence
    } as unknown as PrismaService;
    const svc = new WatchService(prisma, bus);
    const received: number[] = [];
    let completed = false;
    svc.watch('r1', 99).subscribe({
      next: (e) => received.push(e.sequence),
      complete: () => { completed = true; },
    });
    await waitFor(() => completed);
    expect(received).toEqual([]);
    expect(completed).toBe(true);
  });
});
