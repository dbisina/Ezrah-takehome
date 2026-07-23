import { EventType } from '@prisma/client';
import { EventBus } from '../src/events/event-bus.service';
import { PipelineEventDto } from '../src/events/event.types';

function evt(eventId: string, requestId: string, sequence: number): PipelineEventDto {
  return {
    eventId,
    requestId,
    sequence,
    type: EventType.STEP_STARTED,
    step: null,
    status: 'RUNNING',
    occurredAt: '2026-01-01T00:00:00.000Z',
    data: {},
  };
}

describe('EventBus', () => {
  it('delivers events to subscribers of the same request only', () => {
    const bus = new EventBus();
    const got: number[] = [];
    bus.subscribe('r1', (e) => got.push(e.sequence));
    bus.subscribe('r2', () => got.push(-1));

    bus.publish(evt('e1', 'r1', 1));
    expect(got).toEqual([1]);
  });

  it('de-duplicates by eventId (local + kafka double-feed)', () => {
    const bus = new EventBus();
    const got: number[] = [];
    bus.subscribe('r1', (e) => got.push(e.sequence));

    const e = evt('same-id', 'r1', 5);
    bus.publish(e);
    bus.publish(e); // same eventId again -> dropped
    expect(got).toEqual([5]);
  });

  it('stops delivering after unsubscribe', () => {
    const bus = new EventBus();
    const got: number[] = [];
    const off = bus.subscribe('r1', (e) => got.push(e.sequence));
    bus.publish(evt('e1', 'r1', 1));
    off();
    bus.publish(evt('e2', 'r1', 2));
    expect(got).toEqual([1]);
  });
});
