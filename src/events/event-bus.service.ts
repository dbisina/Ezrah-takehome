import { Injectable } from '@nestjs/common';
import { PipelineEventDto } from './event.types';

type Listener = (event: PipelineEventDto) => void;

/**
 * In-process pub/sub that feeds live events to Watch streams on THIS node.
 *
 * It is fed from two sources: the local commit (fast path, so a watcher on the
 * same node sees an event with no Kafka round-trip) and the per-node Kafka
 * fan-out consumer (so watchers see events produced by OTHER nodes). Those two
 * feeds overlap for locally-produced events, so `publish` de-duplicates by
 * `eventId`. Watch does a second dedup by `sequence`; this one keeps the bus
 * itself from delivering an event twice.
 */
@Injectable()
export class EventBus {
  private readonly listeners = new Map<string, Set<Listener>>();
  // Bounded LRU of recently-seen eventIds to drop the local/Kafka double-feed.
  private readonly seen = new Set<string>();
  private readonly seenOrder: string[] = [];
  private readonly seenCapacity = 20000;

  publish(event: PipelineEventDto): void {
    if (this.seen.has(event.eventId)) return;
    this.remember(event.eventId);

    const set = this.listeners.get(event.requestId);
    if (!set) return;
    for (const listener of set) {
      try {
        listener(event);
      } catch {
        // A misbehaving listener must never affect the producer or other watchers.
      }
    }
  }

  subscribe(requestId: string, listener: Listener): () => void {
    let set = this.listeners.get(requestId);
    if (!set) {
      set = new Set();
      this.listeners.set(requestId, set);
    }
    set.add(listener);
    return () => {
      const current = this.listeners.get(requestId);
      if (!current) return;
      current.delete(listener);
      if (current.size === 0) this.listeners.delete(requestId);
    };
  }

  private remember(eventId: string): void {
    this.seen.add(eventId);
    this.seenOrder.push(eventId);
    if (this.seenOrder.length > this.seenCapacity) {
      const evicted = this.seenOrder.shift();
      if (evicted !== undefined) this.seen.delete(evicted);
    }
  }
}
