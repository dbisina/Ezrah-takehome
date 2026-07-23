import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { StepState } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { TransitionService } from './transition.service';
import { EventBus } from '../events/event-bus.service';
import { EnvConfig } from '../config/env.config';
import { PipelineEventDto } from '../events/event.types';
import { asMessage } from '../common/util';

/**
 * Recovers steps stranded in RUNNING by a crashed consumer.
 *
 * The atomic claim keeps the happy path free of double execution (a RUNNING step
 * is not claimable), which means a consumer that dies after claiming but before
 * finishing would otherwise leave the step stuck forever. Once a step has been
 * RUNNING longer than the lease, this job re-drives it (or fails it, if attempts
 * are exhausted). The transition methods it calls are optimistically guarded, so
 * competing reaper passes cannot double-act.
 */
@Injectable()
export class ReaperService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(ReaperService.name);
  private timer: NodeJS.Timeout | null = null;
  private stopped = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly transition: TransitionService,
    private readonly bus: EventBus,
    private readonly env: EnvConfig,
  ) {}

  onApplicationBootstrap(): void {
    this.schedule();
    this.logger.log(`Reaper started (lease ${this.env.stepLeaseMs}ms, poll ${this.env.reaperIntervalMs}ms)`);
  }

  onApplicationShutdown(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
  }

  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      void this.reap().finally(() => this.schedule());
    }, this.env.reaperIntervalMs);
  }

  /** Re-drive any steps that have been RUNNING past the lease. Public for tests. */
  async reap(): Promise<number> {
    const threshold = new Date(Date.now() - this.env.stepLeaseMs);
    let recovered = 0;
    try {
      const stale = await this.prisma.stepExecution.findMany({
        where: { state: StepState.RUNNING, startedAt: { lt: threshold } },
        take: 100,
      });
      for (const s of stale) {
        const reason = `recovered stale RUNNING ${s.step} (assumed consumer crash; lease ${this.env.stepLeaseMs}ms exceeded)`;
        let events: PipelineEventDto[] = [];
        if (s.attempts < this.env.maxStepAttempts) {
          const r = await this.transition.scheduleRetry(s.requestId, s.step, s.attempts, reason, 0);
          events = r.events;
          if (r.applied) recovered += 1;
        } else {
          const r = await this.transition.failRequest(s.requestId, s.step, s.attempts, reason, {
            transient: true,
            toDlq: true,
          });
          events = r.events;
          if (r.applied) recovered += 1;
        }
        for (const e of events) this.bus.publish(e);
      }
      if (recovered > 0) this.logger.warn(`Reaper recovered ${recovered} stale step(s)`);
    } catch (err) {
      this.logger.error(`Reaper error: ${asMessage(err)}`);
    }
    return recovered;
  }
}
