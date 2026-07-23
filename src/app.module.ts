import { Module } from '@nestjs/common';
import { PrismaModule } from './prisma/prisma.module';
import { EnvConfig } from './config/env.config';
import { KafkaService } from './kafka/kafka.service';
import { EventBus } from './events/event-bus.service';
import { TransitionService } from './pipeline/transition.service';
import { IngestService } from './pipeline/ingest.service';
import { WatchService } from './pipeline/watch.service';
import { OutboxRelay } from './pipeline/outbox-relay.service';
import { ReaperService } from './pipeline/reaper.service';
import { IdentityConsumer } from './pipeline/steps/identity.consumer';
import { ClaimsConsumer } from './pipeline/steps/claims.consumer';
import { SigningConsumer } from './pipeline/steps/signing.consumer';
import { CallbackConsumer } from './pipeline/callback.consumer';
import { DlqConsumer } from './pipeline/dlq.consumer';
import { EventsFanoutConsumer } from './pipeline/events-fanout.consumer';
import { PipelineController } from './grpc/pipeline.controller';

@Module({
  imports: [PrismaModule],
  controllers: [PipelineController],
  providers: [
    EnvConfig,
    KafkaService,
    EventBus,
    TransitionService,
    IngestService,
    WatchService,
    // Background workers (started via lifecycle hooks):
    OutboxRelay,
    ReaperService,
    IdentityConsumer,
    ClaimsConsumer,
    SigningConsumer,
    CallbackConsumer,
    DlqConsumer,
    EventsFanoutConsumer,
  ],
})
export class AppModule {}
