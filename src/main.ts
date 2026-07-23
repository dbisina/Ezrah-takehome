import 'reflect-metadata';
import { join } from 'node:path';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { EnvConfig } from './config/env.config';

/**
 * The service is a pure gRPC microservice: no HTTP server. The Kafka consumers,
 * outbox relay, and reaper are Nest providers started by lifecycle hooks when
 * the microservice boots.
 */
async function bootstrap(): Promise<void> {
  const logger = new Logger('Bootstrap');
  const env = new EnvConfig();

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.GRPC,
    options: {
      package: 'ezrah.credential.v1',
      protoPath: join(__dirname, '..', 'proto', 'credential_pipeline.proto'),
      url: env.grpcUrl,
      loader: {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
      },
    },
    bufferLogs: false,
  });

  app.enableShutdownHooks();
  await app.listen();
  logger.log(`gRPC CredentialPipeline listening on ${env.grpcUrl}`);
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Fatal bootstrap error', err);
  process.exit(1);
});
