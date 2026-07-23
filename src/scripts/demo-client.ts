/* eslint-disable no-console */
import { join } from 'node:path';
import { createServer, Server } from 'node:http';
import { credentials, loadPackageDefinition, ServiceError } from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

/**
 * End-to-end demo of the credential pipeline over gRPC. It:
 *   1. starts a tiny HTTP sink to receive the final callback,
 *   2. submits a valid KYC request and streams Watch to the console live,
 *   3. re-submits the SAME request to show idempotent de-duplication,
 *   4. submits an invalid request to show it is rejected before the pipeline.
 *
 * Run the stack first (`docker compose up`), then `npm run demo`.
 */

const GRPC_TARGET = process.env.GRPC_TARGET ?? 'localhost:50051';
const CALLBACK_PORT = Number(process.env.CALLBACK_PORT ?? 4000);
// The service runs in Docker; host.docker.internal lets it reach this sink on the host.
const CALLBACK_HOST = process.env.CALLBACK_HOST ?? 'host.docker.internal';
const CALLBACK_URL = `http://${CALLBACK_HOST}:${CALLBACK_PORT}/callback`;

const protoPath = join(__dirname, '..', '..', 'proto', 'credential_pipeline.proto');
const pkgDef = protoLoader.loadSync(protoPath, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const proto = loadPackageDefinition(pkgDef) as any;
const client = new proto.ezrah.credential.v1.CredentialPipeline(
  GRPC_TARGET,
  credentials.createInsecure(),
);

function unary<T>(method: string, req: unknown): Promise<T> {
  return new Promise((resolve, reject) => {
    client[method](req, (err: ServiceError | null, res: T) => (err ? reject(err) : resolve(res)));
  });
}

function watch(requestId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const stream = client.Watch({ requestId, fromSequence: '0' });
    stream.on('data', printEvent);
    stream.on('end', resolve);
    stream.on('error', reject);
  });
}

function printEvent(e: any): void {
  const data = safeParse(e.dataJson);
  const extra = summarize(e.type, data);
  const step = e.step ? ` step=${e.step}` : '';
  console.log(`  #${String(e.sequence).padStart(2, '0')} ${e.type.padEnd(18)}${step}${extra}`);
}

function summarize(type: string, data: Record<string, unknown>): string {
  if (type === 'STEP_FAILED') return `  (${data.error ?? ''}${data.willRetry ? ', will retry' : ''})`;
  if (type === 'REQUEST_FAILED') return `  (${data.reason ?? ''})`;
  if (type === 'REQUEST_COMPLETED') {
    const cred = data.credential as { proof?: { jws?: string } } | undefined;
    return `  (signed; jws=${cred?.proof?.jws?.slice(0, 16) ?? '?'}...)`;
  }
  return '';
}

function safeParse(s: string): Record<string, unknown> {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}

function startCallbackSink(): Server {
  const server = createServer((req, res) => {
    if (req.method === 'POST') {
      let body = '';
      req.on('data', (c) => (body += c));
      req.on('end', () => {
        const idem = req.headers['x-idempotency-key'];
        const parsed = safeParse(body);
        console.log(`\n[callback sink] received (idempotency-key=${idem}): status=${parsed.status}`);
        res.writeHead(200).end('ok');
      });
    } else {
      res.writeHead(404).end();
    }
  });
  server.listen(CALLBACK_PORT, () => console.log(`[callback sink] listening on :${CALLBACK_PORT}`));
  return server;
}

async function main(): Promise<void> {
  const sink = startCallbackSink();

  const kyc = {
    subjectDid: 'did:polygon:0xabc123demoSubject',
    credentialType: 'KYCCredential',
    claims: {
      fullName: 'Ada Lovelace',
      dateOfBirth: '1815-12-10',
      residenceCountry: 'GB',
      documentType: 'passport',
      documentNumber: 'P1234567',
    },
    callbackUrl: CALLBACK_URL,
    idempotencyKey: `demo-${Date.now()}`,
  };

  console.log('\n=== 1) Submit a valid KYCCredential request ===');
  const ack = await unary<any>('Submit', kyc);
  console.log(`ack: requestId=${ack.requestId} status=${ack.status} duplicate=${ack.duplicate}`);

  console.log('\n=== 2) Watch it move through the pipeline (live) ===');
  await watch(ack.requestId);

  const status = await unary<any>('GetStatus', { requestId: ack.requestId });
  console.log(`\nfinal status: ${status.status}`);

  console.log('\n=== 3) Re-submit the SAME request (idempotency) ===');
  const dup = await unary<any>('Submit', kyc);
  console.log(`ack: requestId=${dup.requestId} duplicate=${dup.duplicate} (same id: ${dup.requestId === ack.requestId})`);

  console.log('\n=== 4) Submit an invalid request (missing claims) ===');
  try {
    await unary('Submit', {
      subjectDid: 'did:polygon:0xdef456',
      credentialType: 'EmploymentCredential',
      claims: { employerName: 'Ezrah' }, // missing jobTitle, startDate
    });
    console.log('unexpectedly accepted');
  } catch (err) {
    console.log(`rejected as expected: ${(err as ServiceError).message}`);
  }

  await new Promise((r) => setTimeout(r, 1000)); // let the callback arrive
  sink.close();
  console.log('\nDone.');
  process.exit(0);
}

main().catch((err) => {
  console.error('Demo failed:', err);
  process.exit(1);
});
