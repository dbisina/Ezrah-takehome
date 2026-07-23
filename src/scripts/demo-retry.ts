/* eslint-disable no-console */
import { join } from 'node:path';
import { credentials, loadPackageDefinition, ServiceError } from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

/**
 * Demonstrates the manual Retry RPC and "resume from failure".
 *
 * Submits a request with a BLANK required claim, which passes the ingest
 * presence check but fails the deterministic claims-validation step, a
 * reproducible terminal failure. We then call Retry and show that:
 *   - the pipeline resumes at CLAIMS_VALIDATION (the first incomplete step),
 *   - IDENTITY_VERIFICATION is NOT re-run (its attempt count is unchanged),
 *   - the event sequence continues monotonically across the retry.
 *
 * Run the stack first (`docker compose up`), then `npm run demo:retry`.
 */

const GRPC_TARGET = process.env.GRPC_TARGET ?? 'localhost:50051';

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

function watch(requestId: string, fromSequence = 0): Promise<void> {
  return new Promise((resolve, reject) => {
    const stream = client.Watch({ requestId, fromSequence: String(fromSequence) });
    stream.on('data', (e: any) => {
      const data = safe(e.dataJson);
      const step = e.step ? ` step=${e.step}` : '';
      const extra =
        e.type === 'STEP_FAILED' || e.type === 'REQUEST_FAILED'
          ? `  (${data.error ?? data.reason ?? ''})`
          : '';
      console.log(`  #${String(e.sequence).padStart(2, '0')} ${e.type.padEnd(18)}${step}${extra}`);
    });
    stream.on('end', resolve);
    stream.on('error', reject);
  });
}

function safe(s: string): Record<string, unknown> {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}

function identityAttempts(status: any): number {
  const s = (status.steps ?? []).find((x: any) => x.step === 'IDENTITY_VERIFICATION');
  return Number(s?.attempts ?? 0);
}

async function main(): Promise<void> {
  console.log('\n=== Submit a request with a BLANK required claim (documentNumber) ===');
  const ack = await unary<any>('Submit', {
    subjectDid: 'did:polygon:0xretrydemo',
    credentialType: 'KYCCredential',
    claims: {
      fullName: 'Retry Demo',
      dateOfBirth: '1990-01-01',
      residenceCountry: 'GB',
      documentType: 'passport',
      documentNumber: '   ', // blank -> passes ingest presence, fails claims validation
    },
    idempotencyKey: `retry-demo-${Date.now()}`,
  });
  console.log(`ack: requestId=${ack.requestId}`);

  console.log('\n=== Watch: it should fail at CLAIMS_VALIDATION ===');
  await watch(ack.requestId);

  const s1 = await unary<any>('GetStatus', { requestId: ack.requestId });
  const idAttempts1 = identityAttempts(s1);
  console.log(
    `\nstatus=${s1.status} failureStep=${s1.failureStep} | identity attempts so far: ${idAttempts1} lastSequence=${s1.lastSequence}`,
  );

  console.log('\n=== Call Retry (resume from first incomplete step) ===');
  const retry = await unary<any>('Retry', { requestId: ack.requestId });
  console.log(`retry: resumedStep=${retry.resumedStep} status=${retry.status}`);

  console.log('\n=== Watch again from where we left off ===');
  await watch(ack.requestId, Number(s1.lastSequence) + 1);

  const s2 = await unary<any>('GetStatus', { requestId: ack.requestId });
  const idAttempts2 = identityAttempts(s2);
  console.log(`\nfinal status=${s2.status} failureStep=${s2.failureStep}`);
  console.log(
    `identity attempts before retry: ${idAttempts1}, after retry: ${idAttempts2} (${idAttempts1 === idAttempts2 ? 'unchanged, completed step was not re-run' : 'changed unexpectedly'})`,
  );

  process.exit(0);
}

main().catch((err) => {
  console.error('Demo failed:', err);
  process.exit(1);
});
