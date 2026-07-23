import { createHash } from 'node:crypto';
import { CredentialType } from '@prisma/client';

/**
 * Required claims per credential type. This is the single source of truth for
 * both the ingest-time presence check and the pipeline's claims-validation step.
 */
export const REQUIRED_CLAIMS: Record<CredentialType, readonly string[]> = {
  [CredentialType.EmploymentCredential]: ['employerName', 'jobTitle', 'startDate'],
  [CredentialType.IdentityCredential]: ['fullName', 'dateOfBirth', 'nationalId'],
  [CredentialType.KYCCredential]: [
    'fullName',
    'dateOfBirth',
    'residenceCountry',
    'documentType',
    'documentNumber',
  ],
};

export function isCredentialType(value: string): value is CredentialType {
  return (Object.values(CredentialType) as string[]).includes(value);
}

export interface ValidationResult {
  valid: boolean;
  missing: string[];
  blank: string[];
}

/**
 * Ingest-time gate: cheap structural check that the required claim KEYS are
 * present. Obviously-malformed requests are rejected here, before anything is
 * queued. (See `validateClaims` for the deeper, pipeline-step validation.)
 */
export function hasRequiredClaims(
  type: CredentialType,
  claims: Record<string, string>,
): ValidationResult {
  const required = REQUIRED_CLAIMS[type];
  const missing = required.filter((k) => !(k in claims));
  return { valid: missing.length === 0, missing, blank: [] };
}

/**
 * Pipeline claims-validation step: deterministic and stricter than the ingest
 * gate. Required claims must be present AND non-blank, so it can fail on data
 * that slipped past the presence check (e.g. a key present but set to an empty
 * string). The result depends only on the data, with no randomness.
 */
export function validateClaims(
  type: CredentialType,
  claims: Record<string, string>,
): ValidationResult {
  const required = REQUIRED_CLAIMS[type];
  const missing = required.filter((k) => !(k in claims));
  const blank = required.filter(
    (k) => k in claims && (claims[k] === undefined || String(claims[k]).trim() === ''),
  );
  return { valid: missing.length === 0 && blank.length === 0, missing, blank };
}

export interface SignedCredential {
  '@context': string[];
  id: string;
  type: string[];
  issuer: string;
  issuanceDate: string;
  credentialSubject: Record<string, string> & { id: string };
  proof: {
    type: string;
    created: string;
    proofPurpose: string;
    verificationMethod: string;
    jws: string;
  };
}

const ISSUER_DID = 'did:ezrah:issuer';

/**
 * Simulated credential signing. Deliberately DETERMINISTIC over the request's
 * stable inputs (id, subject, claims, issuanceDate): re-running signing for the
 * same request reproduces byte-identical output, so an at-least-once redelivery
 * can never mint a second, different credential. The `jws` is a sha-256 digest
 * standing in for a real detached signature.
 */
export function signCredential(input: {
  requestId: string;
  subjectDid: string;
  credentialType: CredentialType;
  claims: Record<string, string>;
  issuanceDate: string;
}): SignedCredential {
  // Claims first, subject id LAST so it always wins: a request whose claims map
  // happens to contain an "id" key must not be able to override the credential
  // subject's DID (which would diverge the signed subject from request.subjectDid).
  const subject: Record<string, string> & { id: string } = {
    ...input.claims,
    id: input.subjectDid,
  };

  const payload = {
    id: `urn:uuid:${input.requestId}`,
    type: ['VerifiableCredential', input.credentialType],
    issuer: ISSUER_DID,
    issuanceDate: input.issuanceDate,
    credentialSubject: subject,
  };

  const jws = createHash('sha256')
    .update(`${ISSUER_DID}.${stableStringify(payload)}`)
    .digest('base64url');

  return {
    '@context': [
      'https://www.w3.org/2018/credentials/v1',
      'https://ezrah.co/credentials/v1',
    ],
    ...payload,
    proof: {
      type: 'Sha256Signature2020',
      created: input.issuanceDate,
      proofPurpose: 'assertionMethod',
      verificationMethod: `${ISSUER_DID}#key-1`,
      jws,
    },
  };
}

/** Order-stable JSON so the signature digest does not depend on key order. */
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(',')}}`;
}

/** Deterministic idempotency key derived from request content. */
export function deriveIdempotencyKey(input: {
  subjectDid: string;
  credentialType: string;
  claims: Record<string, string>;
}): string {
  return createHash('sha256')
    .update(stableStringify(input))
    .digest('hex');
}
