import { CredentialType } from '@prisma/client';
import {
  deriveIdempotencyKey,
  hasRequiredClaims,
  isCredentialType,
  signCredential,
  validateClaims,
} from '../src/domain/credential';
import { backoffMs } from '../src/common/util';

describe('credential domain', () => {
  const kycClaims = {
    fullName: 'Ada Lovelace',
    dateOfBirth: '1815-12-10',
    residenceCountry: 'GB',
    documentType: 'passport',
    documentNumber: 'P1',
  };

  it('recognizes valid credential types', () => {
    expect(isCredentialType('KYCCredential')).toBe(true);
    expect(isCredentialType('NopeCredential')).toBe(false);
  });

  it('ingest presence check catches missing keys but not blanks', () => {
    expect(hasRequiredClaims(CredentialType.EmploymentCredential, { employerName: 'E' }).valid).toBe(false);
    expect(
      hasRequiredClaims(CredentialType.EmploymentCredential, {
        employerName: 'E',
        jobTitle: '',
        startDate: '2020',
      }).valid,
    ).toBe(true); // blank passes presence; the pipeline step catches it
  });

  it('pipeline claims validation is stricter: missing AND blank fail', () => {
    expect(validateClaims(CredentialType.KYCCredential, kycClaims).valid).toBe(true);

    const blank = validateClaims(CredentialType.KYCCredential, { ...kycClaims, documentNumber: '   ' });
    expect(blank.valid).toBe(false);
    expect(blank.blank).toContain('documentNumber');

    const missing = validateClaims(CredentialType.IdentityCredential, { fullName: 'A' });
    expect(missing.valid).toBe(false);
    expect(missing.missing).toEqual(expect.arrayContaining(['dateOfBirth', 'nationalId']));
  });

  it('signing is deterministic over stable inputs', () => {
    const input = {
      requestId: '11111111-1111-1111-1111-111111111111',
      subjectDid: 'did:polygon:0xabc',
      credentialType: CredentialType.KYCCredential,
      claims: kycClaims,
      issuanceDate: '2026-01-01T00:00:00.000Z',
    };
    const a = signCredential(input);
    const b = signCredential(input);
    expect(a).toEqual(b);
    expect(a.proof.jws).toBe(b.proof.jws);

    const different = signCredential({ ...input, claims: { ...kycClaims, fullName: 'Grace Hopper' } });
    expect(different.proof.jws).not.toBe(a.proof.jws);
  });

  it('a claim named "id" cannot override the subject DID', () => {
    const c = signCredential({
      requestId: '22222222-2222-2222-2222-222222222222',
      subjectDid: 'did:ezrah:alice',
      credentialType: CredentialType.IdentityCredential,
      claims: { fullName: 'Alice', dateOfBirth: '2000-01-01', nationalId: 'N1', id: 'did:ezrah:mallory' },
      issuanceDate: '2026-01-01T00:00:00.000Z',
    });
    expect(c.credentialSubject.id).toBe('did:ezrah:alice');
  });

  it('derived idempotency key is stable regardless of claim key order', () => {
    const a = deriveIdempotencyKey({
      subjectDid: 'did:x',
      credentialType: 'KYCCredential',
      claims: { a: '1', b: '2' },
    });
    const b = deriveIdempotencyKey({
      subjectDid: 'did:x',
      credentialType: 'KYCCredential',
      claims: { b: '2', a: '1' },
    });
    expect(a).toBe(b);

    const c = deriveIdempotencyKey({
      subjectDid: 'did:y',
      credentialType: 'KYCCredential',
      claims: { a: '1', b: '2' },
    });
    expect(c).not.toBe(a);
  });

  it('backoff grows exponentially and is capped', () => {
    expect(backoffMs(1, 1000, 15000)).toBe(1000);
    expect(backoffMs(2, 1000, 15000)).toBe(2000);
    expect(backoffMs(3, 1000, 15000)).toBe(4000);
    expect(backoffMs(99, 1000, 15000)).toBe(15000);
  });
});
