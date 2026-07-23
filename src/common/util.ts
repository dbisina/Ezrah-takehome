export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Exponential backoff (1-indexed attempt) capped at max. */
export function backoffMs(attempt: number, base: number, max: number): number {
  const exp = base * 2 ** Math.max(0, attempt - 1);
  return Math.min(exp, max);
}

export function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
