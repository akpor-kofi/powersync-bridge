/** Injectable time so the engine is testable with virtual time. */
export interface BridgeClock {
  now(): number;
  /** Returns a cancel function. */
  setTimeout(fn: () => void, ms: number): () => void;
}

export const realClock: BridgeClock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => {
    const h = setTimeout(fn, ms);
    return () => clearTimeout(h);
  },
};

/** Resolves after `ms` on the given clock; rejects if the signal aborts first. */
export function delay(clock: BridgeClock, ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason ?? new Error('aborted'));
    const cancel = clock.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      cancel();
      reject(signal?.reason ?? new Error('aborted'));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export class TimeoutError extends Error {
  override readonly name = 'TimeoutError';
}

/** Race a promise against a clock-based timeout. */
export async function withTimeout<T>(clock: BridgeClock, p: Promise<T>, ms: number, signal?: AbortSignal): Promise<T> {
  const holder: { cancel?: () => void } = {};
  const timeout = new Promise<never>((_, reject) => {
    holder.cancel = clock.setTimeout(() => reject(new TimeoutError(`timed out after ${ms}ms`)), ms);
    signal?.addEventListener('abort', () => reject(signal.reason ?? new Error('aborted')), { once: true });
  });
  try {
    return await Promise.race([p, timeout]);
  } finally {
    holder.cancel?.();
  }
}
