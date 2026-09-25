import type { Store } from './types';

/**
 * External store with a stable snapshot for useSyncExternalStore (R10/R11).
 * `replace()` swaps the snapshot and notifies once; `swapRoot()` is what an identity change
 * calls — it is synchronous, so no committed frame can observe the outgoing state (R15).
 */
export class SnapshotStore<S> implements Store<S> {
  private snapshot: S;
  private readonly listeners = new Set<() => void>();

  constructor(initial: S) {
    this.snapshot = initial;
  }

  getSnapshot(): S {
    return this.snapshot;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  replace(next: S): void {
    if (Object.is(next, this.snapshot)) return;
    this.snapshot = next;
    for (const l of Array.from(this.listeners)) l();
  }

  get listenerCount(): number {
    return this.listeners.size;
  }
}

/** Shallow structural equality for result records so equal reconciles produce no re-render. */
export function shallowEqualResult(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  if (ka.length !== kb.length) return false;
  for (const k of ka) {
    const va = a[k];
    const vb = b[k];
    if (Array.isArray(va) && Array.isArray(vb)) {
      if (va.length !== vb.length) return false;
      for (let i = 0; i < va.length; i++) if (!Object.is(va[i], vb[i])) return false;
      continue;
    }
    if (va !== null && typeof va === 'object' && vb !== null && typeof vb === 'object') {
      if (!shallowEqualResult(va as Record<string, unknown>, vb as Record<string, unknown>)) return false;
      continue;
    }
    if (!Object.is(va, vb)) return false;
  }
  return true;
}
