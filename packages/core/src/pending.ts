import type { CheckpointCompletion, Clock, FreshnessTracker } from './freshness';
import type { BridgeDriver, CrudDelta, PendingOpRef } from './types';

export function refKey(table: string, id: string): string {
  return `${table}\u0000${id}`;
}

/**
 * Snapshot-lifetime pending window (§5.2): SEEDED with the live set at requestedAt, then every
 * added AND completed ref observed until the snapshot retires. Never closed at consume.
 */
export class PendingWindow {
  private readonly refs = new Map<string, PendingOpRef>();
  private open = true;

  constructor(seed: Iterable<PendingOpRef>) {
    for (const r of seed) this.refs.set(refKey(r.table, r.id), r);
  }

  observe(delta: CrudDelta): void {
    if (!this.open) return;
    for (const r of delta.added) this.refs.set(refKey(r.table, r.id), r);
    for (const r of delta.completed) this.refs.set(refKey(r.table, r.id), r);
  }

  get(table: string, id: string): PendingOpRef | undefined {
    return this.refs.get(refKey(table, id));
  }

  close(): void {
    this.open = false;
    this.refs.clear();
  }
}

/**
 * Identity-scoped engine state (F6-001): the crud-delta mirror (livePending), the open snapshot
 * windows, and `recentlyAcked`. Created at engine init — before any list mounts — and replaced
 * wholesale on identity swap.
 */
export class CrudMirror {
  private readonly live = new Map<string, PendingOpRef>();
  private readonly recentlyAcked = new Map<string, { ref: PendingOpRef; ackedAt: number }>();
  private readonly windows = new Set<PendingWindow>();
  private readonly listeners = new Set<() => void>();
  private unsubCrud: (() => void) | null = null;
  private unsubCompletion: (() => void) | null = null;
  private seeded: Promise<void> | null = null;

  constructor(
    private readonly driver: BridgeDriver,
    private readonly freshness: FreshnessTracker,
    private readonly clock: Clock,
  ) {}

  start(): void {
    this.stop();
    this.live.clear();
    this.recentlyAcked.clear();
    for (const w of this.windows) w.close();
    this.windows.clear();
    this.unsubCrud = this.driver.onCrudChange((delta) => this.apply(delta));
    this.unsubCompletion = this.freshness.onCompletion((c) => this.retireAcked(c));
    this.seeded = this.resync();
  }

  stop(): void {
    this.unsubCrud?.();
    this.unsubCompletion?.();
    this.unsubCrud = null;
    this.unsubCompletion = null;
  }

  /** Resolves once the initial live set has been read. */
  ready(): Promise<void> {
    return this.seeded ?? Promise.resolve();
  }

  /** Re-read the queue and apply the difference as a delta (fallback trigger: every local diff emission). */
  async resync(): Promise<void> {
    const refs = await this.driver.pendingUploadIds();
    const next = new Map<string, PendingOpRef>();
    for (const r of refs) next.set(refKey(r.table, r.id), r);
    const added: PendingOpRef[] = [];
    const completed: PendingOpRef[] = [];
    for (const [k, r] of next) if (!this.live.has(k)) added.push(r);
    for (const [k, r] of this.live) if (!next.has(k)) completed.push(r);
    if (added.length || completed.length) this.apply({ added, completed });
  }

  apply(delta: CrudDelta): void {
    const now = this.clock();
    for (const r of delta.added) this.live.set(refKey(r.table, r.id), r);
    for (const r of delta.completed) {
      const k = refKey(r.table, r.id);
      this.live.delete(k);
      this.recentlyAcked.set(k, { ref: r, ackedAt: now });
    }
    for (const w of this.windows) w.observe(delta);
    for (const l of this.listeners) l();
  }

  /** A ref retires only when a checkpoint whose STAMP ≥ ackTime completes (F5-001). */
  private retireAcked(c: CheckpointCompletion): void {
    if (c.stream !== null) return; // per-stream completions do not carry the whole write path
    let changed = false;
    for (const [k, v] of this.recentlyAcked) {
      if (c.stamp >= v.ackedAt) {
        this.recentlyAcked.delete(k);
        changed = true;
      }
    }
    if (changed) for (const l of this.listeners) l();
  }

  openWindow(): PendingWindow {
    const w = new PendingWindow(this.live.values());
    this.windows.add(w);
    return w;
  }

  closeWindow(w: PendingWindow): void {
    w.close();
    this.windows.delete(w);
  }

  /** The three-set union (R1). */
  pendingOp(table: string, id: string, window?: PendingWindow): PendingOpRef | undefined {
    const k = refKey(table, id);
    return this.live.get(k) ?? window?.get(table, id) ?? this.recentlyAcked.get(k)?.ref;
  }

  isLivePending(table: string, id: string): boolean {
    return this.live.has(refKey(table, id));
  }

  onChange(l: () => void): () => void {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  }
}
