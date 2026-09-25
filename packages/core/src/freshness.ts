import type { BridgeDriver, StreamDescription, SyncStatusView } from './types';

export type Clock = () => number;

export interface CheckpointCompletion {
  /** Lower-bound stamp of the checkpoint's data position (download start), ms. */
  readonly stamp: number;
  readonly completedAt: number;
  /** Stream this completion was observed on; null for the global status. */
  readonly stream: string | null;
}

export function streamKey(desc: StreamDescription): string {
  return `${desc.name}:${desc.params ? JSON.stringify(desc.params) : ''}`;
}

/**
 * Session-scoped, monotonic, transition-independent freshness (§5.2, R3/R5).
 *
 * stamp(checkpoint_n) = max(observed download start ≤ its completion,
 *                           completion time of checkpoint n−1,
 *                           sessionConnectAt when no other bound exists)
 * localFreshAt = max(previous, stamp) — a null observation never lowers it.
 */
export class FreshnessTracker {
  private sessionConnectAt: number | null = null;
  private localFreshAt = Number.NEGATIVE_INFINITY;
  private prevCompletionAt: number | null = null;
  private lastDownloadStart: number | null = null;
  private lastSeenSyncedAt: number | null = null;
  private lastDownloading = false;
  private lastConnected = false;
  private lastHasSynced: boolean | undefined = undefined;
  private readonly streamFreshAt = new Map<string, number>();
  private readonly streamSeenSyncedAt = new Map<string, number>();
  private readonly completions: CheckpointCompletion[] = [];
  private readonly completionListeners = new Set<(c: CheckpointCompletion) => void>();
  private readonly clearListeners = new Set<() => void>();
  private unsubscribe: (() => void) | null = null;

  constructor(
    private readonly driver: BridgeDriver,
    private readonly clock: Clock,
  ) {}

  /** Call once at engine init and again after every identity swap. */
  start(epochStartedAt: number): void {
    this.stop();
    this.reset();
    const status = this.driver.syncStatus();
    this.lastConnected = status.connected;
    this.lastDownloading = status.downloading;
    this.lastHasSynced = status.hasSynced;
    this.lastSeenSyncedAt = status.lastSyncedAt?.getTime() ?? null;
    if (status.connected) this.sessionConnectAt = epochStartedAt;
    // Boot mid-download: provisional bound for the FIRST completion, never applied to localFreshAt now.
    if (status.downloading) {
      this.prevCompletionAt = Math.max(epochStartedAt, this.lastSeenSyncedAt ?? Number.NEGATIVE_INFINITY);
    }
    for (const s of status.syncStreams) {
      if (s.lastSyncedAt) this.streamSeenSyncedAt.set(streamKey(s), s.lastSyncedAt.getTime());
    }
    this.unsubscribe = this.driver.onStatusChanged(() => this.onStatus(this.driver.syncStatus()));
  }

  stop(): void {
    this.unsubscribe?.();
    this.unsubscribe = null;
  }

  private reset(): void {
    this.sessionConnectAt = null;
    this.localFreshAt = Number.NEGATIVE_INFINITY;
    this.prevCompletionAt = null;
    this.lastDownloadStart = null;
    this.lastSeenSyncedAt = null;
    this.streamFreshAt.clear();
    this.streamSeenSyncedAt.clear();
    this.completions.length = 0;
  }

  get sessionConnectAtMs(): number | null {
    return this.sessionConnectAt;
  }

  get localFreshAtMs(): number {
    return this.localFreshAt;
  }

  streamFreshAtMs(desc: StreamDescription): number {
    return this.streamFreshAt.get(streamKey(desc)) ?? Number.NEGATIVE_INFINITY;
  }

  /** Freshness for a list: the owning stream's stamp when declared, else the global stamp. */
  freshAtFor(stream: StreamDescription | undefined): number {
    if (!stream) return this.localFreshAt;
    return Math.max(this.streamFreshAtMs(stream), this.localFreshAt);
  }

  /** "Answered" = in-session freshness evidence (R5). */
  isFresh(stream: StreamDescription | undefined): boolean {
    if (this.sessionConnectAt === null) return false;
    return this.freshAtFor(stream) >= this.sessionConnectAt;
  }

  onCompletion(l: (c: CheckpointCompletion) => void): () => void {
    this.completionListeners.add(l);
    return () => this.completionListeners.delete(l);
  }

  onClear(l: () => void): () => void {
    this.clearListeners.add(l);
    return () => this.clearListeners.delete(l);
  }

  private onStatus(status: SyncStatusView): void {
    const now = this.clock();

    if (status.connected && !this.lastConnected && this.sessionConnectAt === null) {
      this.sessionConnectAt = now; // FIRST connect of the epoch — never per reconnect (R5)
    }
    this.lastConnected = status.connected;

    if (status.downloading && !this.lastDownloading) this.lastDownloadStart = now;
    this.lastDownloading = status.downloading;

    const syncedAt = status.lastSyncedAt?.getTime() ?? null;
    if (syncedAt !== null && syncedAt !== this.lastSeenSyncedAt) {
      this.recordCompletion(now, null);
    }
    if (syncedAt !== null) this.lastSeenSyncedAt = syncedAt; // null never lowers anything (R5)

    for (const s of status.syncStreams) {
      const key = streamKey(s);
      const at = s.lastSyncedAt?.getTime() ?? null;
      if (at !== null && at !== this.streamSeenSyncedAt.get(key)) {
        this.streamSeenSyncedAt.set(key, at);
        const stamp = this.stampFor(now);
        const prev = this.streamFreshAt.get(key) ?? Number.NEGATIVE_INFINITY;
        this.streamFreshAt.set(key, Math.max(prev, stamp));
        const c: CheckpointCompletion = { stamp: Math.max(prev, stamp), completedAt: now, stream: key };
        for (const l of this.completionListeners) l(c);
      }
    }

    if (this.lastHasSynced === true && status.hasSynced !== true) {
      for (const l of this.clearListeners) l();
    }
    this.lastHasSynced = status.hasSynced;
  }

  private stampFor(now: number): number {
    const candidates: number[] = [];
    if (this.lastDownloadStart !== null && this.lastDownloadStart <= now) candidates.push(this.lastDownloadStart);
    if (this.prevCompletionAt !== null) candidates.push(this.prevCompletionAt);
    if (candidates.length === 0 && this.sessionConnectAt !== null) candidates.push(this.sessionConnectAt);
    if (candidates.length === 0) return Number.NEGATIVE_INFINITY;
    return Math.max(...candidates);
  }

  private recordCompletion(now: number, stream: string | null): void {
    const stamp = this.stampFor(now);
    this.localFreshAt = Math.max(this.localFreshAt, stamp);
    this.prevCompletionAt = now;
    this.lastDownloadStart = null; // consumed; the next stamp derives from this completion
    const c: CheckpointCompletion = { stamp: this.localFreshAt, completedAt: now, stream };
    this.completions.push(c);
    for (const l of this.completionListeners) l(c);
  }
}
