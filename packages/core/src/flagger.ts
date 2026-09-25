import type { BridgeClock } from './clock';
import { probePresence } from './removal';
import { SnapshotStore } from './store';
import type { BridgeDriver, StreamHandle } from './types';

export type FlagState = 'idle' | 'flagging' | 'syncing' | 'synced' | 'timeout' | 'error';

export interface FlaggerConfig {
  /** App-supplied backend call, e.g. POST /sync/flag. */
  readonly requestFlag: (ids: readonly string[]) => Promise<void>;
  /** Stream whose JOIN reads the per-user sync_requests table. */
  readonly stream: string;
  /** Table the flagged rows land in (for presence detection). */
  readonly table: string;
  readonly timeoutMs?: number;
}

export interface FlagResult {
  readonly state: FlagState;
  readonly error?: Error;
}

/**
 * OPTIONAL primitive (§7): client-triggered, per-user server-side flagging. Use only for bucket
 * economy or server-decided membership; `useOnDemandStream` is the default single-row path.
 * `synced` is detected by watching the SPECIFIC requested ids, never by waitForFirstSync.
 */
export class AutoSyncFlagStore {
  readonly store = new SnapshotStore<FlagResult>({ status: 'idle' } as unknown as FlagResult);
  private handle: StreamHandle | null = null;
  private unsubStatus: (() => void) | null = null;
  private cancelTimeout: (() => void) | null = null;
  private disposed = false;
  private refs = 0;

  constructor(
    private readonly driver: BridgeDriver,
    private readonly clock: BridgeClock,
    private readonly config: FlaggerConfig,
    private readonly ids: readonly string[],
  ) {
    this.store.replace({ state: 'idle' });
  }

  retain(): void {
    this.refs += 1;
    if (this.refs === 1) void this.start();
  }

  release(): void {
    this.refs -= 1;
    if (this.refs <= 0) this.dispose();
  }

  get isDisposed(): boolean {
    return this.disposed;
  }

  swapRoot(): void {
    this.store.replace({ state: 'idle' });
    this.disposed = true;
  }

  dispose(): void {
    this.disposed = true;
    this.unsubStatus?.();
    this.cancelTimeout?.();
    this.handle?.unsubscribe();
    this.handle = null;
  }

  private async start(): Promise<void> {
    this.store.replace({ state: 'flagging' });
    try {
      await this.config.requestFlag(this.ids);
      if (this.disposed) return;
      this.handle = await this.driver.syncStream(this.config.stream, null, {});
      this.store.replace({ state: 'syncing' });
      // timeout ≠ failure: keep the subscription, keep watching
      this.cancelTimeout = this.clock.setTimeout(() => {
        if (!this.disposed && this.store.getSnapshot().state === 'syncing') this.store.replace({ state: 'timeout' });
      }, this.config.timeoutMs ?? 15_000);
      const check = async () => {
        if (this.disposed) return;
        const present = await probePresence(this.driver, this.config.table, this.ids);
        if (!this.disposed && this.ids.every((id) => present.has(id))) {
          this.cancelTimeout?.();
          this.store.replace({ state: 'synced' });
          this.unsubStatus?.();
        }
      };
      this.unsubStatus = this.driver.onStatusChanged(() => void check());
      await check();
    } catch (e) {
      if (!this.disposed) this.store.replace({ state: 'error', error: e instanceof Error ? e : new Error(String(e)) });
    }
  }
}
