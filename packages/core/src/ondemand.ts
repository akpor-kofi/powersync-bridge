import { type BridgeClock, TimeoutError, withTimeout } from './clock';
import { SnapshotStore } from './store';
import type { BridgeDriver, OnDemandResult, Params, StreamHandle } from './types';

export interface OnDemandOptions {
  readonly ttl?: number;
  readonly priority?: 0 | 1 | 2 | 3;
  readonly timeoutMs?: number;
}

/**
 * The default "pull one row" primitive (§7, [DX]): a plain on-demand equality-parameter stream.
 * Held while retained; the TTL keeps rows warm after release.
 */
export class OnDemandStreamStore {
  readonly store = new SnapshotStore<OnDemandResult>({ status: 'subscribing' });
  private handle: StreamHandle | null = null;
  private readonly abort = new AbortController();
  private refs = 0;
  private disposed = false;

  constructor(
    private readonly driver: BridgeDriver,
    private readonly clock: BridgeClock,
    private readonly name: string,
    private readonly params: Params | null,
    private readonly opts: OnDemandOptions,
  ) {}

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
    this.store.replace({ status: 'subscribing' });
    this.disposed = true;
  }

  dispose(): void {
    this.disposed = true;
    this.abort.abort(new Error('disposed'));
    this.handle?.unsubscribe();
    this.handle = null;
  }

  private async start(): Promise<void> {
    if (!this.driver.syncStatus().connected) {
      this.store.replace({ status: 'offline' });
      // still subscribe so it syncs when the socket returns
    }
    try {
      const opts: { ttl?: number; priority?: 0 | 1 | 2 | 3 } = {};
      if (this.opts.ttl !== undefined) opts.ttl = this.opts.ttl;
      if (this.opts.priority !== undefined) opts.priority = this.opts.priority;
      this.handle = await this.driver.syncStream(this.name, this.params, opts);
      if (this.disposed) {
        this.handle.unsubscribe();
        return;
      }
      if (this.driver.syncStatus().connected) this.store.replace({ status: 'syncing' });
      await withTimeout(this.clock, this.handle.waitForFirstSync(this.abort.signal), this.opts.timeoutMs ?? 15_000, this.abort.signal);
      if (!this.disposed) this.store.replace({ status: 'synced' });
    } catch (e) {
      if (this.disposed) return;
      if (e instanceof TimeoutError) this.store.replace({ status: this.driver.syncStatus().connected ? 'timeout' : 'offline' });
      else this.store.replace({ status: 'error', error: e instanceof Error ? e : new Error(String(e)) });
    }
  }
}
