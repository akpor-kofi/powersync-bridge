import { type BridgeClock, realClock } from './clock';
import { type FlaggerConfig, AutoSyncFlagStore } from './flagger';
import { FreshnessTracker } from './freshness';
import { IdentityTracker, type IdentityKeyOptions, identityKeyFromClaims, identityKeyFromToken } from './identity';
import { InfiniteListStore } from './infinite';
import { type OnDemandOptions, OnDemandStreamStore } from './ondemand';
import { CrudMirror } from './pending';
import { RacedListStore } from './raced';
import { type MinimalQueryClient, SnapshotCache } from './snapshots';
import { SnapshotStore } from './store';
import type {
  BridgeDriver,
  InfiniteListDefinition,
  MeasureSink,
  OrderKey,
  Params,
  QueryAdapter,
  RacedListDefinition,
  Store,
} from './types';

export type Platform = 'web' | 'native';
export type RacePolicy = 'race' | 'local-first';

export interface PlatformGates {
  readonly native?: RacePolicy;
  readonly web?: RacePolicy;
}

export interface BridgeOptions {
  readonly driver: BridgeDriver;
  readonly adapter: QueryAdapter;
  /** TanStack QueryClient — used for fetchQuery + keying only (R11). Optional. */
  readonly queryClient?: MinimalQueryClient;
  readonly platform?: Platform;
  /** Native defaults to local-first (R8); web to race. */
  readonly platformGates?: PlatformGates;
  readonly measure?: { readonly sink: MeasureSink };
  readonly identity?: IdentityKeyOptions;
  readonly clock?: BridgeClock;
  readonly onEvicted?: (listId: string, ids: readonly string[]) => void;
}

/** The shape of a PowerSync backend connector, structurally (no SDK import in core). */
export interface ConnectorLike {
  fetchCredentials(): Promise<{ token: string; endpoint?: string; expiresAt?: Date } | null>;
  uploadData(database: unknown): Promise<void>;
}

interface Disposable {
  retain(): void;
  release(): void;
  swapRoot(): void;
  dispose(): void;
  readonly isDisposed: boolean;
}

function detectPlatform(): Platform {
  const nav = (globalThis as { navigator?: { product?: string } }).navigator;
  return nav?.product === 'ReactNative' ? 'native' : 'web';
}

function scopeKeyOf(scope: unknown): string {
  return JSON.stringify(scope ?? null);
}

/**
 * The bridge: one per app, created once in a plain module before login (§5.1 root object).
 * Owns the identity-scoped engine state (crud-delta mirror, recentlyAcked, freshness) from
 * creation — before any list mounts (F6-001).
 */
export class Bridge {
  readonly driver: BridgeDriver;
  readonly adapter: QueryAdapter;
  readonly platform: Platform;
  readonly clock: BridgeClock;
  /** Increments on every identity swap; hooks subscribe to re-acquire their stores. */
  readonly epochStore: Store<number>;

  private readonly identity = new IdentityTracker();
  private readonly freshness: FreshnessTracker;
  private readonly crud: CrudMirror;
  private readonly snapshots: SnapshotCache;
  private readonly epochSnapshot: SnapshotStore<number>;
  private readonly stores = new Map<string, Disposable>();
  private readonly gates: Required<PlatformGates>;
  private readonly measure: MeasureSink | undefined;
  private readonly identityOpts: IdentityKeyOptions;
  private readonly onEvicted: BridgeOptions['onEvicted'];
  private disposed = false;

  constructor(opts: BridgeOptions) {
    this.driver = opts.driver;
    this.adapter = opts.adapter;
    this.clock = opts.clock ?? realClock;
    this.platform = opts.platform ?? detectPlatform();
    this.gates = { native: opts.platformGates?.native ?? 'local-first', web: opts.platformGates?.web ?? 'race' };
    this.measure = opts.measure?.sink;
    this.identityOpts = opts.identity ?? {};
    this.onEvicted = opts.onEvicted;
    this.epochSnapshot = new SnapshotStore<number>(0);
    this.epochStore = this.epochSnapshot;
    this.freshness = new FreshnessTracker(this.driver, () => this.clock.now());
    this.crud = new CrudMirror(this.driver, this.freshness, () => this.clock.now());
    this.snapshots = new SnapshotCache(this.clock, opts.queryClient);

    this.freshness.start(this.clock.now());
    this.crud.start();
    this.freshness.onClear(() => this.identity.signalClear());
    this.identity.onChange(() => this.swapEpoch());
  }

  get epoch(): number {
    return this.identity.epoch;
  }

  get identityKey(): string | null {
    return this.identity.currentKey;
  }

  /** Resolves once the initial upload-queue read has landed (tests). */
  ready(): Promise<void> {
    return this.crud.ready();
  }

  // ---------------------------------------------------------------------------
  // Identity (R15, [DX])
  // ---------------------------------------------------------------------------

  /** Connect THROUGH the bridge so identity is derived from every token the connector produces. */
  wrapConnector<C extends ConnectorLike>(connector: C): C {
    const wrapped = Object.create(connector) as C;
    Object.defineProperty(wrapped, 'fetchCredentials', {
      value: async () => {
        const creds = await connector.fetchCredentials();
        if (creds?.token) this.identity.observe(identityKeyFromToken(creds.token, this.identityOpts));
        return creds;
      },
      configurable: true,
      writable: true,
    });
    Object.defineProperty(wrapped, 'uploadData', {
      value: (db: unknown) => connector.uploadData(db),
      configurable: true,
      writable: true,
    });
    return wrapped;
  }

  /** Escape hatch for scope that is not in the token. */
  setIdentity(scope: Record<string, unknown> | null): void {
    this.identity.observe(scope === null ? null : identityKeyFromClaims(scope, this.identityOpts));
  }

  /**
   * Identity swap (R15): bump the epoch AND swap every store's root to empty 'pending' in ONE
   * synchronous operation, then tear the rest down asynchronously.
   */
  private swapEpoch(): void {
    const outgoing = Array.from(this.stores.values());
    this.stores.clear();
    for (const s of outgoing) s.swapRoot(); // synchronous: no committed frame can see the old tenant
    this.epochSnapshot.replace(this.identity.epoch);
    // async teardown
    queueMicrotask(() => {
      this.snapshots.clear();
      for (const s of outgoing) s.dispose();
      this.freshness.start(this.clock.now());
      this.crud.start();
    });
  }

  // ---------------------------------------------------------------------------
  // Store acquisition (ref-counted; shared across components with the same key)
  // ---------------------------------------------------------------------------

  private acquire<T extends Disposable>(key: string, make: () => T): T {
    const existing = this.stores.get(key) as T | undefined;
    if (existing && !existing.isDisposed) return existing;
    const s = make();
    this.stores.set(key, s);
    return s;
  }

  private apiLegEnabled(): boolean {
    return this.gates[this.platform] === 'race';
  }

  raced<TItem, TScope, TApi, K extends OrderKey<TItem>>(
    def: RacedListDefinition<TItem, TScope, TApi, K>,
    scope: TScope,
  ): RacedListStore<TItem, TScope, TApi, K> {
    const scopeKey = scopeKeyOf(scope);
    const key = `raced:${this.epoch}:${def.id}:${scopeKey}`;
    return this.acquire(key, () =>
      new RacedListStore<TItem, TScope, TApi, K>(
        {
          driver: this.driver,
          adapter: this.adapter,
          crud: this.crud,
          freshness: this.freshness,
          snapshots: this.snapshots,
          clock: this.clock,
          epoch: this.epoch,
          identityKey: this.identityKey,
          apiLegEnabled: this.apiLegEnabled(),
          measure: this.measure,
        },
        def,
        scope,
        scopeKey,
      ),
    );
  }

  infinite<TItem, TScope, K extends OrderKey<TItem>, TCursor, TPartition>(
    def: InfiniteListDefinition<TItem, TScope, K, TCursor, TPartition>,
    scope: TScope,
  ): InfiniteListStore<TItem, TScope, K, TCursor, TPartition> {
    const key = `infinite:${this.epoch}:${def.id}:${scopeKeyOf(scope)}`;
    return this.acquire(key, () =>
      new InfiniteListStore<TItem, TScope, K, TCursor, TPartition>(
        {
          driver: this.driver,
          adapter: this.adapter,
          clock: this.clock,
          onEvicted: this.onEvicted ? (ids) => this.onEvicted?.(def.id, ids) : undefined,
        },
        def,
        scope,
      ),
    );
  }

  onDemand(name: string, params: Params | null, opts: OnDemandOptions = {}): OnDemandStreamStore {
    const key = `ondemand:${this.epoch}:${name}:${JSON.stringify(params)}`;
    return this.acquire(key, () => new OnDemandStreamStore(this.driver, this.clock, name, params, opts));
  }

  autoSync(config: FlaggerConfig, ids: readonly string[]): AutoSyncFlagStore {
    const key = `flag:${this.epoch}:${config.stream}:${JSON.stringify([...ids].sort())}`;
    return this.acquire(key, () => new AutoSyncFlagStore(this.driver, this.clock, config, ids));
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    for (const s of this.stores.values()) s.dispose();
    this.stores.clear();
    this.snapshots.clear();
    this.freshness.stop();
    this.crud.stop();
  }
}

export function createBridge(opts: BridgeOptions): Bridge {
  return new Bridge(opts);
}
