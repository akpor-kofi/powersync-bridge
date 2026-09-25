import { type BridgeClock, TimeoutError, withTimeout } from './clock';
import { streamKey } from './freshness';
import { cursorOfRow, idOf, isAfterCursor, rowComparator } from './keyset';
import { classifyRemovals } from './removal';
import { SnapshotStore, shallowEqualResult } from './store';
import type {
  Boundary,
  BoundaryReason,
  BridgeDriver,
  CursorOf,
  InfiniteListDefinition,
  InfiniteListResult,
  ListErrors,
  OrderKey,
  PagedRow,
  PartitionPolicy,
  QueryAdapter,
  RowDiff,
  StreamDescription,
  StreamHandle,
} from './types';

export interface InfiniteDeps {
  readonly driver: BridgeDriver;
  readonly adapter: QueryAdapter;
  readonly clock: BridgeClock;
  readonly onEvicted?: ((ids: readonly string[]) => void) | undefined;
}

interface Entry<TItem> {
  row: TItem;
  origin: 'local' | 'api';
  presentLocally: boolean;
}

const EMPTY: InfiniteListResult<never> = Object.freeze({
  items: Object.freeze([]) as readonly never[],
  boundary: 'more',
  awaitingSync: false,
  loading: false,
  errors: Object.freeze({}),
  warnings: Object.freeze([]) as readonly string[],
});

export function emptyInfiniteResult<T>(): InfiniteListResult<T> {
  return EMPTY as InfiniteListResult<T>;
}

const DEFAULT_TTL = 300;
const DEFAULT_CAP = 3;
const DEFAULT_TIMEOUT = 15_000;
const DEFAULT_KEEP = 3;

/**
 * The infinite list engine (§6.2): one global id set, a loaded-range local watch, and bounded
 * escalation through equality partitions or API pages.
 */
export class InfiniteListStore<TItem, TScope, K extends OrderKey<TItem>, TCursor, TPartition> {
  readonly store = new SnapshotStore<InfiniteListResult<TItem>>(emptyInfiniteResult<TItem>());

  private readonly compare: (a: TItem, b: TItem) => number;
  private readonly table: string;
  private readonly abort = new AbortController();

  private pages = 1;
  private local = new Map<string, TItem>();
  private lookahead = false;
  private emissionSeen = false;
  private localError: Error | undefined;
  private unsubDiffs: (() => void) | null = null;

  private readonly apiRows = new Map<string, TItem>();
  private readonly retained = new Map<string, TItem>();
  private readonly tombstones = new Set<string>();

  private readonly subs = new Map<string, StreamHandle>();
  private readonly lru: string[] = [];
  private probeCursor: TPartition | null = null;
  private serverFloor: TPartition | null | undefined = undefined; // undefined = not fetched yet
  private floorRevalidated = false;

  private boundary: Boundary = 'more';
  private reason: BoundaryReason | undefined;
  private awaitingSync = false;
  private loading = false;
  private errors: ListErrors = {};
  private warnings: string[] = [];
  private errorCount = 0;
  private nextRetryAt = 0;

  private disposed = false;
  private refs = 0;

  constructor(
    private readonly deps: InfiniteDeps,
    private readonly def: InfiniteListDefinition<TItem, TScope, K, TCursor, TPartition>,
    private readonly scope: TScope,
  ) {
    this.compare = rowComparator<TItem>(def.orderKey, def.direction ?? 'asc');
    this.table = def.table ?? def.id;
  }

  retain(): void {
    this.refs += 1;
    if (this.refs === 1) this.subscribeLocal();
  }

  release(): void {
    this.refs -= 1;
    if (this.refs <= 0) this.dispose();
  }

  get isDisposed(): boolean {
    return this.disposed;
  }

  swapRoot(): void {
    this.store.replace(emptyInfiniteResult<TItem>());
    this.disposed = true;
  }

  dispose(): void {
    this.disposed = true;
    this.abort.abort(new Error('disposed'));
    this.unsubDiffs?.();
    this.unsubDiffs = null;
    for (const h of this.subs.values()) h.unsubscribe();
    this.subs.clear();
    this.lru.length = 0;
  }

  // ---------------------------------------------------------------------------

  private subscribeLocal(): void {
    this.unsubDiffs?.();
    const limit = this.pages * this.def.pageSize + 1; // lookahead row
    const builder = this.def.query({ cursor: this.def.initialCursor as TCursor, limit }, this.scope);
    const prepared = this.deps.adapter.prepare(builder);
    this.unsubDiffs = this.deps.driver.diffs(prepared).subscribe(
      (diff) => void this.onDiff(diff),
      (err) => {
        this.localError = err;
        this.render();
      },
    );
  }

  private async onDiff(diff: RowDiff<TItem>): Promise<void> {
    if (this.disposed) return;
    const prev = this.local;
    const next = new Map<string, TItem>();
    const window = this.pages * this.def.pageSize;
    const sorted = [...diff.all].sort(this.compare);
    this.lookahead = sorted.length > window;
    for (const row of sorted.slice(0, window)) {
      const id = idOf(row);
      next.set(id, row);
      this.tombstones.delete(id); // a re-added id retires a list-instance tombstone (R2)
      this.retained.delete(id);
      this.apiRows.delete(id); // local claims the id
    }
    this.local = next;
    this.emissionSeen = true;

    const removed = new Map<string, TItem>();
    for (const [id, row] of prev) if (!next.has(id)) removed.set(id, row);
    if (removed.size > 0) await this.applyRemovals(removed);
    this.render();
  }

  private provenance(row: TItem): StreamDescription | undefined {
    const pol = this.def.escalate;
    if (pol.kind !== 'partition') return undefined;
    const p = pol.partitionOf(cursorOfRow<TItem, K>(row, this.def.orderKey) as unknown as TCursor);
    if (p === null) return undefined;
    return { name: pol.stream, params: pol.partitionParams(p) };
  }

  private async applyRemovals(removed: ReadonlyMap<string, TItem>): Promise<void> {
    const classes = await classifyRemovals(
      { driver: this.deps.driver, clock: this.deps.clock, table: this.table, provenance: (r) => this.provenance(r), isInWindow: this.def.isInWindow },
      removed,
    );
    if (this.disposed) return;
    const evicted: string[] = [];
    for (const [id, cls] of classes) {
      const row = removed.get(id);
      if (!row) continue;
      switch (cls) {
        case 'filtered':
          break;
        case 'delete':
          this.tombstones.add(id);
          break;
        case 'eviction':
        case 'retain':
          this.retained.set(id, row);
          evicted.push(id);
          break;
      }
    }
    if (evicted.length > 0) this.deps.onEvicted?.(evicted); // once per emission
  }

  // ---------------------------------------------------------------------------

  private lastCursor(): TCursor {
    const all = this.mergedEntries();
    const last = all[all.length - 1];
    if (!last) return this.def.initialCursor as TCursor;
    return cursorOfRow<TItem, K>(last.row, this.def.orderKey) as unknown as TCursor;
  }

  /** Explicit user retry clears the backoff (R16). */
  retry(): Promise<void> {
    this.errorCount = 0;
    this.nextRetryAt = 0;
    return this.loadMore();
  }

  async loadMore(): Promise<void> {
    if (this.disposed || this.loading || this.boundary === 'end') return;
    if (this.lookahead) {
      this.pages += 1;
      this.boundary = 'more';
      this.reason = undefined;
      this.subscribeLocal();
      this.render();
      return;
    }
    this.loading = true;
    this.render();
    try {
      const pol = this.def.escalate;
      if (pol.kind === 'partition') await this.escalatePartition(pol);
      else if (pol.kind === 'api') await this.escalateApi(pol.fetchPage);
      else await this.escalateOffset(pol.fetchPage);
    } finally {
      this.loading = false;
      this.awaitingSync = false;
      if (!this.disposed) this.render();
    }
  }

  private setUnknown(reason: BoundaryReason, err?: Error): void {
    this.boundary = 'unknown';
    this.reason = reason;
    if (err) this.errors = { ...this.errors, stream: err };
  }

  private async escalatePartition(pol: PartitionPolicy<TCursor, TPartition>): Promise<void> {
    // Gate: the SYNC socket, for PartitionPolicy only (R16).
    if (!this.deps.driver.syncStatus().connected) return this.setUnknown('offline');

    const isBefore = pol.isBefore ?? ((a: TPartition, b: TPartition) => (a as unknown as number | string) < (b as unknown as number | string));
    if (this.serverFloor === undefined && pol.serverFloor) {
      try {
        this.serverFloor = await pol.serverFloor();
      } catch {
        this.serverFloor = null;
      }
    }
    const floor = this.serverFloor ?? pol.floor;
    const cap = pol.partitionCap ?? DEFAULT_CAP;
    const timeoutMs = pol.timeoutMs ?? DEFAULT_TIMEOUT;

    // The cursor where escalation STARTED. Rows that land during the wait may already have
    // advanced the live cursor past them (diff emission racing the probe) — the probe must
    // look beyond the cursor we escalated from, never the moving one (F-006).
    const cursor = this.lastCursor();
    let p: TPartition | null;
    if (this.probeCursor !== null) {
      p = pol.nextPartition(this.probeCursor);
      if (p === null) return this.finishEnd(pol);
    } else {
      p = pol.partitionOf(cursor);
    }

    for (let probes = 0; ; probes++) {
      if (p === null || isBefore(p, floor)) return this.finishEnd(pol);
      if (probes === cap) return this.setUnknown('partition-cap');

      const params = pol.partitionParams(p);
      const key = streamKey({ name: pol.stream, params });
      let handle = this.subs.get(key);
      if (!handle) {
        handle = await this.deps.driver.syncStream(pol.stream, params, { ttl: pol.ttl ?? DEFAULT_TTL });
        this.track(key, handle);
      }
      this.awaitingSync = true;
      this.render();
      try {
        await withTimeout(this.deps.clock, handle.waitForFirstSync(this.abort.signal), timeoutMs, this.abort.signal);
      } catch (e) {
        if (this.disposed) return;
        this.untrack(key);
        return this.setUnknown(e instanceof TimeoutError ? 'timeout' : 'error', e instanceof Error ? e : undefined);
      }
      if (this.disposed) return;

      // Re-query beyond the escalation cursor: did rows land?
      const builder = this.def.query({ cursor, limit: this.def.pageSize + 1 }, this.scope);
      const rows = await this.deps.driver.query(this.deps.adapter.prepare(builder), this.abort.signal);
      if (rows.length > 0) {
        this.probeCursor = null;
        this.pages += 1;
        this.boundary = 'more';
        this.reason = undefined;
        this.subscribeLocal();
        return;
      }
      this.probeCursor = p; // proven empty
      this.untrack(key); // empty partitions hold buckets for nothing
      p = pol.nextPartition(p);
    }
  }

  private async finishEnd(pol: PartitionPolicy<TCursor, TPartition>): Promise<void> {
    // The floor is a lower bound: re-validate once before trusting 'end' (F3-013).
    if (!this.floorRevalidated && pol.serverFloor) {
      this.floorRevalidated = true;
      try {
        const fresh = await pol.serverFloor();
        if (fresh !== null && fresh !== this.serverFloor) {
          this.serverFloor = fresh;
          this.probeCursor = null;
          return this.escalatePartition(pol);
        }
      } catch {
        // keep the previous floor
      }
    }
    this.boundary = 'end';
    this.reason = undefined;
  }

  private track(key: string, handle: StreamHandle): void {
    this.subs.set(key, handle);
    this.lru.push(key);
    const keep = this.def.keepPagesAlive ?? DEFAULT_KEEP;
    while (this.lru.length > keep) {
      const old = this.lru.shift();
      if (old && old !== key) this.untrack(old);
    }
  }

  private untrack(key: string): void {
    const h = this.subs.get(key);
    if (h) h.unsubscribe();
    this.subs.delete(key);
    const i = this.lru.indexOf(key);
    if (i >= 0) this.lru.splice(i, 1);
  }

  private apiGate(): boolean {
    const now = this.deps.clock.now();
    if (now < this.nextRetryAt) {
      this.setUnknown('offline');
      return false;
    }
    return true;
  }

  private apiFailure(err: unknown): void {
    this.errorCount += 1;
    const backoff = Math.min(30_000, 1000 * 2 ** (this.errorCount - 1));
    this.nextRetryAt = this.deps.clock.now() + backoff;
    const e = err instanceof Error ? err : new Error(String(err));
    this.errors = { ...this.errors, api: e };
    this.setUnknown(this.deps.driver.onlineHint() === false ? 'offline' : 'error');
  }

  private async escalateApi(fetchPage: (cursor: TCursor, signal: AbortSignal) => Promise<readonly TItem[]>): Promise<void> {
    if (!this.apiGate()) return;
    try {
      const rows = await withTimeout(this.deps.clock, fetchPage(this.lastCursor(), this.abort.signal), this.deps.driver.onlineHint() === false ? 3000 : 30_000, this.abort.signal);
      this.absorbApi(rows);
    } catch (e) {
      if (!this.disposed) this.apiFailure(e);
    }
  }

  private async escalateOffset(fetchPage: (offset: number, signal: AbortSignal) => Promise<readonly TItem[]>): Promise<void> {
    if (!this.apiGate()) return;
    const offset = this.mergedEntries().length;
    try {
      const rows = await withTimeout(this.deps.clock, fetchPage(offset, this.abort.signal), 30_000, this.abort.signal);
      const cursor = this.lastCursor();
      // A page that overlaps rows we already hold, or that sorts before our cursor, means the
      // list shifted under the offset (live inserts). Duplicates are dropped; the shift is flagged.
      const shifted = rows.some(
        (r) =>
          this.local.has(idOf(r)) ||
          this.apiRows.has(idOf(r)) ||
          (cursor !== undefined && !isAfterCursor<TItem, K>(r, cursor as unknown as CursorOf<TItem, K>, this.def.orderKey, this.def.direction ?? 'asc')),
      );
      if (shifted && !this.warnings.includes('offset-shift')) this.warnings = [...this.warnings, 'offset-shift'];
      this.absorbApi(rows);
    } catch (e) {
      if (!this.disposed) this.apiFailure(e);
    }
  }

  private absorbApi(rows: readonly TItem[]): void {
    this.errorCount = 0;
    this.nextRetryAt = 0;
    let added = 0;
    for (const row of rows) {
      const id = idOf(row);
      if (this.local.has(id) || this.apiRows.has(id) || this.tombstones.has(id)) continue; // id-set dedupe (R13)
      this.apiRows.set(id, row);
      added += 1;
    }
    if (rows.length === 0) {
      this.boundary = 'end';
      this.reason = undefined;
    } else {
      this.boundary = 'more';
      this.reason = undefined;
    }
  }

  // ---------------------------------------------------------------------------

  private mergedEntries(): Entry<TItem>[] {
    const entries: Entry<TItem>[] = [];
    for (const row of this.local.values()) entries.push({ row, origin: 'local', presentLocally: true });
    for (const [id, row] of this.retained) if (!this.local.has(id)) entries.push({ row, origin: 'local', presentLocally: false });
    for (const [id, row] of this.apiRows) if (!this.local.has(id) && !this.retained.has(id)) entries.push({ row, origin: 'api', presentLocally: false });
    entries.sort((a, b) => this.compare(a.row, b.row));
    return entries;
  }

  private render(): void {
    if (this.disposed) return;
    const items: PagedRow<TItem>[] = this.mergedEntries().map((e) => ({ row: e.row, origin: e.origin, presentLocally: e.presentLocally }));
    const errors: ListErrors = { ...this.errors };
    if (this.localError) errors.local = this.localError;
    const result: InfiniteListResult<TItem> = {
      items,
      boundary: this.boundary,
      ...(this.reason !== undefined ? { boundaryReason: this.reason } : {}),
      awaitingSync: this.awaitingSync,
      loading: this.loading,
      errors,
      warnings: this.warnings,
    };
    const prev = this.store.getSnapshot();
    if (shallowEqualResult(prev as unknown as Record<string, unknown>, result as unknown as Record<string, unknown>)) return;
    this.store.replace(result);
  }
}
