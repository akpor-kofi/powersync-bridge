import type { BridgeClock } from './clock';
import type { FreshnessTracker } from './freshness';
import { cursorOfRow, idOf, rowComparator } from './keyset';
import type { CrudMirror, PendingWindow } from './pending';
import { classifyRemovals } from './removal';
import type { SnapshotCache, SnapshotRecord } from './snapshots';
import { SnapshotStore, shallowEqualResult } from './store';
import type {
  BridgeDriver,
  ListErrors,
  ListState,
  MeasureSink,
  OrderKey,
  PagedRow,
  PreparedQuery,
  QueryAdapter,
  RaceMeasurement,
  RacedListDefinition,
  RacedListResult,
  RowDiff,
} from './types';

export interface RacedDeps {
  readonly driver: BridgeDriver;
  readonly adapter: QueryAdapter;
  readonly crud: CrudMirror;
  readonly freshness: FreshnessTracker;
  readonly snapshots: SnapshotCache;
  readonly clock: BridgeClock;
  readonly epoch: number;
  readonly identityKey: string | null;
  readonly apiLegEnabled: boolean;
  readonly measure: MeasureSink | undefined;
}

interface Snap<TItem> {
  readonly rows: Map<string, TItem>;
  readonly record: SnapshotRecord<unknown>;
  readonly window: PendingWindow;
}

interface OverlayEntry<TItem> {
  row: TItem;
  origin: 'local' | 'api';
  presentLocally: boolean;
}

const EMPTY_RESULT: RacedListResult<never> = Object.freeze({
  items: Object.freeze([]) as readonly never[],
  state: 'pending',
  awaiting: Object.freeze({ local: true, api: true }),
  errors: Object.freeze({}),
});

export function emptyRacedResult<T>(): RacedListResult<T> {
  return EMPTY_RESULT as RacedListResult<T>;
}

/**
 * The raced list engine (§5.2). One instance per (epoch, definition, scope); ref-counted by hooks.
 */
export class RacedListStore<TItem, TScope, TApi, K extends OrderKey<TItem>> {
  readonly store = new SnapshotStore<RacedListResult<TItem>>(emptyRacedResult<TItem>());

  private readonly compare: (a: TItem, b: TItem) => number;
  private readonly table: string;
  private readonly abort = new AbortController();
  private readonly startedAt: number;

  private snap: Snap<TItem> | null = null;
  private snapPending = false;
  private snapError: Error | undefined;
  private snapWindow: PendingWindow | null = null;

  private local: Map<string, TItem> | null = null;
  private localError: Error | undefined;
  private emissionSeen = false;
  private readonly inSession = new Set<string>();
  private unsubDiffs: (() => void) | null = null;
  private unsubCrud: (() => void) | null = null;
  private unsubFresh: (() => void) | null = null;
  private prepared: PreparedQuery<TItem> | null = null;

  private owned = false;
  private overlay = new Map<string, OverlayEntry<TItem>>();
  private readonly tombstones = new Set<string>();

  private disposed = false;
  private refs = 0;
  private measured = false;
  private localMs: number | null = null;
  private apiMs: number | null = null;

  constructor(
    private readonly deps: RacedDeps,
    private readonly def: RacedListDefinition<TItem, TScope, TApi, K>,
    private readonly scope: TScope,
    private readonly scopeKey: string,
  ) {
    this.compare = rowComparator<TItem>(def.orderKey, def.direction ?? 'asc');
    this.table = def.table ?? def.id;
    this.startedAt = deps.clock.now();
  }

  retain(): void {
    this.refs += 1;
    if (this.refs === 1) this.start();
  }

  release(): void {
    this.refs -= 1;
    if (this.refs <= 0) this.dispose();
  }

  get isDisposed(): boolean {
    return this.disposed;
  }

  /** Synchronous part of an identity swap: the snapshot becomes empty 'pending' at once (R15). */
  swapRoot(): void {
    this.store.replace(emptyRacedResult<TItem>());
    this.disposed = true;
  }

  dispose(): void {
    this.disposed = true;
    this.abort.abort(new Error('disposed'));
    this.unsubDiffs?.();
    this.unsubCrud?.();
    this.unsubFresh?.();
    if (this.snapWindow) this.deps.crud.closeWindow(this.snapWindow);
    this.snapWindow = null;
  }

  // ---------------------------------------------------------------------------

  private start(): void {
    const { driver, adapter, crud, freshness } = this.deps;
    const prepared: PreparedQuery<TItem> = adapter.prepare(this.def.query(this.scope));
    this.prepared = prepared;
    this.unsubDiffs = driver.diffs(prepared).subscribe(
      (diff) => void this.onDiff(diff),
      (err) => {
        this.localError = err;
        this.reconcile();
      },
    );
    this.unsubCrud = crud.onChange(() => this.reconcile());
    this.unsubFresh = freshness.onCompletion(() => void this.onCompletion());

    if (this.deps.apiLegEnabled) this.startApiLeg();
    this.reconcile();
  }

  private startApiLeg(): void {
    const { snapshots, crud } = this.deps;
    const staleTime = this.def.staleTime ?? 10 * 60_000;
    this.snapPending = true;
    // The window is opened at request time and seeded with the live set (F4-001).
    const window = crud.openWindow();
    this.snapWindow = window;
    const key = ['powersync-bridge', this.deps.identityKey, this.deps.epoch, this.def.id, this.scopeKey] as const;
    snapshots
      .fetch<TApi>(key, staleTime, (signal) => {
        const combined = new AbortController();
        const onAbort = () => combined.abort();
        signal.addEventListener('abort', onAbort, { once: true });
        this.abort.signal.addEventListener('abort', onAbort, { once: true });
        return this.def.fetchSnapshot(this.scope, combined.signal);
      })
      .then(
        (record) => {
          if (this.disposed) return;
          this.snapPending = false;
          this.apiMs = this.deps.clock.now() - this.startedAt;
          const rows = new Map<string, TItem>();
          for (const raw of record.rows) {
            const item = this.def.mapApi(raw);
            rows.set(idOf(item), item);
          }
          this.snap = { rows, record, window };
          this.reconcile();
        },
        (err: unknown) => {
          if (this.disposed) return;
          this.snapPending = false;
          this.snapError = err instanceof Error ? err : new Error(String(err));
          crud.closeWindow(window);
          this.snapWindow = null;
          this.reconcile();
        },
      );
  }

  /**
   * A checkpoint completed. Status events and diff emissions are separate SDK events with no
   * ordering guarantee; taking ownership on the stale local map would render the pre-checkpoint
   * value for one frame (fresh→old→fresh). Pre-ownership we re-query once, then reconcile.
   */
  private async onCompletion(): Promise<void> {
    if (this.disposed || this.owned || !this.prepared || !this.emissionSeen) {
      this.reconcile();
      return;
    }
    try {
      const rows = await this.deps.driver.query(this.prepared, this.abort.signal);
      if (this.disposed) return;
      const prev = this.local ?? new Map<string, TItem>();
      const next = new Map<string, TItem>();
      for (const row of rows) {
        const id = idOf(row);
        next.set(id, row);
        const before = prev.get(id);
        if ((before === undefined || JSON.stringify(before) !== JSON.stringify(row)) && !this.deps.crud.isLivePending(this.table, id)) {
          this.inSession.add(id);
          this.tombstones.delete(id);
        }
      }
      this.local = next;
    } catch {
      // keep the last emission; the diff stream will catch up
    }
    this.reconcile();
  }

  private async onDiff(diff: RowDiff<TItem>): Promise<void> {
    if (this.disposed) return;
    const { crud } = this.deps;
    const prev = this.local;
    const next = new Map<string, TItem>();
    for (const row of diff.all) next.set(idOf(row), row);

    if (this.emissionSeen) {
      // Rows added/updated by a diff with no live pending op arrived via sync: in-session (R4).
      for (const row of [...diff.added, ...diff.updated]) {
        const id = idOf(row);
        if (!crud.isLivePending(this.table, id)) this.inSession.add(id);
        this.tombstones.delete(id); // a re-added id retires a list-instance tombstone (R2)
      }
    }
    this.local = next;
    if (!this.emissionSeen) {
      this.emissionSeen = true;
      this.localMs = this.deps.clock.now() - this.startedAt;
    }

    // Re-read the queue on every local diff emission (F3-012): a write that matters produced one.
    void crud.resync();

    if (this.owned && prev) {
      const removed = new Map<string, TItem>();
      for (const id of diff.removed) {
        const row = prev.get(id);
        if (row) removed.set(id, row);
      }
      if (removed.size > 0) await this.applyRemovals(removed);
    }
    this.reconcile();
  }

  private async applyRemovals(removed: ReadonlyMap<string, TItem>): Promise<void> {
    const classes = await classifyRemovals(
      {
        driver: this.deps.driver,
        clock: this.deps.clock,
        table: this.table,
        provenance: () => this.def.stream,
        isInWindow: this.def.isInWindow,
      },
      removed,
    );
    if (this.disposed) return;
    for (const [id, cls] of classes) {
      const entry = this.overlay.get(id);
      switch (cls) {
        case 'filtered':
          this.overlay.delete(id);
          break;
        case 'delete':
          this.overlay.delete(id);
          this.tombstones.add(id);
          break;
        case 'eviction':
        case 'retain':
          if (entry) this.overlay.set(id, { ...entry, presentLocally: false });
          break;
      }
    }
  }

  // ---------------------------------------------------------------------------

  private reconcile(): void {
    if (this.disposed) return;
    const result = this.owned ? this.renderOwned() : this.renderRace();
    const prev = this.store.getSnapshot();
    if (shallowEqualResult(prev as unknown as Record<string, unknown>, result as unknown as Record<string, unknown>)) return;
    this.store.replace(result);
    this.report(result);
  }

  private freshAt(): number {
    return this.deps.freshness.freshAtFor(this.def.stream);
  }

  private localFresh(): boolean {
    return this.emissionSeen && this.deps.freshness.isFresh(this.def.stream);
  }

  private tryTakeOwnership(): boolean {
    if (this.owned) return true;
    const apiFreshAt = this.snap?.record.fetchedAt ?? Number.NEGATIVE_INFINITY;
    if (!(this.localFresh() && this.freshAt() > apiFreshAt)) return false;
    // Ownership: local owns the list; S and its shields retire wholesale (R2, F4-005).
    this.owned = true;
    const local = this.local ?? new Map<string, TItem>();
    const overlay = new Map<string, OverlayEntry<TItem>>();
    for (const [id, row] of local) overlay.set(id, { row, origin: 'local', presentLocally: true });
    if (this.snap) {
      if (this.def.apiScopeSupersetOfLocal) {
        for (const [id, row] of this.snap.rows) {
          if (!overlay.has(id) && !this.tombstones.has(id)) overlay.set(id, { row, origin: 'api', presentLocally: false });
        }
      }
      this.deps.crud.closeWindow(this.snap.window);
      this.snapWindow = null;
      this.snap = null;
    }
    this.overlay = overlay;
    return true;
  }

  private renderOwned(): RacedListResult<TItem> {
    // Post-ownership: diffs apply directly; retained rows keep their presence flag.
    const local = this.local ?? new Map<string, TItem>();
    for (const [id, row] of local) {
      const e = this.overlay.get(id);
      if (e) {
        e.row = row;
        e.origin = 'local';
        e.presentLocally = true;
      } else if (!this.tombstones.has(id)) {
        this.overlay.set(id, { row, origin: 'local', presentLocally: true });
      }
    }
    const items = this.finalize(Array.from(this.overlay.values()));
    return this.build(items, 'converged', { local: false, api: false });
  }

  private renderRace(): RacedListResult<TItem> {
    if (this.tryTakeOwnership()) return this.renderOwned();

    const { crud } = this.deps;
    const S = this.snap;
    const local = this.local;
    const apiFreshAt = S?.record.fetchedAt ?? Number.NEGATIVE_INFINITY;
    const localFreshAt = this.freshAt();
    const apiFresher = apiFreshAt > localFreshAt;

    if (!S && !local) {
      return this.build([], 'pending', { local: !this.emissionSeen, api: this.snapPending });
    }
    if (!S && local && this.def.holdPending === 'pending' && this.snapPending) {
      return this.build([], 'pending', { local: false, api: true });
    }

    const entries: OverlayEntry<TItem>[] = [];
    const stale = !S; // rendered from local before any snapshot covered it (R12)
    const ids = new Set<string>([...(S?.rows.keys() ?? []), ...(local?.keys() ?? [])]);

    for (const id of ids) {
      const inLocal = local?.get(id);
      const inS = S?.rows.get(id);
      const pending = crud.pendingOp(this.table, id, S?.window);

      // 1. pending (three-set union): local state wins unconditionally (R1/R2)
      if (pending) {
        if (inLocal !== undefined) entries.push({ row: inLocal, origin: 'local', presentLocally: true });
        // DELETE absent locally → suppressed; INSERT/UPDATE filtered out locally → not rendered
        continue;
      }
      // 2. in both, not pending → the fresher source wins (R3)
      if (inLocal !== undefined && inS !== undefined) {
        entries.push(apiFresher ? { row: inS, origin: 'api', presentLocally: true } : { row: inLocal, origin: 'local', presentLocally: true });
        continue;
      }
      // 3. local-only, not pending
      if (inLocal !== undefined) {
        if (!S) {
          entries.push({ row: inLocal, origin: 'local', presentLocally: true });
        } else if (this.inSession.has(id)) {
          entries.push({ row: inLocal, origin: 'local', presentLocally: true }); // never quarantined (R4)
        } else if (apiFresher) {
          // pre-session row positively excluded by a fresh snapshot → QUARANTINE (R4)
        } else {
          entries.push({ row: inLocal, origin: 'local', presentLocally: true });
        }
        continue;
      }
      // 4. api-only, not tombstoned
      if (inS !== undefined && !this.tombstones.has(id)) {
        entries.push({ row: inS, origin: 'api', presentLocally: false });
      }
    }

    const items = this.finalize(entries, stale);
    const state: ListState = !S ? 'local' : !local ? 'api' : apiFresher ? 'api' : 'local';
    return this.build(items, state, { local: !this.emissionSeen, api: this.snapPending });
  }

  private finalize(entries: OverlayEntry<TItem>[], stale = false): PagedRow<TItem>[] {
    entries.sort((a, b) => this.compare(a.row, b.row));
    const limited = entries.slice(0, this.def.limit); // R7: the list is a window
    return limited.map((e) => (stale ? { row: e.row, origin: e.origin, presentLocally: e.presentLocally, stale: true } : { row: e.row, origin: e.origin, presentLocally: e.presentLocally }));
  }

  private build(items: PagedRow<TItem>[], state: ListState, awaiting: { local: boolean; api: boolean }): RacedListResult<TItem> {
    const errors: ListErrors = {};
    if (this.localError) errors.local = this.localError;
    if (this.snapError) errors.api = this.snapError;
    return { items, state, awaiting, errors };
  }

  private report(result: RacedListResult<TItem>): void {
    const sink = this.deps.measure;
    if (!sink) return;
    if (this.measured && result.state !== 'converged') return;
    if (result.state === 'pending') return;
    this.measured = true;
    const connectAt = this.deps.freshness.sessionConnectAtMs;
    const freshAt = this.deps.freshness.freshAtFor(this.def.stream);
    const m: RaceMeasurement = {
      list: this.def.id,
      winner: this.localMs !== null && (this.apiMs === null || this.localMs <= this.apiMs) ? 'local' : this.apiMs !== null ? 'api' : 'none',
      localMs: this.localMs,
      apiMs: this.apiMs,
      sessionFreshnessMs: connectAt !== null && Number.isFinite(freshAt) && freshAt >= connectAt ? this.deps.clock.now() - connectAt : null,
      rowsRendered: result.items.length,
      source: result.state,
    };
    sink(m);
  }

  /** The cursor of the last rendered row — used by raced × infinite composition (R13). */
  lastCursor(): ReturnType<typeof cursorOfRow<TItem, K>> | undefined {
    const items = this.store.getSnapshot().items;
    const last = items[items.length - 1];
    return last ? cursorOfRow<TItem, K>(last.row, this.def.orderKey) : undefined;
  }
}
