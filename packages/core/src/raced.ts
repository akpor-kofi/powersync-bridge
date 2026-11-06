import type { BridgeClock } from './clock';
import { streamKey, type FreshnessTracker } from './freshness';
import { cursorOfRow, idOf, rowComparator } from './keyset';
import type { CrudMirror, PendingWindow } from './pending';
import { classifyRemovals } from './removal';
import { BackgroundReads, CoalescedRefresh } from './refresh';
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
  readonly backgroundReads?: BackgroundReads;
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
  private abort = new AbortController();
  private observation = 0;
  private readonly checkpointRefresh: CoalescedRefresh;
  private localFreshAt = Number.NEGATIVE_INFINITY;
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
  private activeRefs = 0;
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
    this.checkpointRefresh = new CoalescedRefresh(
      (isCurrent) => this.onCompletion(isCurrent),
      () => this.reconcile(),
    );
  }

  retain(active = true): void {
    this.refs += 1;
    if (active) this.activeRefs += 1;
    if (this.activeRefs > 0 && this.unsubDiffs === null) this.start();
  }

  release(active = true): void {
    this.refs -= 1;
    if (active) this.activeRefs -= 1;
    queueMicrotask(() => {
      if (this.refs <= 0) this.dispose();
      else if (this.activeRefs <= 0) this.pause();
    });
  }

  get isDisposed(): boolean {
    return this.disposed || (this.refs <= 0 && this.unsubDiffs !== null);
  }

  /** Synchronous part of an identity swap: the snapshot becomes empty 'pending' at once (R15). */
  swapRoot(): void {
    this.store.replace(emptyRacedResult<TItem>());
    this.disposed = true;
  }

  dispose(): void {
    this.disposed = true;
    this.pause();
    if (this.snapWindow) this.deps.crud.closeWindow(this.snapWindow);
    this.snapWindow = null;
    this.local?.clear();
    this.local = null;
    this.snap = null;
    this.prepared = null;
    this.overlay.clear();
    this.inSession.clear();
    this.tombstones.clear();
    this.store.replace(emptyRacedResult<TItem>());
  }

  private pause(): void {
    this.observation += 1;
    this.checkpointRefresh.cancel();
    this.abort.abort(new Error('observer stopped'));
    this.unsubDiffs?.();
    this.unsubCrud?.();
    this.unsubFresh?.();
    this.unsubDiffs = null;
    this.unsubCrud = null;
    this.unsubFresh = null;
    if (this.snapPending) {
      this.deps.snapshots.invalidate(this.snapshotKey());
      if (this.snapWindow) this.deps.crud.closeWindow(this.snapWindow);
      this.snapWindow = this.snap?.window ?? null;
      this.snapPending = false;
    }
  }

  // ---------------------------------------------------------------------------

  private start(): void {
    if (this.disposed) return;
    this.abort = new AbortController();
    const observation = ++this.observation;
    const { driver, adapter, crud, freshness } = this.deps;
    const prepared: PreparedQuery<TItem> = adapter.prepare(this.def.query(this.scope));
    this.prepared = prepared;
    this.unsubDiffs = driver.diffs(prepared).subscribe(
      (diff) => {
        if (observation === this.observation) void this.onDiff(diff);
      },
      (err) => {
        if (observation !== this.observation) return;
        this.localError = err;
        this.reconcile();
      },
    );
    this.unsubCrud = crud.onChange(() => this.reconcile());
    this.unsubFresh = freshness.onCompletion((completion) => {
      // Unscoped queries only gain freshness from a global checkpoint. Other
      // streams must not enqueue the same aggregate read for each subscription.
      if (
        completion.stream === null ||
        (this.def.stream && completion.stream === streamKey(this.def.stream))
      ) {
        if (!this.owned) this.checkpointRefresh.request();
      }
    });

    if (this.deps.apiLegEnabled && !this.owned) this.startApiLeg();
    this.reconcile();
  }

  private startApiLeg(): void {
    const observation = this.observation;
    const abort = this.abort;
    const { snapshots, crud } = this.deps;
    const staleTime = this.def.staleTime ?? 10 * 60_000;
    this.snapPending = true;
    // The window is opened at request time and seeded with the live set (F4-001).
    const window = crud.openWindow();
    this.snapWindow = window;
    const key = this.snapshotKey();
    snapshots
      .fetch<TApi>(key, staleTime, (signal) => {
        const combined = new AbortController();
        const onAbort = () => combined.abort();
        signal.addEventListener('abort', onAbort, { once: true });
        abort.signal.addEventListener('abort', onAbort, { once: true });
        return this.def.fetchSnapshot(this.scope, combined.signal).finally(() => {
          signal.removeEventListener('abort', onAbort);
          abort.signal.removeEventListener('abort', onAbort);
        });
      })
      .then(
        (record) => {
          if (this.disposed || observation !== this.observation) return;
          this.snapPending = false;
          this.apiMs = this.deps.clock.now() - this.startedAt;
          const rows = new Map<string, TItem>();
          for (const raw of record.rows) {
            const item = this.def.mapApi(raw);
            rows.set(idOf(item), item);
          }
          if (this.snap) crud.closeWindow(this.snap.window);
          this.snap = { rows, record, window };
          this.reconcile();
        },
        (err: unknown) => {
          if (this.disposed || observation !== this.observation) return;
          this.snapPending = false;
          this.snapError = err instanceof Error ? err : new Error(String(err));
          crud.closeWindow(window);
          this.snapWindow = this.snap?.window ?? null;
          this.reconcile();
        },
      );
  }

  private snapshotKey(): readonly unknown[] {
    return ['powersync-bridge', this.deps.identityKey, this.deps.epoch, this.def.id, this.scopeKey];
  }

  /**
   * A checkpoint completed. Status events and diff emissions are separate SDK events with no
   * ordering guarantee; taking ownership on the stale local map would render the pre-checkpoint
   * value for one frame (fresh→old→fresh). Pre-ownership we re-query once, then reconcile.
   */
  private async onCompletion(isCurrent: () => boolean): Promise<void> {
    if (this.disposed || this.owned || !this.prepared || this.activeRefs <= 0) return;
    const freshAt = this.deps.freshness.freshAtFor(this.def.stream);
    try {
      const prepared = this.prepared;
      const signal = this.abort.signal;
      const read = () =>
        isCurrent() ? this.deps.driver.query(prepared, signal) : Promise.resolve(undefined);
      const rows = await (this.deps.backgroundReads
        ? this.deps.backgroundReads.run(read, signal)
        : read());
      if (!rows) return;
      if (this.disposed || !isCurrent()) return;
      const prev = this.local ?? new Map<string, TItem>();
      const next = new Map<string, TItem>();
      for (const row of rows) {
        const id = idOf(row);
        next.set(id, row);
        const before = prev.get(id);
        if (
          (before === undefined || JSON.stringify(before) !== JSON.stringify(row)) &&
          !this.deps.crud.isLivePending(this.table, id)
        ) {
          this.inSession.add(id);
          this.tombstones.delete(id);
        }
      }
      this.local = next;
      this.localFreshAt = Math.max(this.localFreshAt, freshAt);
      this.emissionSeen = true;
      this.localError = undefined;
    } catch {
      // keep the last emission; the diff stream will catch up
    }
    this.reconcile();
  }

  private async onDiff(diff: RowDiff<TItem>): Promise<void> {
    if (this.disposed || this.activeRefs <= 0) return;
    const observation = this.observation;
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
    if (!this.checkpointRefresh.pending)
      this.localFreshAt = this.deps.freshness.freshAtFor(this.def.stream);
    if (!this.emissionSeen) {
      this.emissionSeen = true;
      this.localMs = this.deps.clock.now() - this.startedAt;
    }

    // Re-read the queue on every local diff emission (F3-012): a write that matters produced one.
    void crud.resync();

    if (this.owned && prev) {
      const removed = new Map<string, TItem>();
      // A resumed watch emits a fresh baseline, so its removed[] is empty even
      // when rows disappeared while paused.
      for (const [id, row] of prev) if (!next.has(id)) removed.set(id, row);
      if (removed.size > 0) await this.applyRemovals(removed, observation);
    }
    if (observation !== this.observation) return;
    this.reconcile();
  }

  private async applyRemovals(
    removed: ReadonlyMap<string, TItem>,
    observation: number,
  ): Promise<void> {
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
    if (this.disposed || observation !== this.observation) return;
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
    if (
      shallowEqualResult(
        prev as unknown as Record<string, unknown>,
        result as unknown as Record<string, unknown>,
      )
    )
      return;
    this.store.replace(result);
    this.report(result);
  }

  private freshAt(): number {
    return this.localFreshAt;
  }

  private localFresh(): boolean {
    const connectedAt = this.deps.freshness.sessionConnectAtMs;
    return this.emissionSeen && connectedAt !== null && this.localFreshAt >= connectedAt;
  }

  private tryTakeOwnership(): boolean {
    if (this.owned) return true;
    if (this.checkpointRefresh.pending) return false;
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
          if (!overlay.has(id) && !this.tombstones.has(id))
            overlay.set(id, { row, origin: 'api', presentLocally: false });
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
        if (inLocal !== undefined)
          entries.push({ row: inLocal, origin: 'local', presentLocally: true });
        // DELETE absent locally → suppressed; INSERT/UPDATE filtered out locally → not rendered
        continue;
      }
      // 2. in both, not pending → the fresher source wins (R3)
      if (inLocal !== undefined && inS !== undefined) {
        entries.push(
          apiFresher
            ? { row: inS, origin: 'api', presentLocally: true }
            : { row: inLocal, origin: 'local', presentLocally: true },
        );
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
    return limited.map((e) =>
      stale
        ? { row: e.row, origin: e.origin, presentLocally: e.presentLocally, stale: true }
        : { row: e.row, origin: e.origin, presentLocally: e.presentLocally },
    );
  }

  private build(
    items: PagedRow<TItem>[],
    state: ListState,
    awaiting: { local: boolean; api: boolean },
  ): RacedListResult<TItem> {
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
      winner:
        this.localMs !== null && (this.apiMs === null || this.localMs <= this.apiMs)
          ? 'local'
          : this.apiMs !== null
            ? 'api'
            : 'none',
      localMs: this.localMs,
      apiMs: this.apiMs,
      sessionFreshnessMs:
        connectAt !== null && Number.isFinite(freshAt) && freshAt >= connectAt
          ? this.deps.clock.now() - connectAt
          : null,
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
