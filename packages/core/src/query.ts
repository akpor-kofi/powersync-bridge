import { withTimeout } from './clock';
import { idOf, keyColumns, rowComparator } from './keyset';
import type { PendingWindow } from './pending';
import type { RacedDeps } from './raced';
import { classifyRemovals } from './removal';
import { SnapshotStore, shallowEqualResult } from './store';
import type { ListErrors, OrderKey, PagedRow, RowsBuilder, Store } from './types';

export interface QueryPage<T> {
  readonly cursor: T | undefined;
  readonly limit: number;
}

/** Query keys include every predicate, identity and permission that can change the result. */
export interface InfiniteQueryOptions<T extends { id: string }> {
  readonly queryKey: readonly unknown[];
  readonly query: (page: QueryPage<T>) => RowsBuilder<T>;
  /** Return rows in the same order as SQLite. Honor limit, including its lookahead row. */
  readonly fetchPage: (page: QueryPage<T> & { readonly signal: AbortSignal }) => Promise<readonly T[]>;
  readonly orderBy: OrderKey<T>;
  readonly direction?: 'asc' | 'desc';
  readonly pageSize?: number;
  readonly maxRows?: number;
  readonly staleTime?: number;
  readonly enabled?: boolean;
  readonly table?: string;
  /** Heterogeneous feeds identify each underlying business row for pending-write protection. */
  readonly getRowIdentity?: (row: T) => { table: string; id: string };
  readonly isInWindow?: (row: T) => boolean;
}

/** Shared by core and React so changing page configuration cannot reuse an incompatible store. */
export const infiniteQueryKey = <T extends { id: string }>(options: InfiniteQueryOptions<T>): string =>
  JSON.stringify([
    options.queryKey,
    options.orderBy,
    options.direction ?? 'desc',
    options.pageSize ?? 25,
    options.maxRows ?? 500,
    options.enabled !== false,
  ]);

export interface InfiniteQueryResult<T> {
  readonly data: T[];
  readonly items: readonly PagedRow<T>[];
  readonly isLoading: boolean;
  readonly isFetching: boolean;
  readonly isFetchingNextPage: boolean;
  readonly hasNextPage: boolean;
  readonly needsOlderWindow: boolean;
  readonly error: Error | undefined;
  readonly errors: ListErrors;
  readonly isError: boolean;
  readonly windowRevision: number;
}

export class OfflineError extends Error {
  override readonly name = 'OfflineError';
  constructor() {
    super('Additional records require a connection.');
  }
}

interface Page<T> {
  cursor: T | undefined;
  rows: readonly T[];
  end: boolean;
  fetchedAt: number;
  window: PendingWindow;
}

export interface QueryDeps extends RacedDeps {
  readonly connectivity: Store<boolean | undefined>;
}

/** One loaded-range watch and one ordered API cursor chain. Every requested page races both. */
export class InfiniteQueryStore<T extends { id: string }> {
  readonly store: SnapshotStore<InfiniteQueryResult<T>>;
  private readonly pageSize: number;
  private readonly maxRows: number;
  private readonly compare: (a: T, b: T) => number;
  private table: string | undefined;
  private target: number;
  private anchor: T | undefined;
  private revision = 0;
  private local = new Map<string, T>();
  private readonly retained = new Map<string, T>();
  private readonly tombstones = new Set<string>();
  private readonly filtered = new Set<string>();
  private readonly inSession = new Set<string>();
  private pages: Page<T>[] = [];
  private localPending = true;
  private localStamp = Number.NEGATIVE_INFINITY;
  private localError: Error | undefined;
  private apiError: Error | undefined;
  private apiPending = false;
  private apiFailed = false;
  private offline: boolean;
  private generation = 0;
  private watchGeneration = 0;
  private lastLocalWatch = 0;
  private refs = 0;
  private ready = false;
  private disposed = false;
  private unwatch: (() => void) | undefined;
  private stops: (() => void)[] = [];
  private request: AbortController | undefined;
  private requestKey: readonly unknown[] | undefined;

  constructor(
    private readonly deps: QueryDeps,
    private readonly options: InfiniteQueryOptions<T>,
  ) {
    this.pageSize = options.pageSize ?? 25;
    this.maxRows = options.maxRows ?? 500;
    if (
      !Number.isInteger(this.pageSize) ||
      this.pageSize < 1 ||
      !Number.isInteger(this.maxRows) ||
      this.maxRows < this.pageSize
    )
      throw new Error('powersync-bridge: pageSize must be positive and maxRows must be at least pageSize');
    this.target = this.pageSize;
    this.compare = rowComparator(options.orderBy, options.direction ?? 'desc');
    this.table = options.table;
    this.offline = this.readOffline();
    this.store = new SnapshotStore(this.result());
  }

  get isDisposed(): boolean {
    return this.disposed || (this.refs <= 0 && this.unwatch !== undefined);
  }
  retain(): void {
    this.refs += 1;
    if (this.refs !== 1 || this.unwatch || this.options.enabled === false) return;
    this.stops = [
      this.deps.crud.onChange(() => this.render()),
      this.deps.freshness.onCompletion(() => void this.checkpoint()),
      this.deps.driver.onStatusChanged(() => this.networkChanged()),
      this.deps.connectivity.subscribe(() => this.networkChanged()),
    ];
    this.watch();
    void this.deps.crud
      .ready()
      .then(() => {
        this.ready = true;
        this.pump();
      })
      .catch((error: unknown) => {
        if (this.disposed) return;
        this.localPending = false;
        this.localError = error instanceof Error ? error : new Error(String(error));
        this.render();
      });
  }
  release(): void {
    this.refs -= 1;
    if (this.refs <= 0)
      queueMicrotask(() => {
        if (this.refs <= 0) this.dispose();
      });
  }
  swapRoot(): void {
    this.disposed = true;
    this.local.clear();
    this.retained.clear();
    this.clearPages();
    this.render(true);
  }
  dispose(): void {
    this.disposed = true;
    this.generation += 1;
    this.watchGeneration += 1;
    this.cancelRequest();
    this.unwatch?.();
    this.unwatch = undefined;
    for (const stop of this.stops) stop();
    this.stops = [];
    this.clearPages();
    this.local.clear();
    this.retained.clear();
    this.tombstones.clear();
    this.filtered.clear();
    this.inSession.clear();
    this.render(true);
  }

  private readOffline(): boolean {
    return (this.deps.connectivity.getSnapshot() ?? this.deps.driver.onlineHint()) === false;
  }
  private cancelRequest(): void {
    this.request?.abort();
    if (this.requestKey) this.deps.snapshots.invalidate(this.requestKey);
    this.requestKey = undefined;
  }
  private networkChanged(): void {
    if (this.disposed) return;
    const offline = this.readOffline();
    const reconnect = this.offline && !offline;
    this.offline = offline;
    if (offline && this.request) {
      this.cancelRequest();
      this.generation += 1;
      this.apiPending = false;
      this.request = undefined;
    }
    if (reconnect) {
      this.apiFailed = false;
      this.apiError = undefined;
      this.pump();
    }
    this.render();
  }
  private identity(row: T): { table: string; id: string } {
    if (this.options.getRowIdentity) return this.options.getRowIdentity(row);
    if (!this.table)
      throw new Error(
        'powersync-bridge: provide table or getRowIdentity when the adapter cannot infer the business table',
      );
    return { table: this.table, id: idOf(row) };
  }
  private pending(row: T): boolean {
    const { table, id } = this.identity(row);
    return Boolean(
      this.deps.crud.pendingOp(table, id) ||
      this.pages.some((page) => this.deps.crud.pendingOp(table, id, page.window)),
    );
  }
  private localBuilder(): RowsBuilder<T> {
    return this.options.query({ cursor: this.anchor, limit: this.target + 1 });
  }
  private watch(): void {
    this.unwatch?.();
    const token = ++this.watchGeneration;
    const generation = this.generation;
    this.localPending = true;
    let prepared;
    try {
      prepared = this.deps.adapter.prepare(this.localBuilder());
      if (!this.table && prepared.tables.length === 1) this.table = prepared.tables[0];
      if (!this.table && !this.options.getRowIdentity)
        throw new Error(
          'powersync-bridge: provide table or getRowIdentity when the adapter cannot infer the business table',
        );
    } catch (error) {
      this.localPending = false;
      this.localError = error instanceof Error ? error : new Error(String(error));
      this.render();
      return;
    }
    this.unwatch = this.deps.driver.diffs(prepared).subscribe(
      (diff) => {
        void this.acceptLocal(diff.all, token, generation).catch((error: unknown) => {
          if (token !== this.watchGeneration || generation !== this.generation || this.disposed) return;
          this.localPending = false;
          this.localError = error instanceof Error ? error : new Error(String(error));
          this.render();
        });
      },
      (error) => {
        if (token !== this.watchGeneration || generation !== this.generation || this.disposed) return;
        this.localPending = false;
        this.localError = error;
        this.render();
      },
    );
    this.render();
  }
  private async acceptLocal(rows: readonly T[], token: number, generation: number): Promise<void> {
    await this.deps.crud.resync();
    if (token !== this.watchGeneration || generation !== this.generation || this.disposed) return;
    const previous = this.local;
    const firstEmission = this.lastLocalWatch !== token;
    this.lastLocalWatch = token;
    const next = new Map(rows.map((row) => [idOf(row), row]));
    const removed = new Map<string, T>();
    for (const [id, row] of previous)
      if (!next.has(id)) {
        removed.set(id, row);
        this.tombstones.add(id);
      }
    for (const [id, row] of next) {
      if (
        previous.size &&
        (!firstEmission || previous.has(id)) &&
        JSON.stringify(previous.get(id)) !== JSON.stringify(row) &&
        !this.pending(row)
      )
        this.inSession.add(id);
      this.tombstones.delete(id);
      this.filtered.delete(id);
      this.retained.delete(id);
    }
    this.local = next;
    this.localPending = false;
    this.localError = undefined;
    this.localStamp = this.deps.freshness.freshAtFor(undefined);
    this.render();
    // Probe by business table, including heterogeneous feeds whose display IDs are composite.
    const groups = new Map<string, Map<string, T>>();
    for (const row of removed.values()) {
      const identity = this.identity(row);
      const group = groups.get(identity.table) ?? new Map<string, T>();
      group.set(identity.id, row);
      groups.set(identity.table, group);
    }
    try {
      for (const [table, group] of groups) {
        const classes = await classifyRemovals(
          {
            driver: this.deps.driver,
            clock: this.deps.clock,
            table,
            provenance: () => undefined,
            isInWindow: this.options.isInWindow,
          },
          group,
        );
        if (token !== this.watchGeneration || generation !== this.generation || this.disposed) return;
        for (const row of removed.values()) {
          const identity = this.identity(row);
          if (identity.table !== table || this.local.has(row.id)) continue;
          const classification = classes.get(identity.id);
          if (!classification) continue;
          if (classification === 'delete') this.tombstones.add(row.id);
          else if (classification === 'filtered') {
            this.tombstones.delete(row.id);
            this.filtered.add(row.id);
          } else {
            this.tombstones.delete(row.id);
            this.retained.set(row.id, row);
          }
        }
      }
    } catch (error) {
      // A failed presence probe must not resurrect a deleted or filtered API row.
      if (token !== this.watchGeneration || generation !== this.generation || this.disposed) return;
      this.localError = error instanceof Error ? error : new Error(String(error));
    }
    const retained = [...this.retained.values()].sort(this.compare).slice(0, this.maxRows);
    this.retained.clear();
    for (const row of retained) this.retained.set(row.id, row);
    const known = new Set([
      ...this.local.keys(),
      ...this.retained.keys(),
      ...this.pages.flatMap((page) => page.rows.map((row) => row.id)),
    ]);
    for (const set of [this.inSession, this.tombstones, this.filtered]) {
      if (set.size > this.maxRows * 4) for (const id of set) if (!known.has(id)) set.delete(id);
    }
    this.render();
  }
  private async checkpoint(): Promise<void> {
    if (this.disposed || !this.unwatch) return;
    const token = this.watchGeneration;
    const generation = this.generation;
    try {
      const rows = await this.deps.driver.query(this.deps.adapter.prepare(this.localBuilder()));
      await this.acceptLocal(rows, token, generation);
    } catch (error) {
      if (token !== this.watchGeneration || generation !== this.generation || this.disposed) return;
      this.localError = error instanceof Error ? error : new Error(String(error));
      this.render();
    }
  }
  private clearPages(): void {
    for (const page of this.pages) this.deps.crud.closeWindow(page.window);
    this.pages = [];
  }
  private apiEnd(): boolean {
    return this.pages.at(-1)?.end === true;
  }
  private apiCount(): number {
    return this.pages.reduce((count, page) => count + page.rows.length, 0);
  }
  private apiCursor(): T | undefined {
    const last = this.pages.at(-1)?.rows.at(-1);
    return last ? last : this.anchor;
  }
  private cursorKey(row: T | undefined): readonly unknown[] | undefined {
    if (!row) return undefined;
    const [first, second] = keyColumns(this.options.orderBy);
    return [row[first], row[second]];
  }
  private pump(): void {
    if (
      !this.ready ||
      this.disposed ||
      this.options.enabled === false ||
      this.offline ||
      this.apiPending ||
      this.apiFailed ||
      this.apiEnd()
    )
      return;
    if (!this.table && !this.options.getRowIdentity) return;
    if (this.pages.length >= Math.ceil(this.maxRows / this.pageSize) + 1) return;
    if (this.apiCount() >= this.target && this.entries().length >= this.target) return;
    const cursor = this.apiCursor();
    const generation = this.generation;
    const request = new AbortController();
    this.request = request;
    const window = this.deps.crud.openWindow();
    this.apiPending = true;
    this.render();
    const key = [
      'powersync-bridge.query',
      this.deps.identityKey,
      this.deps.epoch,
      infiniteQueryKey(this.options),
      this.cursorKey(this.anchor),
      this.revision,
      generation,
      this.cursorKey(cursor),
    ];
    this.requestKey = key;
    void this.deps.snapshots
      .fetch<T>(key, this.options.staleTime ?? 60_000, async (signal) => {
        const abort = () => request.abort();
        signal.addEventListener('abort', abort, { once: true });
        try {
          return await withTimeout(
            this.deps.clock,
            this.options
              .fetchPage({ cursor, limit: this.pageSize + 1, signal: request.signal })
              .then((rows) => ({ rows })),
            30_000,
            request.signal,
          );
        } finally {
          signal.removeEventListener('abort', abort);
        }
      })
      .then((record) => {
        if (this.disposed || generation !== this.generation || request.signal.aborted) {
          this.deps.crud.closeWindow(window);
          return;
        }
        const rows = [...record.rows];
        for (let index = 1; index < rows.length; index++) {
          const before = rows[index - 1];
          const row = rows[index];
          if (before && row && this.compare(before, row) >= 0)
            throw new Error('powersync-bridge: API rows must use a strict, stable order with an id tiebreaker');
        }
        if (cursor !== undefined && rows.some((row) => this.compare(row, cursor) <= 0))
          throw new Error('powersync-bridge: API page must advance beyond the supplied cursor');
        this.pages.push({
          cursor,
          rows: rows.slice(0, this.pageSize),
          end: rows.length <= this.pageSize,
          fetchedAt: record.fetchedAt,
          window,
        });
        this.apiError = undefined;
      })
      .catch((error: unknown) => {
        this.deps.crud.closeWindow(window);
        if (this.disposed || generation !== this.generation || request.signal.aborted) return;
        this.apiFailed = true;
        this.apiError = error instanceof Error ? error : new Error(String(error));
        request.abort();
      })
      .finally(() => {
        if (generation !== this.generation || this.disposed) return;
        this.apiPending = false;
        this.request = undefined;
        this.requestKey = undefined;
        this.render();
        this.pump();
      });
  }

  private entries(): PagedRow<T>[] {
    const api = new Map<string, { row: T; page: Page<T> }>();
    for (const page of this.pages) for (const row of page.rows) api.set(row.id, { row, page });
    const ids = new Set([...this.local.keys(), ...this.retained.keys(), ...api.keys()]);
    const entries: PagedRow<T>[] = [];
    for (const id of ids) {
      if (this.tombstones.has(id) || this.filtered.has(id)) continue;
      const local = this.local.get(id);
      const remote = api.get(id);
      const row = local ?? remote?.row ?? this.retained.get(id);
      if (!row) continue;
      if (this.pending(row)) {
        if (local) entries.push({ row: local, origin: 'local', presentLocally: true });
        continue;
      }
      if (remote) {
        entries.push(
          local && this.localStamp > remote.page.fetchedAt
            ? { row: local, origin: 'local', presentLocally: true }
            : { row: remote.row, origin: 'api', presentLocally: Boolean(local) },
        );
      } else if (local) {
        const covering = this.pages.find((page) => {
          if (page.cursor !== undefined && this.compare(local, page.cursor) <= 0) return false;
          const last = page.rows.at(-1);
          return page.end || Boolean(last && this.compare(local, last) <= 0);
        });
        if (!covering || this.inSession.has(id) || this.localStamp > covering.fetchedAt)
          entries.push({ row: local, origin: 'local', presentLocally: true });
      } else entries.push({ row, origin: 'local', presentLocally: false });
    }
    return entries.sort((a, b) => this.compare(a.row, b.row));
  }
  private result(empty = false): InfiniteQueryResult<T> {
    const enabled = !empty && this.options.enabled !== false;
    const entries = enabled ? this.entries() : [];
    const items = entries.slice(0, this.target);
    const more = entries.length > this.target || !this.apiEnd();
    const missing = items.length < this.target;
    const errors: ListErrors = {};
    if (this.localError) errors.local = this.localError;
    if (this.apiError) errors.api = this.apiError;
    const offlineError = this.offline && missing && !this.localPending ? new OfflineError() : undefined;
    const error =
      enabled && missing && !this.localPending && !this.apiPending
        ? (this.localError ?? this.apiError ?? offlineError)
        : undefined;
    return {
      data: items.map((item) => item.row),
      items,
      isLoading: enabled && items.length === 0 && !error && (this.localPending || this.apiPending),
      isFetching: enabled && (this.localPending || this.apiPending),
      isFetchingNextPage: enabled && this.target > this.pageSize && (this.localPending || this.apiPending),
      hasNextPage: enabled && this.target < this.maxRows && more,
      needsOlderWindow: enabled && items.length >= this.maxRows && more,
      error,
      errors,
      isError: Boolean(error),
      windowRevision: this.revision,
    };
  }
  private render(empty = false): void {
    if (this.disposed && !empty) return;
    const result = this.result(empty);
    if (
      !shallowEqualResult(
        this.store.getSnapshot() as unknown as Record<string, unknown>,
        result as unknown as Record<string, unknown>,
      )
    )
      this.store.replace(result);
  }
  readonly fetchNextPage = (): void => {
    if (this.disposed || this.options.enabled === false || this.target >= this.maxRows) return;
    if (!this.store.getSnapshot().hasNextPage) return;
    this.target = Math.min(this.target + this.pageSize, this.maxRows);
    this.watch();
    this.pump();
  };
  readonly retry = (): void => {
    if (this.disposed || this.options.enabled === false) return;
    this.apiFailed = false;
    this.apiError = undefined;
    this.watch();
    this.pump();
  };
  readonly refresh = (): void => {
    this.reset(this.anchor);
  };
  readonly continueOlder = (): void => {
    const last = this.store.getSnapshot().data.at(-1);
    if (last) this.reset(last);
  };
  private reset(anchor: T | undefined): void {
    if (this.disposed || this.options.enabled === false) return;
    this.generation += 1;
    this.cancelRequest();
    this.request = undefined;
    this.apiPending = false;
    this.apiFailed = false;
    this.apiError = undefined;
    this.localError = undefined;
    this.clearPages();
    this.local.clear();
    this.retained.clear();
    this.inSession.clear();
    this.tombstones.clear();
    this.filtered.clear();
    this.target = this.pageSize;
    this.anchor = anchor;
    this.revision += 1;
    this.watch();
    this.pump();
  }
}
