import type { AbstractPowerSyncDatabase, CrudEntry, SyncStatus } from '@powersync/common';
import type {
  BridgeDriver,
  CrudDelta,
  DifferentialStream,
  Params,
  PendingOp,
  PendingOpRef,
  PreparedQuery,
  PriorityStatus,
  QueryAdapter,
  RowDiff,
  StreamDescription,
  StreamHandle,
  StreamStatus,
  SyncStatusView,
} from 'powersync-bridge';

export interface PowerSyncDriverOptions {
  /** Web: navigator.onLine; React Native: NetInfo. Omit → undefined (unknown). */
  onlineHint?: () => boolean | undefined;
  /** Throttle for table-change notifications feeding differential streams. Default 30 ms. */
  throttleMs?: number;
}

/** PowerSync CrudEntry.op → bridge PendingOp. */
function mapOp(op: CrudEntry['op']): PendingOp {
  switch (String(op)) {
    case 'PUT':
      return 'INSERT';
    case 'PATCH':
      return 'UPDATE';
    default:
      return 'DELETE';
  }
}

function idOf(row: unknown): string {
  const id = (row as { id?: unknown }).id;
  if (id === undefined || id === null) throw new Error('@powersync-bridge/powersync: rows must have an `id`');
  return String(id);
}

function refKey(r: PendingOpRef): string {
  return `${r.table}\u0000${r.id}`;
}

function toStreamStatus(s: NonNullable<SyncStatus['syncStreams']>[number]): StreamStatus {
  const sub = s.subscription;
  return {
    name: sub.name,
    params: (sub.parameters as Params | null) ?? null,
    active: sub.active,
    expiresAt: sub.expiresAt ?? null,
    hasSynced: sub.hasSynced,
    lastSyncedAt: sub.lastSyncedAt ?? null,
  };
}

/**
 * The PowerSync driver (§9). Everything the engine needs from the SDK, and nothing else:
 * decoded queries through the adapter, differential streams, the upload-queue mirror emitting
 * {added, completed} deltas, batched presence probes, sync streams, and sync status.
 *
 * Differential streams: the SDK's `differentialWatch` only accepts raw SQL with a per-row
 * mapper, which cannot decode joined ORM rows (SQLite result columns collide across joins;
 * Drizzle maps positionally). The driver therefore listens to table changes with `onChange`
 * and loads DECODED rows through the adapter on each change, diffing by id once — the same
 * single O(n) pass the SDK would run, not a second one (§11).
 */
export class PowerSyncDriver implements BridgeDriver {
  private readonly crudMirror = new Map<string, PendingOpRef>();
  private readonly crudListeners = new Set<(delta: CrudDelta) => void>();
  private crudWatchStop: (() => void) | null = null;
  private crudResyncInFlight: Promise<void> | null = null;

  constructor(
    private readonly db: AbstractPowerSyncDatabase,
    private readonly adapter: QueryAdapter,
    private readonly opts: PowerSyncDriverOptions = {},
  ) {}

  get queryAdapter(): QueryAdapter {
    return this.adapter;
  }

  // -- queries ---------------------------------------------------------------

  async query<T>(q: PreparedQuery<T>, signal?: AbortSignal): Promise<T[]> {
    return q.run(signal);
  }

  diffs<T>(q: PreparedQuery<T>, opts: { throttleMs?: number } = {}): DifferentialStream<T> {
    return {
      subscribe: (onDiff, onError) => {
        let prev = new Map<string, string>();
        let first = true;
        let closed = false;
        let running: Promise<void> | null = null;
        let pending = false;

        const run = async (): Promise<void> => {
          if (closed) return;
          if (running) {
            pending = true; // coalesce
            return;
          }
          running = (async () => {
            try {
              const rows = await q.run();
              if (closed) return;
              const next = new Map<string, string>();
              const byId = new Map<string, T>();
              for (const r of rows) {
                const id = idOf(r);
                next.set(id, JSON.stringify(r));
                byId.set(id, r);
              }
              const added: T[] = [];
              const updated: T[] = [];
              const removed: string[] = [];
              for (const [id, json] of next) {
                const p = prev.get(id);
                if (p === undefined) added.push(byId.get(id) as T);
                else if (p !== json) updated.push(byId.get(id) as T);
              }
              for (const id of prev.keys()) if (!next.has(id)) removed.push(id);
              const changed = first || added.length > 0 || updated.length > 0 || removed.length > 0;
              prev = next;
              first = false;
              if (changed) {
                const diff: RowDiff<T> = { all: rows, added, updated, removed };
                onDiff(diff);
              }
            } catch (e) {
              onError?.(e instanceof Error ? e : new Error(String(e)));
            } finally {
              running = null;
              if (pending) {
                pending = false;
                void run();
              }
            }
          })();
          return running;
        };

        const stop = this.db.onChange(
          { onChange: () => void run() },
          { tables: [...q.tables], throttleMs: opts.throttleMs ?? this.opts.throttleMs ?? 30 },
        );
        void run();
        return () => {
          closed = true;
          stop();
        };
      },
    };
  }

  // -- upload queue mirror ----------------------------------------------------

  async pendingUploadIds(): Promise<PendingOpRef[]> {
    const refs: PendingOpRef[] = [];
    let batch = await this.db.getCrudBatch(100);
    while (batch) {
      for (const e of batch.crud) refs.push({ table: e.table, id: String(e.id), op: mapOp(e.op) });
      if (!batch.haveMore) break;
      batch = await this.db.getCrudBatch(100 * 2 ** Math.min(6, refs.length / 100));
      // NOTE: getCrudBatch always starts from the head of the queue; growing the limit pages the
      // whole queue in without calling complete(). Cap at 6,400 per call to bound the read.
      if (refs.length >= 6400) break;
    }
    // de-duplicate by (table,id), keeping the LAST op for an id
    const byKey = new Map<string, PendingOpRef>();
    for (const r of refs) byKey.set(refKey(r), r);
    return Array.from(byKey.values());
  }

  /** Diffs consecutive queue snapshots and EMITS the delta (F4-001, F5-003). */
  onCrudChange(cb: (delta: CrudDelta) => void): () => void {
    this.crudListeners.add(cb);
    if (this.crudListeners.size === 1) this.startCrudWatch();
    return () => {
      this.crudListeners.delete(cb);
      if (this.crudListeners.size === 0) this.stopCrudWatch();
    };
  }

  private startCrudWatch(): void {
    // A local write always changes a user table; an upload completion changes sync status.
    // Both trigger a re-read of the queue; the delta is derived by diffing snapshots.
    const stopChange = this.db.onChange({ onChange: () => void this.resyncCrud() }, { throttleMs: 50 });
    const stopStatus = this.db.registerListener({ statusChanged: () => void this.resyncCrud() });
    this.crudWatchStop = () => {
      stopChange();
      stopStatus();
    };
    void this.resyncCrud();
  }

  private stopCrudWatch(): void {
    this.crudWatchStop?.();
    this.crudWatchStop = null;
  }

  private resyncCrud(): Promise<void> {
    if (this.crudResyncInFlight) return this.crudResyncInFlight;
    this.crudResyncInFlight = (async () => {
      try {
        const refs = await this.pendingUploadIds();
        const next = new Map<string, PendingOpRef>();
        for (const r of refs) next.set(refKey(r), r);
        const added: PendingOpRef[] = [];
        const completed: PendingOpRef[] = [];
        for (const [k, r] of next) if (!this.crudMirror.has(k)) added.push(r);
        for (const [k, r] of this.crudMirror) if (!next.has(k)) completed.push(r);
        this.crudMirror.clear();
        for (const [k, r] of next) this.crudMirror.set(k, r);
        if (added.length || completed.length) {
          const delta: CrudDelta = { added, completed };
          for (const l of Array.from(this.crudListeners)) l(delta);
        }
      } finally {
        this.crudResyncInFlight = null;
      }
    })();
    return this.crudResyncInFlight;
  }

  // -- presence ----------------------------------------------------------------

  async presentIds(table: string, ids: readonly string[]): Promise<Set<string>> {
    if (ids.length === 0) return new Set();
    const quoted = `"${table.replace(/"/g, '""')}"`;
    const placeholders = ids.map(() => '?').join(', ');
    const rows = await this.db.getAll<{ id: string }>(`SELECT id FROM ${quoted} WHERE id IN (${placeholders})`, [...ids]);
    return new Set(rows.map((r) => String(r.id)));
  }

  onlineHint(): boolean | undefined {
    if (this.opts.onlineHint) return this.opts.onlineHint();
    const nav = (globalThis as { navigator?: { onLine?: boolean } }).navigator;
    return typeof nav?.onLine === 'boolean' ? nav.onLine : undefined;
  }

  // -- streams -----------------------------------------------------------------

  async syncStream(name: string, params: Params | null, opts: { ttl?: number; priority?: 0 | 1 | 2 | 3 } = {}): Promise<StreamHandle> {
    const stream = this.db.syncStream(name, params ?? undefined);
    const subOpts: { ttl?: number; priority?: 0 | 1 | 2 | 3 } = {};
    if (opts.ttl !== undefined) subOpts.ttl = opts.ttl;
    if (opts.priority !== undefined) subOpts.priority = opts.priority;
    const sub = await stream.subscribe(subOpts);
    return {
      name,
      params,
      waitForFirstSync: (signal?: AbortSignal) => sub.waitForFirstSync(signal),
      unsubscribe: () => sub.unsubscribe(),
    };
  }

  syncStatus(): SyncStatusView {
    const s = this.db.currentStatus;
    const streams = (s.syncStreams ?? []).map(toStreamStatus);
    return {
      connected: s.connected,
      downloading: s.dataFlowStatus.downloading,
      hasSynced: s.hasSynced,
      lastSyncedAt: s.lastSyncedAt ?? null,
      syncStreams: streams,
      forStream(desc: StreamDescription): StreamStatus | undefined {
        const key = `${desc.name}:${desc.params ? JSON.stringify(desc.params) : ''}`;
        return streams.find((x) => `${x.name}:${x.params ? JSON.stringify(x.params) : ''}` === key);
      },
      statusForPriority(priority: number): PriorityStatus {
        const p = s.statusForPriority(priority);
        return { hasSynced: p?.hasSynced ?? false, lastSyncedAt: p?.lastSyncedAt ?? null };
      },
    };
  }

  onStatusChanged(cb: () => void): () => void {
    return this.db.registerListener({ statusChanged: () => cb() });
  }
}

export function createPowerSyncDriver(
  db: AbstractPowerSyncDatabase,
  opts: PowerSyncDriverOptions & { adapter: QueryAdapter },
): PowerSyncDriver {
  const { adapter, ...rest } = opts;
  return new PowerSyncDriver(db, adapter, rest);
}
