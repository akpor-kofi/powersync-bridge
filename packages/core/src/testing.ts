/**
 * `powersync-bridge/testing` — a fake driver with VIRTUAL TIME for deterministic interleavings.
 * Every invariant test (T1–T44) runs on this. Nothing here touches PowerSync.
 */
import type { BridgeClock } from './clock';
import { streamKey } from './freshness';
import { idOf } from './keyset';
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
  RowsBuilder,
  StreamDescription,
  StreamHandle,
  StreamStatus,
  SyncStatusView,
} from './types';

export type Row = Record<string, unknown> & { id: string };

// ---------------------------------------------------------------------------
// Virtual clock
// ---------------------------------------------------------------------------

export class VirtualClock implements BridgeClock {
  private t = 1_000_000;
  private timers: Array<{ at: number; fn: () => void; id: number }> = [];
  private seq = 0;

  now(): number {
    return this.t;
  }

  setTimeout(fn: () => void, ms: number): () => void {
    const id = ++this.seq;
    this.timers.push({ at: this.t + ms, fn, id });
    return () => {
      this.timers = this.timers.filter((x) => x.id !== id);
    };
  }

  /** Advance time, firing due timers in order. */
  async advance(ms: number): Promise<void> {
    const target = this.t + ms;
    for (;;) {
      const due = this.timers.filter((x) => x.at <= target).sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      this.timers = this.timers.filter((x) => x.id !== due.id);
      this.t = due.at;
      due.fn();
      await flush();
    }
    this.t = target;
    await flush();
  }
}

/** Drain microtasks (the engines do async work after every emission). */
export async function flush(rounds = 8): Promise<void> {
  for (let i = 0; i < rounds; i++) await new Promise<void>((r) => setTimeout(r, 0));
}

// ---------------------------------------------------------------------------
// Fake ORM: queries are functions over in-memory tables
// ---------------------------------------------------------------------------

export interface FakeQuery<T> extends RowsBuilder<T> {
  readonly __tables: readonly string[];
  readonly __run: () => T[];
}

export class FakeOrm {
  constructor(readonly driver: FakeDriver) {}

  /** A query over `tables` computed by `fn`; thenable so TItem infers like a Drizzle builder. */
  query<T>(tables: readonly string[], fn: (db: FakeDriver) => T[]): FakeQuery<T> {
    const driver = this.driver;
    const q = {
      __tables: tables,
      __run: () => fn(driver),
      then<R1 = T[], R2 = never>(onFul?: ((v: T[]) => R1 | PromiseLike<R1>) | null, onRej?: ((e: unknown) => R2 | PromiseLike<R2>) | null): PromiseLike<R1 | R2> {
        return Promise.resolve(fn(driver)).then(onFul ?? undefined, onRej ?? undefined);
      },
    };
    return q as FakeQuery<T>;
  }

  rows<T extends Row = Row>(table: string): T[] {
    return Array.from(this.driver.tables.get(table)?.values() ?? []) as T[];
  }
}

export class FakeAdapter implements QueryAdapter {
  readonly orm: FakeOrm;
  constructor(driver: FakeDriver) {
    this.orm = new FakeOrm(driver);
  }
  prepare<T>(builder: RowsBuilder<T>): PreparedQuery<T> {
    const q = builder as FakeQuery<T>;
    return { tables: q.__tables, run: async () => q.__run() };
  }
}

// ---------------------------------------------------------------------------
// Fake driver
// ---------------------------------------------------------------------------

interface FakeStream {
  desc: StreamDescription;
  refs: number;
  expiresAt: number | null;
  hasSynced: boolean;
  lastSyncedAt: number | null;
  firstSync: { promise: Promise<void>; resolve: () => void };
}

interface DiffSub<T> {
  q: PreparedQuery<T>;
  prev: Map<string, string>;
  onDiff: (d: RowDiff<T>) => void;
  first: boolean;
}

export class FakeDriver implements BridgeDriver {
  readonly tables = new Map<string, Map<string, Row>>();
  readonly crud = new Map<string, PendingOpRef>();
  readonly streams = new Map<string, FakeStream>();
  readonly clock: VirtualClock;

  private connected = false;
  private downloading = false;
  private hasSynced: boolean | undefined = false;
  private lastSyncedAt: number | null = null;
  private online: boolean | undefined = true;
  private readonly crudListeners = new Set<(d: CrudDelta) => void>();
  private readonly statusListeners = new Set<() => void>();
  private readonly diffSubs = new Set<DiffSub<unknown>>();
  readonly presentIdsCalls: Array<{ table: string; ids: readonly string[] }> = [];
  readonly subscribeLog: StreamDescription[] = [];
  streamTtlDefault = 86_400_000;

  constructor(clock = new VirtualClock()) {
    this.clock = clock;
  }

  // -- BridgeDriver -----------------------------------------------------------

  async query<T>(q: PreparedQuery<T>): Promise<T[]> {
    return q.run();
  }

  diffs<T>(q: PreparedQuery<T>): DifferentialStream<T> {
    return {
      subscribe: (onDiff, onError) => {
        const sub: DiffSub<T> = { q, prev: new Map(), onDiff, first: true };
        this.diffSubs.add(sub as DiffSub<unknown>);
        queueMicrotask(() => {
          if (this.diffSubs.has(sub as DiffSub<unknown>)) void this.emitFor(sub, onError);
        });
        return () => this.diffSubs.delete(sub as DiffSub<unknown>);
      },
    };
  }

  async pendingUploadIds(): Promise<PendingOpRef[]> {
    return Array.from(this.crud.values());
  }

  onCrudChange(cb: (delta: CrudDelta) => void): () => void {
    this.crudListeners.add(cb);
    return () => this.crudListeners.delete(cb);
  }

  async presentIds(table: string, ids: readonly string[]): Promise<Set<string>> {
    this.presentIdsCalls.push({ table, ids });
    const t = this.tables.get(table);
    return new Set(ids.filter((id) => t?.has(id)));
  }

  onlineHint(): boolean | undefined {
    return this.online;
  }

  async syncStream(name: string, params: Params | null, opts: { ttl?: number } = {}): Promise<StreamHandle> {
    const desc = { name, params };
    const key = streamKey(desc);
    this.subscribeLog.push(desc);
    let s = this.streams.get(key);
    if (!s) {
      s = { desc, refs: 0, expiresAt: null, hasSynced: false, lastSyncedAt: null, firstSync: deferred() };
      this.streams.set(key, s);
      (s as FakeStream & { ttl?: number }).ttl = (opts.ttl ?? this.streamTtlDefault / 1000) * 1000; // first TTL wins
    }
    s.refs += 1;
    s.expiresAt = null;
    const stream = s;
    let done = false;
    return {
      name,
      params,
      waitForFirstSync: (signal?: AbortSignal) =>
        new Promise<void>((resolve, reject) => {
          stream.firstSync.promise.then(resolve);
          signal?.addEventListener('abort', () => reject(signal.reason ?? new Error('aborted')), { once: true });
        }),
      unsubscribe: () => {
        if (done) return;
        done = true;
        stream.refs -= 1;
        if (stream.refs <= 0) stream.expiresAt = this.clock.now() + ((stream as FakeStream & { ttl?: number }).ttl ?? this.streamTtlDefault);
        this.notifyStatus();
      },
    };
  }

  syncStatus(): SyncStatusView {
    const now = this.clock.now();
    const syncStreams: StreamStatus[] = [];
    for (const s of this.streams.values()) {
      if (s.expiresAt !== null && now >= s.expiresAt) continue; // expired: no longer tracked
      syncStreams.push({
        name: s.desc.name,
        params: s.desc.params,
        active: s.refs > 0,
        expiresAt: s.expiresAt === null ? null : new Date(s.expiresAt),
        hasSynced: s.hasSynced,
        lastSyncedAt: s.lastSyncedAt === null ? null : new Date(s.lastSyncedAt),
      });
    }
    const self = this;
    return {
      connected: this.connected,
      downloading: this.downloading,
      hasSynced: this.hasSynced,
      lastSyncedAt: this.lastSyncedAt === null ? null : new Date(this.lastSyncedAt),
      syncStreams,
      forStream(desc) {
        return syncStreams.find((x) => streamKey(x) === streamKey(desc));
      },
      statusForPriority(): PriorityStatus {
        return { hasSynced: self.hasSynced === true, lastSyncedAt: self.lastSyncedAt === null ? null : new Date(self.lastSyncedAt) };
      },
    };
  }

  onStatusChanged(cb: () => void): () => void {
    this.statusListeners.add(cb);
    return () => this.statusListeners.delete(cb);
  }

  // -- Test helpers: sync side -----------------------------------------------

  /** Open a previously synced DB: hasSynced is PERSISTED (true before any connect). */
  openWarm(lastSyncedAt: number): void {
    this.hasSynced = true;
    this.lastSyncedAt = lastSyncedAt;
  }

  connect(): void {
    this.connected = true;
    this.notifyStatus();
  }

  disconnect(): void {
    this.connected = false;
    this.notifyStatus();
  }

  startDownload(): void {
    this.downloading = true;
    this.notifyStatus();
  }

  /** A checkpoint completes: rows in `apply` land, lastSyncedAt advances, downloading ends unless `keepDownloading`. */
  async completeCheckpoint(apply: () => void = () => {}, opts: { keepDownloading?: boolean; stream?: StreamDescription } = {}): Promise<void> {
    apply();
    this.lastSyncedAt = this.clock.now();
    this.hasSynced = true;
    this.downloading = opts.keepDownloading ?? false;
    if (opts.stream) {
      const s = this.streams.get(streamKey(opts.stream));
      if (s) {
        s.hasSynced = true;
        s.lastSyncedAt = this.clock.now();
      }
    }
    this.notifyStatus();
    await this.emitAll();
  }

  /** The service restarted: the SDK resets lastSyncedAt to null. */
  serviceRestart(): void {
    this.lastSyncedAt = null;
    this.notifyStatus();
  }

  /** disconnectAndClear(): tables wiped, hasSynced → false. */
  async clear(): Promise<void> {
    this.tables.clear();
    this.crud.clear();
    this.hasSynced = false;
    this.lastSyncedAt = null;
    this.connected = false;
    this.notifyStatus();
    await this.emitAll();
  }

  setOnline(v: boolean | undefined): void {
    this.online = v;
  }

  /** Resolve a stream subscription's first sync, landing `apply` rows. */
  async resolveFirstSync(desc: StreamDescription, apply: () => void = () => {}): Promise<void> {
    const s = this.streams.get(streamKey(desc));
    if (!s) throw new Error(`no subscription for ${streamKey(desc)}`);
    apply();
    s.hasSynced = true;
    s.lastSyncedAt = this.clock.now();
    s.firstSync.resolve();
    this.notifyStatus();
    await this.emitAll();
  }

  /** Expire every TTL-pending stream whose expiry has passed and remove its rows (`ownerOf` maps a row to its stream). */
  async expireStreams(ownerOf: (table: string, row: Row) => StreamDescription | undefined): Promise<void> {
    const now = this.clock.now();
    for (const [key, s] of this.streams) {
      if (s.expiresAt !== null && now >= s.expiresAt) {
        for (const [table, rows] of this.tables) {
          for (const [id, row] of rows) {
            const owner = ownerOf(table, row);
            if (owner && streamKey(owner) === key) rows.delete(id);
          }
        }
        this.streams.delete(key);
      }
    }
    this.notifyStatus();
    await this.emitAll();
  }

  // -- Test helpers: data side ----------------------------------------------

  /** Get-or-create a table map (tests write rows directly through this). */
  table(name: string): Map<string, Row> {
    let t = this.tables.get(name);
    if (!t) {
      t = new Map();
      this.tables.set(name, t);
    }
    return t;
  }

  /** Rows arriving via sync (no crud entry). Emits diffs unless `silent`. */
  async synced(table: string, rows: Row[], opts: { silent?: boolean; delete?: boolean } = {}): Promise<void> {
    const t = this.table(table);
    for (const r of rows) {
      if (opts.delete) t.delete(r.id);
      else t.set(r.id, { ...r });
    }
    if (!opts.silent) await this.emitAll();
  }

  /** A local write: row applied + crud entry + delta + diffs. */
  async writeLocal(table: string, op: PendingOp, row: Row): Promise<void> {
    const t = this.table(table);
    if (op === 'DELETE') t.delete(row.id);
    else t.set(row.id, { ...(t.get(row.id) ?? {}), ...row });
    const ref: PendingOpRef = { table, id: row.id, op };
    this.crud.set(`${table}\u0000${row.id}`, ref);
    for (const l of this.crudListeners) l({ added: [ref], completed: [] });
    await this.emitAll();
  }

  /** Uploads complete: crud drained, completed delta emitted. */
  completeUploads(filter?: (r: PendingOpRef) => boolean): void {
    const completed: PendingOpRef[] = [];
    for (const [k, r] of this.crud) {
      if (!filter || filter(r)) {
        this.crud.delete(k);
        completed.push(r);
      }
    }
    if (completed.length) for (const l of this.crudListeners) l({ added: [], completed });
  }

  /** Re-run every differential watch and emit changes (what a table change notification does). */
  async emitAll(): Promise<void> {
    for (const sub of Array.from(this.diffSubs)) await this.emitFor(sub);
    await flush(2);
  }

  notifyStatus(): void {
    for (const l of Array.from(this.statusListeners)) l();
  }

  private async emitFor<T>(sub: DiffSub<T>, onError?: (e: Error) => void): Promise<void> {
    let rows: T[];
    try {
      rows = await sub.q.run();
    } catch (e) {
      onError?.(e instanceof Error ? e : new Error(String(e)));
      return;
    }
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
      const p = sub.prev.get(id);
      if (p === undefined) added.push(byId.get(id) as T);
      else if (p !== json) updated.push(byId.get(id) as T);
    }
    for (const id of sub.prev.keys()) if (!next.has(id)) removed.push(id);
    const changed = sub.first || added.length || updated.length || removed.length;
    sub.prev = next;
    sub.first = false;
    if (changed) sub.onDiff({ all: rows, added, updated, removed });
  }
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/** Build an unsigned JWT for identity tests. */
export function fakeJwt(claims: Record<string, unknown>): string {
  const enc = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
  return `${enc({ alg: 'none' })}.${enc(claims)}.sig`;
}
