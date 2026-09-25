/**
 * Shared vocabulary — defined once (ARCHITECTURE.md §5.1, §6.1, §9).
 * No `any` in public signatures; the only `unknown` is an inbound raw payload.
 */

// ---------------------------------------------------------------------------
// Rows & list state (§5.1)
// ---------------------------------------------------------------------------

export type RowOrigin = 'local' | 'api';

export interface PagedRow<T> {
  readonly row: T;
  /** Producing source, post-arbitration. */
  readonly origin: RowOrigin;
  /** A LOCAL row exists in SQLite right now. Write guards key on this, never on origin (R14). */
  readonly presentLocally: boolean;
  /** Rendered from local before any fresh snapshot covered it (R12). */
  readonly stale?: true;
}

export type ListState = 'pending' | 'api' | 'local' | 'converged';

export type Boundary = 'more' | 'end' | 'unknown';
export type BoundaryReason = 'offline' | 'timeout' | 'error' | 'partition-cap';

export interface ListErrors {
  local?: Error;
  api?: Error;
  stream?: Error;
}

// ---------------------------------------------------------------------------
// Keys & cursors (§6.1, §8.3)
// ---------------------------------------------------------------------------

export type KeyOf<T> = keyof T & string;
export type OrderKey<T> = KeyOf<T> | readonly [KeyOf<T>, KeyOf<T>];
export type SortDirection = 'asc' | 'desc';

/** Cursor type for an order key: scalar for a single key, tuple for a 2-tuple (F3-010). */
export type CursorOf<T, K> = K extends readonly [infer A extends KeyOf<T>, infer B extends KeyOf<T>]
  ? [T[A], T[B]]
  : K extends KeyOf<T>
    ? T[K]
    : never;

/**
 * The cursor type of a definition — INCLUDES `undefined` (the first page). Annotate the page
 * argument of an infinite query with it:
 *   query: ({ cursor, limit }: PageArgs<Cursor<Sale, ['soldAt', 'id']>>, scope) => …
 * Every position that carries a cursor (page argument, partitionOf, fetchPage, initialCursor)
 * uses the same type, so TypeScript sees one consistent candidate for TCursor.
 */
export type Cursor<T, K extends OrderKey<T>> = CursorOf<T, K> | undefined;

/** Base64-encoded tuple the API never unpacks (§12.1). */
export type OpaqueCursor = string & { readonly __opaqueCursor: true };

export type Params = Record<string, string | number | boolean | null>;

// ---------------------------------------------------------------------------
// Driver surface (§9)
// ---------------------------------------------------------------------------

export type PendingOp = 'INSERT' | 'UPDATE' | 'DELETE';

export interface PendingOpRef {
  readonly table: string;
  readonly id: string;
  readonly op: PendingOp;
}

export interface CrudDelta {
  readonly added: readonly PendingOpRef[];
  readonly completed: readonly PendingOpRef[];
}

/** What the PowerSync SDK's `db.query()` accepts (kept structural: no SDK import in core). */
export interface CompilableQuery<T> {
  compile(): { sql: string; parameters?: unknown[] };
  execute(): Promise<T[]>;
}

/** A query the adapter has bound to the ORM. Rows come back DECODED (F-010). */
export interface PreparedQuery<T> {
  /** Tables the query reads; derived by the adapter from the builder (F-022). */
  readonly tables: readonly string[];
  run(signal?: AbortSignal): Promise<T[]>;
  /** When present, the driver hands this to the SDK's differentialWatch (R6). */
  readonly compilable?: CompilableQuery<T>;
}

/** One emission of a differential watch. `all` is the full current result, reference-stable per id. */
export interface RowDiff<T> {
  readonly all: readonly T[];
  readonly added: readonly T[];
  readonly updated: readonly T[];
  /** Ids no longer in the result set. */
  readonly removed: readonly string[];
}

export interface DifferentialStream<T> {
  subscribe(onDiff: (diff: RowDiff<T>) => void, onError?: (err: Error) => void): () => void;
}

export interface StreamDescription {
  readonly name: string;
  readonly params: Params | null;
}

export interface StreamStatus extends StreamDescription {
  readonly active: boolean;
  readonly expiresAt: Date | null;
  readonly hasSynced: boolean;
  readonly lastSyncedAt: Date | null;
}

export interface StreamHandle extends StreamDescription {
  /** Resolves when the subscription's first sync has been applied. Rejects on abort. */
  waitForFirstSync(signal?: AbortSignal): Promise<void>;
  unsubscribe(): void;
}

export interface PriorityStatus {
  readonly hasSynced: boolean;
  readonly lastSyncedAt: Date | null;
}

export interface SyncStatusView {
  readonly connected: boolean;
  readonly downloading: boolean;
  /** Persisted by the SDK: true at open on any previously synced DB. Never "answered". */
  readonly hasSynced: boolean | undefined;
  readonly lastSyncedAt: Date | null;
  readonly syncStreams: readonly StreamStatus[];
  forStream(desc: StreamDescription): StreamStatus | undefined;
  statusForPriority(priority: number): PriorityStatus;
}

export interface BridgeDriver {
  query<T>(q: PreparedQuery<T>, signal?: AbortSignal): Promise<T[]>;
  diffs<T>(q: PreparedQuery<T>, opts?: { throttleMs?: number }): DifferentialStream<T>;
  /** Full read of the upload queue (iterate `getCrudBatch` until `haveMore` is false), keyed (table, id). */
  pendingUploadIds(): Promise<PendingOpRef[]>;
  /** The driver diffs consecutive queue snapshots and EMITS the delta; core never re-diffs. */
  onCrudChange(cb: (delta: CrudDelta) => void): () => void;
  /** Batched presence probe: which of `ids` exist in `table` right now (R14). */
  presentIds(table: string, ids: readonly string[]): Promise<Set<string>>;
  /** A hint for the ApiPolicy gate, never a skip. `undefined` when unknown. */
  onlineHint(): boolean | undefined;
  syncStream(name: string, params: Params | null, opts?: { ttl?: number; priority?: 0 | 1 | 2 | 3 }): Promise<StreamHandle>;
  syncStatus(): SyncStatusView;
  onStatusChanged(cb: () => void): () => void;
}

// ---------------------------------------------------------------------------
// Adapter surface (§9): binds an ORM builder to a PreparedQuery.
// ---------------------------------------------------------------------------

/**
 * Anything whose awaited value is `T[]`. Drizzle/Kysely select builders are thenables
 * with exactly this shape, which is what lets `TItem` be inferred from the query (§8.1).
 */
export type RowsBuilder<T> = PromiseLike<T[]>;

export interface QueryAdapter {
  prepare<T>(builder: RowsBuilder<T>): PreparedQuery<T>;
}

// ---------------------------------------------------------------------------
// Definitions (§5.1, §6.1)
// ---------------------------------------------------------------------------

export interface Snapshot<TApi> {
  readonly rows: readonly TApi[];
  /** Optional PowerSync write checkpoint the backend served at (§10.2). */
  readonly asOf?: string;
}

/**
 * Definitions close over the app's ORM instance (`drizzle.select()…`) instead of receiving it:
 * a query callback whose every parameter is annotated is not context-sensitive, which is what
 * lets TypeScript infer TItem from its return (§8.1) alongside the other properties.
 */
export interface RacedListDefinition<TItem, TScope, TApi, K extends OrderKey<TItem>> {
  readonly kind: 'raced';
  readonly id: string;
  /** Builder; the adapter executes it decoded. ORDER BY … LIMIT. */
  readonly query: (scope: TScope) => RowsBuilder<TItem>;
  /** The API leg. */
  readonly fetchSnapshot: (scope: TScope, signal: AbortSignal) => Promise<Snapshot<TApi>>;
  /** Return type is the compile-time check against TItem (§8.2). */
  readonly mapApi: (raw: TApi) => TItem;
  readonly orderKey: K;
  /** Direction of the ORDER BY. Applies to every key of a tuple. Default 'asc'. */
  readonly direction?: SortDirection;
  readonly limit: number;
  /** ms; default 10 min. One snapshot per epoch × queryKey × window (R9). */
  readonly staleTime?: number;
  /** Owning stream; gates freshness per-stream (R5). */
  readonly stream?: StreamDescription;
  /** The table rows live in — needed for presence probes and write guards. Inferred by adapters when possible. */
  readonly table?: string;
  /** Keep api-only rows never seen locally after ownership (out-of-scope assumption). Default false. */
  readonly apiScopeSupersetOfLocal?: boolean;
  /** Pre-snapshot: render local rows flagged stale (default) or hold 'pending'. */
  readonly holdPending?: 'stale' | 'pending';
  /** Classifies rows for predicate windows (§6.4). */
  readonly isInWindow?: (row: TItem) => boolean;
}

export interface PageArgs<TCursor> {
  /** `undefined` for the first page (TCursor includes undefined — see `Cursor`). */
  readonly cursor: TCursor;
  /** The engine passes pageSize + 1 (lookahead). */
  readonly limit: number;
}

export interface PartitionPolicy<TCursor, TPartition> {
  readonly kind: 'partition';
  readonly stream: string;
  partitionOf(cursor: TCursor): TPartition | null;
  nextPartition(p: TPartition): TPartition | null;
  partitionParams(p: TPartition): Params;
  /** Static floor: always required. */
  readonly floor: TPartition;
  /** Optional exact floor from the server (a lower bound, re-validated on 'end'). */
  serverFloor?(): Promise<TPartition | null>;
  /** Compares partitions; default: string/number `<`. */
  isBefore?(a: TPartition, b: TPartition): boolean;
  readonly ttl?: number;
  readonly partitionCap?: number;
  readonly timeoutMs?: number;
}

export interface ApiPolicy<TCursor, TItem> {
  readonly kind: 'api';
  fetchPage(cursor: TCursor, signal: AbortSignal): Promise<readonly TItem[]>;
}

/** Degraded mode for offset-only APIs: id-set dedupe removes duplicates; skips are reported. */
export interface ApiOffsetPolicy<TItem> {
  readonly kind: 'api-offset';
  fetchPage(offset: number, signal: AbortSignal): Promise<readonly TItem[]>;
}

export type EscalationPolicy<TCursor, TItem, TPartition = string> =
  | PartitionPolicy<TCursor, TPartition>
  | ApiPolicy<TCursor, TItem>
  | ApiOffsetPolicy<TItem>;

/**
 * `TCursor` is a separate parameter, inferred from the ANNOTATED page argument
 * (`{ cursor, limit }: PageArgs<Cursor<Sale, ['soldAt','id']>>`) and the policy's cursor
 * positions, which all carry the same type.
 */
export interface InfiniteListDefinition<TItem, TScope, K extends OrderKey<TItem>, TCursor, TPartition = string> {
  readonly kind: 'infinite';
  readonly id: string;
  readonly pageSize: number;
  /** Page first so scope-less lists can omit the second parameter entirely. */
  readonly query: (page: PageArgs<TCursor>, scope: TScope) => RowsBuilder<TItem>;
  readonly orderKey: K;
  readonly direction?: SortDirection;
  readonly initialCursor?: TCursor;
  readonly escalate: EscalationPolicy<TCursor, TItem, TPartition>;
  readonly table?: string;
  readonly isInWindow?: (row: TItem) => boolean;
  /** Bridge-active partition subscriptions kept alive (LRU). Bounds memory, never buckets. */
  readonly keepPagesAlive?: number;
}

// ---------------------------------------------------------------------------
// Hook / store result shapes
// ---------------------------------------------------------------------------

export interface RacedListResult<TItem> {
  readonly items: readonly PagedRow<TItem>[];
  readonly state: ListState;
  readonly awaiting: { readonly local: boolean; readonly api: boolean };
  readonly errors: ListErrors;
}

export interface InfiniteListResult<TItem> {
  readonly items: readonly PagedRow<TItem>[];
  readonly boundary: Boundary;
  readonly boundaryReason?: BoundaryReason;
  readonly awaitingSync: boolean;
  readonly loading: boolean;
  readonly errors: ListErrors;
  /** Set in api-offset mode when a fetched page shows the list shifted under us. */
  readonly warnings: readonly string[];
}

export type OnDemandStatus = 'subscribing' | 'syncing' | 'synced' | 'timeout' | 'offline' | 'error';

export interface OnDemandResult {
  readonly status: OnDemandStatus;
  readonly error?: Error;
}

export interface Store<S> {
  getSnapshot(): S;
  subscribe(listener: () => void): () => void;
}

// ---------------------------------------------------------------------------
// Measurement (§5.4)
// ---------------------------------------------------------------------------

export interface RaceMeasurement {
  readonly list: string;
  readonly winner: 'local' | 'api' | 'none';
  readonly localMs: number | null;
  readonly apiMs: number | null;
  /** From connect() to the first in-session local freshness — never a persisted hasSynced. */
  readonly sessionFreshnessMs: number | null;
  readonly rowsRendered: number;
  readonly source: ListState;
}

export type MeasureSink = (m: RaceMeasurement) => void;
