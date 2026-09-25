import type { BridgeClock } from './clock';

/** Minimal TanStack QueryClient surface the bridge relies on (R11: fetchQuery + keying only). */
export interface MinimalQueryClient {
  fetchQuery<T>(opts: {
    queryKey: readonly unknown[];
    queryFn: (ctx: { signal: AbortSignal }) => Promise<T>;
    staleTime?: number;
  }): Promise<T>;
}

export interface SnapshotRecord<TApi> {
  readonly rows: readonly TApi[];
  readonly asOf: string | undefined;
  readonly requestedAt: number;
  readonly fetchedAt: number;
}

interface Entry<TApi> {
  requestedAt: number;
  promise: Promise<SnapshotRecord<TApi>>;
  result: SnapshotRecord<TApi> | null;
  controller: AbortController;
}

/**
 * One fetch per (identity epoch, queryKey, staleTime window) — mount-triggered only, never a
 * timer (R9). When a TanStack client is supplied it owns dedup + staleness; otherwise an
 * internal cache with identical semantics is used, so core keeps zero required dependencies.
 */
export class SnapshotCache {
  private readonly entries = new Map<string, Entry<unknown>>();

  constructor(
    private readonly clock: BridgeClock,
    private readonly queryClient: MinimalQueryClient | undefined,
  ) {}

  fetch<TApi>(
    key: readonly unknown[],
    staleTime: number,
    fetcher: (signal: AbortSignal) => Promise<{ rows: readonly TApi[]; asOf?: string }>,
  ): Promise<SnapshotRecord<TApi>> {
    const k = JSON.stringify(key);
    const now = this.clock.now();
    const existing = this.entries.get(k) as Entry<TApi> | undefined;
    if (existing && (existing.result === null || now - existing.result.fetchedAt < staleTime)) {
      return existing.promise;
    }
    const controller = new AbortController();
    const run = async (signal: AbortSignal): Promise<SnapshotRecord<TApi>> => {
      const res = await fetcher(signal);
      return { rows: res.rows, asOf: res.asOf, requestedAt: now, fetchedAt: this.clock.now() };
    };
    const promise = this.queryClient
      ? this.queryClient.fetchQuery<SnapshotRecord<TApi>>({ queryKey: key, queryFn: ({ signal }) => run(signal), staleTime })
      : run(controller.signal);
    const entry: Entry<TApi> = { requestedAt: now, promise, result: null, controller };
    this.entries.set(k, entry as Entry<unknown>);
    promise.then(
      (r) => {
        entry.result = r;
      },
      () => {
        if (this.entries.get(k) === (entry as Entry<unknown>)) this.entries.delete(k);
      },
    );
    return promise;
  }

  /** Cancel every in-flight fetch under a key prefix (identity swap). */
  abortAll(prefix: readonly unknown[]): void {
    const p = JSON.stringify(prefix).slice(0, -1); // drop the closing bracket → prefix match
    for (const [k, e] of this.entries) {
      if (k.startsWith(p)) {
        e.controller.abort(new Error('identity changed'));
        this.entries.delete(k);
      }
    }
  }

  clear(): void {
    for (const e of this.entries.values()) e.controller.abort(new Error('cleared'));
    this.entries.clear();
  }
}
