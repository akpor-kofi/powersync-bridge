import { toCompilableQuery } from '@powersync/drizzle-driver';
import type { CompilableQuery, PreparedQuery, QueryAdapter, RowsBuilder } from 'powersync-bridge';

const mappings = new WeakMap<object, { source: RowsBuilder<unknown>; map: (row: unknown) => unknown }>();
const rawQueries = new WeakMap<object, PreparedQuery<unknown>>();

/** Map decoded ORM rows without losing table subscriptions or adapter metadata. */
export const mapQuery = <T, TSource = unknown>(source: RowsBuilder<TSource>, map: (row: TSource) => T): RowsBuilder<T> => {
  const builder: RowsBuilder<T> = { then: (resolve, reject) => Promise.resolve(source).then((rows) => rows.map(map)).then(resolve, reject) };
  mappings.set(builder, { source, map: (row) => map(row as TSource) });
  return builder;
};

/** Raw SQL remains a query definition; the bridge owns execution and table invalidation. */
export const rawQuery = <T>(
  db: { getAll<R>(sql: string, parameters: unknown[]): Promise<R[]> },
  statement: { text: string; params: readonly unknown[] },
  tables: readonly string[],
  map: (row: Record<string, unknown>) => T,
): RowsBuilder<T> => {
  const run = async (signal?: AbortSignal) => {
    if (signal?.aborted) throw signal.reason;
    const rows = await db.getAll<Record<string, unknown>>(statement.text, [...statement.params]);
    if (signal?.aborted) throw signal.reason;
    return rows.map(map);
  };
  const builder: RowsBuilder<T> = { then: (resolve, reject) => run().then(resolve, reject) };
  rawQueries.set(builder, { tables, run });
  return builder;
};

/** The parts of a Drizzle select builder the adapter relies on (kept structural). */
interface DrizzleSelectLike<T> extends PromiseLike<T[]> {
  toSQL(): { sql: string; params: unknown[] };
  execute?(): Promise<T[]>;
}

interface DrizzleDbLike {
  /** Drizzle exposes the schema it was created with under `_`. */
  readonly _?: { readonly schema?: Record<string, { readonly dbName?: string }> | undefined };
}

function isSelectLike<T>(b: RowsBuilder<T>): b is DrizzleSelectLike<T> {
  return typeof (b as { toSQL?: unknown }).toSQL === 'function';
}

/**
 * Derive the tables a query reads from its compiled SQL (F-022). Quoted identifiers are
 * intersected with the schema's table names when the schema is known; otherwise every quoted
 * identifier that follows FROM/JOIN is used.
 */
export function tablesFromSql(sql: string, known?: ReadonlySet<string>): string[] {
  const out = new Set<string>();
  const re = /\b(?:from|join)\s+"([^"]+)"/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(sql)) !== null) {
    const name = m[1];
    if (name && (!known || known.has(name))) out.add(name);
  }
  if (known) {
    // subqueries and CTEs reference tables without FROM/JOIN adjacency; include any known name present
    for (const name of known) if (sql.includes(`"${name}"`)) out.add(name);
  }
  return Array.from(out);
}

export interface DrizzleAdapterOptions {
  /** Override table derivation for exotic builders. */
  tablesOf?: (sql: string) => string[];
}

/**
 * `drizzleAdapter(drizzle)` — `drizzle` is `wrapPowerSyncWithDrizzle(db, { schema })`.
 * Queries execute through the ORM (result mappers decode mode-typed columns, F-010) and the
 * PreparedQuery carries a `compilable` so the driver can hand it to differentialWatch (R6).
 */
export function drizzleAdapter(orm: DrizzleDbLike, opts: DrizzleAdapterOptions = {}): QueryAdapter {
  const known = orm._?.schema ? new Set(Object.values(orm._.schema).map((t) => t.dbName).filter((n): n is string => typeof n === 'string')) : undefined;
  return {
    prepare<T>(builder: RowsBuilder<T>): PreparedQuery<T> {
      const raw = rawQueries.get(builder);
      if (raw) return raw as PreparedQuery<T>;
      const mapping = mappings.get(builder);
      if (mapping) {
        const prepared = this.prepare(mapping.source);
        return { tables: prepared.tables, run: async (signal) => (await prepared.run(signal)).map(mapping.map) as T[] };
      }
      if (!isSelectLike(builder)) {
        throw new Error('@powersync-bridge/drizzle: query must return a Drizzle select builder (it has no toSQL())');
      }
      const { sql } = builder.toSQL();
      const tables = opts.tablesOf ? opts.tablesOf(sql) : tablesFromSql(sql, known);
      const compilable = toCompilableQuery(builder as unknown as Parameters<typeof toCompilableQuery>[0]) as unknown as CompilableQuery<T>;
      return {
        tables,
        compilable,
        run: async () => compilable.execute(),
      };
    },
  };
}
