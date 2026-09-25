import type { CursorOf, KeyOf, OpaqueCursor, OrderKey, SortDirection } from './types';

/** `id` is always the implicit tiebreaker (§8.3): a scalar key K is treated as [K, 'id']. */
export function keyColumns<T>(orderKey: OrderKey<T>): [KeyOf<T>, KeyOf<T>] {
  if (typeof orderKey === 'string') return [orderKey, 'id' as KeyOf<T>];
  return [orderKey[0], orderKey[1]];
}

export function idOf(row: unknown): string {
  const r = row as { id?: unknown };
  if (r == null || r.id === undefined || r.id === null) {
    throw new Error('powersync-bridge: every row must have an `id`');
  }
  return String(r.id);
}

function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return 1; // nulls last
  if (b === null || b === undefined) return -1;
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  if (typeof a === 'boolean' && typeof b === 'boolean') return Number(a) - Number(b);
  const sa = String(a);
  const sb = String(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

/** Comparator over rows for the declared order key + direction. Byte order for strings (SQLite BINARY). */
export function rowComparator<T>(orderKey: OrderKey<T>, direction: SortDirection = 'asc'): (a: T, b: T) => number {
  const [k1, k2] = keyColumns(orderKey);
  const sign = direction === 'desc' ? -1 : 1;
  return (a, b) => {
    const c1 = compareValues((a as Record<string, unknown>)[k1], (b as Record<string, unknown>)[k1]);
    if (c1 !== 0) return c1 * sign;
    return compareValues((a as Record<string, unknown>)[k2], (b as Record<string, unknown>)[k2]) * sign;
  };
}

/** The cursor a row yields, in the definition's cursor shape. */
export function cursorOfRow<T, K extends OrderKey<T>>(row: T, orderKey: K): CursorOf<T, K> {
  const r = row as Record<string, unknown>;
  if (typeof orderKey === 'string') return r[orderKey] as CursorOf<T, K>;
  return [r[orderKey[0]], r[orderKey[1]]] as CursorOf<T, K>;
}

/** Whether `row` sorts strictly after `cursor` in the list's order (i.e. is "beyond" it). */
export function isAfterCursor<T, K extends OrderKey<T>>(
  row: T,
  cursor: CursorOf<T, K>,
  orderKey: K,
  direction: SortDirection = 'asc',
): boolean {
  const [k1, k2] = keyColumns(orderKey);
  const r = row as Record<string, unknown>;
  const sign = direction === 'desc' ? -1 : 1;
  const [c1, c2] = typeof orderKey === 'string' ? [cursor, undefined] : (cursor as [unknown, unknown]);
  const d1 = compareValues(r[k1], c1) * sign;
  if (d1 !== 0) return d1 > 0;
  if (c2 === undefined) return false;
  return compareValues(r[k2], c2) * sign > 0;
}

function toBase64(s: string): string {
  if (typeof btoa === 'function') return btoa(unescape(encodeURIComponent(s)));
  return Buffer.from(s, 'utf8').toString('base64');
}

function fromBase64(s: string): string {
  if (typeof atob === 'function') return decodeURIComponent(escape(atob(s)));
  return Buffer.from(s, 'base64').toString('utf8');
}

export function encodeCursor(cursor: unknown): OpaqueCursor {
  return toBase64(JSON.stringify(cursor)) as OpaqueCursor;
}

export function decodeCursor<C>(opaque: OpaqueCursor): C {
  return JSON.parse(fromBase64(opaque)) as C;
}
