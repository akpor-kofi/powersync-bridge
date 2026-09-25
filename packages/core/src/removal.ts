import type { BridgeClock } from './clock';
import type { BridgeDriver, StreamDescription } from './types';

/**
 * Four-way removal taxonomy (§6.4):
 *   filtered  — row still exists locally, no longer matches the list query → drop, no tombstone
 *   delete    — absent, and its producing stream is still tracked before expiry → drop + tombstone
 *   eviction  — absent, and its stream is gone or past expiry → retain, presentLocally:false
 *   retain    — classification impossible (predicate threw) → retain
 */
export type RemovalClass = 'filtered' | 'delete' | 'eviction' | 'retain';

export interface RemovalContext<T> {
  readonly driver: BridgeDriver;
  readonly clock: BridgeClock;
  readonly table: string;
  /** Producing stream for a row, when known (partition escalation). */
  readonly provenance: (row: T) => StreamDescription | undefined;
  readonly isInWindow?: ((row: T) => boolean) | undefined;
}

const PROBE_CHUNK = 500;

export async function probePresence(
  driver: BridgeDriver,
  table: string,
  ids: readonly string[],
): Promise<Set<string>> {
  const present = new Set<string>();
  for (let i = 0; i < ids.length; i += PROBE_CHUNK) {
    const chunk = ids.slice(i, i + PROBE_CHUNK);
    const found = await driver.presentIds(table, chunk);
    for (const id of found) present.add(id);
  }
  return present;
}

export async function classifyRemovals<T>(
  ctx: RemovalContext<T>,
  removed: ReadonlyMap<string, T>,
): Promise<Map<string, RemovalClass>> {
  const out = new Map<string, RemovalClass>();
  const ids = Array.from(removed.keys());
  if (ids.length === 0) return out;

  // ONE batched probe per emission (R14).
  const present = await probePresence(ctx.driver, ctx.table, ids);
  const now = ctx.clock.now();
  const status = ctx.driver.syncStatus();

  for (const [id, row] of removed) {
    if (present.has(id)) {
      out.set(id, 'filtered');
      continue;
    }
    const stream = ctx.provenance(row);
    if (stream) {
      const st = status.forStream(stream);
      const tracked = st !== undefined;
      const beforeExpiry = st?.active === true || st?.expiresAt === null || (st?.expiresAt !== undefined && st.expiresAt !== null && now < st.expiresAt.getTime());
      out.set(id, tracked && beforeExpiry ? 'delete' : 'eviction');
      continue;
    }
    if (ctx.isInWindow) {
      try {
        out.set(id, ctx.isInWindow(row) ? 'delete' : 'eviction');
      } catch {
        out.set(id, 'retain'); // a throwing predicate is never a delete
      }
      continue;
    }
    // No provenance and no predicate: the row left the synced set while its (auto) stream is
    // live — treat as delete (drop). Retention would mask real deletes.
    out.set(id, 'delete');
  }
  return out;
}
