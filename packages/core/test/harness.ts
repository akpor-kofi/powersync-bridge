import { createBridge, type Bridge } from '../src/index';
import { FakeAdapter, FakeDriver, FakeOrm, VirtualClock, flush, type Row } from '../src/testing';
import type { MeasureSink } from '../src/types';

export interface Sale extends Row {
  id: string;
  soldAt: string;
  total: number;
  status: 'OPEN' | 'PAID' | 'VOID';
}

export interface Harness {
  driver: FakeDriver;
  clock: VirtualClock;
  orm: FakeOrm;
  bridge: Bridge;
  measurements: Parameters<MeasureSink>[0][];
}

export function makeHarness(opts: { platform?: 'web' | 'native'; warmSince?: number } = {}): Harness {
  const clock = new VirtualClock();
  const driver = new FakeDriver(clock);
  if (opts.warmSince !== undefined) driver.openWarm(opts.warmSince);
  const adapter = new FakeAdapter(driver);
  const measurements: Parameters<MeasureSink>[0][] = [];
  const bridge = createBridge({
    driver,
    adapter,
    clock,
    platform: opts.platform ?? 'web',
    measure: { sink: (m) => measurements.push(m) },
  });
  return { driver, clock, orm: adapter.orm, bridge, measurements };
}

export function sale(id: string, soldAt: string, patch: Partial<Sale> = {}): Sale {
  return { id, soldAt, total: 10, status: 'OPEN', ...patch };
}

/** Local query: sales ordered by (soldAt DESC, id DESC), limited. */
export function salesQuery(orm: FakeOrm, limit: number, cursor?: [string, string]) {
  return orm.query<Sale>(['sales'], (db) => {
    let rows = db.tables.get('sales') ? (Array.from(db.tables.get('sales')!.values()) as Sale[]) : [];
    if (cursor) rows = rows.filter((r) => r.soldAt < cursor[0] || (r.soldAt === cursor[0] && r.id < cursor[1]));
    rows.sort((a, b) => (a.soldAt === b.soldAt ? (a.id < b.id ? 1 : -1) : a.soldAt < b.soldAt ? 1 : -1));
    return rows.slice(0, limit);
  });
}

export const wait = flush;

export function ids<T extends { row: { id: string } }>(items: readonly T[]): string[] {
  return items.map((i) => i.row.id);
}
