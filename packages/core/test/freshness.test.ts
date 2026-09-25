import { describe, expect, it } from 'vitest';
import { defineRacedList } from '../src/index';
import { makeHarness, sale, salesQuery, wait, type Sale } from './harness';

type Api = { id: string; sold_at: string; total: number };

function setup(h: ReturnType<typeof makeHarness>) {
  let resolve!: (rows: Api[]) => void;
  const def = defineRacedList({
    id: 'sales',
    table: 'sales',
    query: (_scope: {}) => salesQuery(h.orm, 5),
    fetchSnapshot: () => new Promise<{ rows: Api[] }>((r) => (resolve = (rows) => r({ rows }))),
    mapApi: (o: Api): Sale => ({ id: o.id, soldAt: o.sold_at, total: o.total, status: 'OPEN' }),
    orderKey: ['soldAt', 'id'] as const,
    direction: 'desc',
    limit: 5,
  });
  const store = h.bridge.raced(def, {});
  store.retain();
  return { store, resolveApi: async (rows: Api[]) => { resolve(rows); await wait(); } };
}

describe('freshness (R3/R5)', () => {
  it('T24 (transition observed): a snapshot fetched mid-download is not beaten by the checkpoint that started before it', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03', { total: 1 })], { silent: true });
    h.driver.connect();
    const { store, resolveApi } = setup(h);
    await wait();
    h.driver.startDownload(); // t0
    await h.clock.advance(100);
    await resolveApi([{ id: 'a', sold_at: '2026-09-03', total: 2 }]); // fetched at t0+100, newer data
    await h.clock.advance(100);
    await h.driver.completeCheckpoint(() => h.driver.table('sales').set('a', sale('a', '2026-09-03', { total: 1 }))); // data from t0
    expect(store.store.getSnapshot().items[0]?.row.total).toBe(2); // API still wins: stamp t0 < fetchedAt
    expect(store.store.getSnapshot().state).toBe('api');
    // next checkpoint (started after completion) → local owns
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => h.driver.table('sales').set('a', sale('a', '2026-09-03', { total: 2 })));
    expect(store.store.getSnapshot().state).toBe('converged');
  });

  it('T33 (boot mid-download): no transition observed, ownership still transfers on the second completion', async () => {
    const h0 = makeHarness({ warmSince: 900_000 });
    // simulate: connection up and download already running BEFORE the bridge exists
    const clock = h0.clock;
    const driver = h0.driver;
    driver.connect();
    driver.startDownload();
    await clock.advance(50);
    // fresh bridge on the already-downloading driver
    const h = { ...h0 };
    const { createBridge } = await import('../src/index');
    const { FakeAdapter } = await import('../src/testing');
    h.bridge = createBridge({ driver, adapter: new FakeAdapter(driver), clock, platform: 'web' });
    await driver.synced('sales', [sale('a', '2026-09-03', { total: 1 })], { silent: true });
    const { store, resolveApi } = setup(h);
    await wait();
    await clock.advance(10);
    await resolveApi([{ id: 'a', sold_at: '2026-09-03', total: 2 }]);
    await clock.advance(10);
    await driver.completeCheckpoint(); // first completion: provisional stamp (init time) < fetchedAt
    expect(store.store.getSnapshot().state).toBe('api');
    driver.startDownload();
    await clock.advance(10);
    await driver.completeCheckpoint(() => driver.table('sales').set('a', sale('a', '2026-09-03', { total: 2 })));
    expect(store.store.getSnapshot().state).toBe('converged');
  });

  it('T34 (continuous downloading): the stamp advances on every completion even though downloading never idles', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03', { total: 1 })], { silent: true });
    h.driver.connect();
    h.driver.startDownload();
    const { store, resolveApi } = setup(h);
    await wait();
    await h.clock.advance(10);
    await resolveApi([{ id: 'a', sold_at: '2026-09-03', total: 2 }]);
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => {}, { keepDownloading: true }); // stamp ≤ start < fetchedAt
    expect(store.store.getSnapshot().state).toBe('api');
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => h.driver.table('sales').set('a', sale('a', '2026-09-03', { total: 2 })), { keepDownloading: true });
    expect(store.store.getSnapshot().state).toBe('converged'); // stamp = previous completion > fetchedAt
  });

  it('T25: a service restart resetting lastSyncedAt to null never demotes local', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03', { total: 1 })], { silent: true });
    h.driver.connect();
    const { store, resolveApi } = setup(h);
    await wait();
    await h.clock.advance(10);
    await resolveApi([{ id: 'a', sold_at: '2026-09-03', total: 2 }]);
    await h.clock.advance(10);
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => h.driver.table('sales').set('a', sale('a', '2026-09-03', { total: 3 })));
    expect(store.store.getSnapshot().state).toBe('converged');
    h.driver.serviceRestart();
    await wait();
    expect(store.store.getSnapshot().state).toBe('converged');
    expect(store.store.getSnapshot().items[0]?.row.total).toBe(3);
  });

  it('T26: a reconnect does not reset sessionConnectAt; pre-blip freshness stands', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03')], { silent: true });
    h.driver.connect();
    const { store, resolveApi } = setup(h);
    await wait();
    await resolveApi([{ id: 'a', sold_at: '2026-09-03', total: 10 }]);
    await h.clock.advance(10);
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint();
    expect(store.store.getSnapshot().state).toBe('converged');
    h.driver.disconnect();
    await h.clock.advance(1000);
    h.driver.connect();
    await wait();
    expect(store.store.getSnapshot().state).toBe('converged');
  });
});
