import { describe, expect, it } from 'vitest';
import { defineRacedList } from '../src/index';
import { type Harness, ids, makeHarness, sale, salesQuery, wait, type Sale } from './harness';

type Api = { id: string; sold_at: string; total: number; status: 'OPEN' | 'PAID' | 'VOID' };

function apiRow(s: Sale): Api {
  return { id: s.id, sold_at: s.soldAt, total: s.total, status: s.status };
}

function defineRecent(h: Harness, fetch: (signal: AbortSignal) => Promise<Api[]>, extra: { apiScopeSupersetOfLocal?: boolean } = {}) {
  return defineRacedList({
    id: 'recent-sales',
    table: 'sales',
    query: (_scope: { org: string }) => salesQuery(h.orm, 3),
    fetchSnapshot: async (_scope, signal) => ({ rows: await fetch(signal) }),
    mapApi: (o: Api): Sale => ({ id: o.id, soldAt: o.sold_at, total: o.total, status: o.status }),
    orderKey: ['soldAt', 'id'] as const,
    direction: 'desc',
    limit: 3,
    ...extra,
  });
}

/** A controllable API leg. */
function apiLeg() {
  const pending: Array<{ resolve: (rows: Api[]) => void; reject: (e: Error) => void }> = [];
  return {
    fetch: (_signal: AbortSignal) => new Promise<Api[]>((resolve, reject) => pending.push({ resolve, reject })),
    resolve: async (rows: Api[]) => {
      pending.shift()?.resolve(rows);
      await wait();
    },
    reject: async (e = new Error('api down')) => {
      pending.shift()?.reject(e);
      await wait();
    },
    get inflight() {
      return pending.length;
    },
  };
}

describe('raced list — first visit (V1)', () => {
  it('T-empty-local: an empty pre-sync table is not an answer; the API snapshot paints', async () => {
    const h = makeHarness();
    h.driver.connect();
    const api = apiLeg();
    const def = defineRecent(h, api.fetch);
    const store = h.bridge.raced(def, { org: 'o' });
    store.retain();
    await wait();
    expect(store.store.getSnapshot().state).toBe('local'); // empty local rendered stale, not "answered"
    expect(store.store.getSnapshot().items).toHaveLength(0);

    await api.resolve([apiRow(sale('a', '2026-09-03')), apiRow(sale('b', '2026-09-02'))]);
    const r = store.store.getSnapshot();
    expect(r.state).toBe('api');
    expect(ids(r.items)).toEqual(['a', 'b']);
    expect(r.items.every((i) => i.origin === 'api' && i.presentLocally === false)).toBe(true);
  });

  it('T2: pre-first-sync conflict resolves to API; ownership transfers once local is fresher, one-directional', async () => {
    const h = makeHarness({ warmSince: 900_000 }); // warm DB: hasSynced persisted true
    await h.driver.synced('sales', [sale('a', '2026-09-03', { total: 1 }), sale('b', '2026-09-02')], { silent: true });
    h.driver.connect();
    const api = apiLeg();
    const def = defineRecent(h, api.fetch);
    const store = h.bridge.raced(def, { org: 'o' });
    store.retain();
    await wait();
    // local emitted (warm, stale) → rendered stale
    expect(store.store.getSnapshot().state).toBe('local');
    expect(store.store.getSnapshot().items[0]?.stale).toBe(true);

    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03', { total: 99 })), apiRow(sale('b', '2026-09-02'))]);
    let r = store.store.getSnapshot();
    expect(r.state).toBe('api'); // persisted hasSynced never counts: API is fresher
    expect(r.items.find((i) => i.row.id === 'a')?.row.total).toBe(99);

    const history: number[] = [];
    store.store.subscribe(() => history.push(store.store.getSnapshot().items.find((i) => i.row.id === 'a')?.row.total ?? -1));

    // first in-session checkpoint (download starts AFTER the snapshot) → local fresher → owns
    await h.clock.advance(10);
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => {
      h.driver.table('sales').set('a', sale('a', '2026-09-03', { total: 99 }));
    });
    r = store.store.getSnapshot();
    expect(r.state).toBe('converged');
    expect(r.items.find((i) => i.row.id === 'a')?.row.total).toBe(99);
    // no fresh→old→fresh oscillation: value never went back to 1
    expect(history).not.toContain(1);
  });

  it('T3/T17: API failure pre-snapshot renders local rows stale — never a blank list', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03')], { silent: true });
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    await api.reject();
    const r = store.store.getSnapshot();
    expect(r.state).toBe('local');
    expect(ids(r.items)).toEqual(['a']);
    expect(r.items[0]?.stale).toBe(true);
    expect(r.errors.api).toBeInstanceOf(Error);
  });

  it('T3: pre-session local-only rows are quarantined by a fresh snapshot; in-session arrivals never are', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('old', '2026-09-01')], { silent: true }); // pre-session
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    // download starts BEFORE the snapshot is fetched, completes AFTER: its stamp is older than
    // the snapshot, so the snapshot stays fresher (no ownership) while 'fresh' arrives in-session
    h.driver.startDownload();
    await h.clock.advance(5);
    await api.resolve([]); // fresh snapshot: neither row present
    await h.clock.advance(5);
    await h.driver.completeCheckpoint(() => {
      h.driver.table('sales').set('fresh', sale('fresh', '2026-09-02'));
    }, { keepDownloading: true });
    const r = store.store.getSnapshot();
    expect(r.state).toBe('api');
    expect(ids(r.items)).toEqual(['fresh']); // 'old' quarantined, 'fresh' kept
  });
});

describe('raced list — pending-write shields (R1/R2)', () => {
  async function warmWithApi(h: Harness) {
    await h.driver.synced('sales', [sale('a', '2026-09-03', { total: 1 })], { silent: true });
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    return { api, store };
  }

  it('T1/T16: a pending DELETE never resurrects from the snapshot, even after its upload completes mid-flight', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    const { api, store } = await warmWithApi(h);
    await h.driver.writeLocal('sales', 'DELETE', sale('a', '2026-09-03'));
    h.driver.completeUploads(); // upload completes while the API leg is still in flight
    await wait();
    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03', { total: 1 }))]); // lagging server still has it
    expect(ids(store.store.getSnapshot().items)).toEqual([]);
  });

  it('T15/T22: an optimistic UPDATE wins over a fresher snapshot, before and after consume', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    const { api, store } = await warmWithApi(h);
    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03', { total: 1 }))]); // consumed
    await h.driver.writeLocal('sales', 'UPDATE', sale('a', '2026-09-03', { total: 42 })); // AFTER consume
    expect(store.store.getSnapshot().items[0]?.row.total).toBe(42);
    h.driver.completeUploads();
    await wait();
    expect(store.store.getSnapshot().items[0]?.row.total).toBe(42); // recentlyAcked keeps shielding
  });

  it('T31: an op pending BEFORE the request that completes mid-flight is still shielded (seeded window)', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03', { total: 1 })], { silent: true });
    await h.driver.writeLocal('sales', 'UPDATE', sale('a', '2026-09-03', { total: 7 })); // hour-old offline edit
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    h.driver.completeUploads(); // reconnect flush, API leg still in flight
    await wait();
    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03', { total: 1 }))]);
    expect(store.store.getSnapshot().items[0]?.row.total).toBe(7);
  });

  it('T38: recentlyAcked retires only on a checkpoint whose STAMP ≥ ack time, not on completion time', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    const { api, store } = await warmWithApi(h);
    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03', { total: 1 }))]);
    h.driver.startDownload(); // download n starts BEFORE the ack
    await h.clock.advance(10);
    await h.driver.writeLocal('sales', 'UPDATE', sale('a', '2026-09-03', { total: 42 }));
    h.driver.completeUploads(); // ack
    await wait();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => {}); // completes AFTER the ack, data from BEFORE it
    expect(store.store.getSnapshot().items[0]?.row.total).toBe(42); // still shielded
  });
});

describe('raced list — ownership & windows (R6/R7)', () => {
  it('T12/T32: at ownership api-only rows never seen locally are dropped; list truncated to limit', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03'), sale('b', '2026-09-02')], { silent: true });
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03')), apiRow(sale('zzz', '2026-09-04')), apiRow(sale('b', '2026-09-02'))]);
    expect(ids(store.store.getSnapshot().items)).toEqual(['zzz', 'a', 'b']);
    await h.clock.advance(10);
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => {
      const t = h.driver.table('sales');
      t.set('c', sale('c', '2026-09-01'));
      t.set('d', sale('d', '2026-08-30'));
    });
    const r = store.store.getSnapshot();
    expect(r.state).toBe('converged');
    expect(ids(r.items)).toEqual(['a', 'b', 'c']); // zzz dropped (never seen locally), truncated to 3
  });

  it('apiScopeSupersetOfLocal keeps never-seen api-only rows after ownership', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('a', '2026-09-03')], { silent: true });
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch, { apiScopeSupersetOfLocal: true }), { org: 'o' });
    store.retain();
    await wait();
    await h.clock.advance(10);
    await api.resolve([apiRow(sale('a', '2026-09-03')), apiRow(sale('zzz', '2026-09-04'))]);
    await h.clock.advance(10);
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint();
    expect(ids(store.store.getSnapshot().items)).toEqual(['zzz', 'a']);
    expect(store.store.getSnapshot().items[0]?.presentLocally).toBe(false);
  });

  it('T27: a row filtered out of the query (still present locally) is dropped without a tombstone', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    h.driver.connect();
    const api = apiLeg();
    const def = defineRacedList({
      id: 'open-sales',
      table: 'sales',
      query: (_scope: {}) => h.orm.query<Sale>(['sales'], (d) => (Array.from(d.tables.get('sales')?.values() ?? []) as Sale[]).filter((s) => s.status === 'OPEN')),
      fetchSnapshot: async (_s, signal) => ({ rows: await api.fetch(signal) }),
      mapApi: (o: Api): Sale => ({ id: o.id, soldAt: o.sold_at, total: o.total, status: o.status }),
      orderKey: 'id',
      limit: 10,
    });
    const store = h.bridge.raced(def, {});
    store.retain();
    await wait();
    await api.resolve([]);
    await h.clock.advance(10);
    h.driver.startDownload();
    await h.clock.advance(10);
    await h.driver.completeCheckpoint(() => h.driver.table('sales').set('a', sale('a', '2026-09-03')));
    expect(store.store.getSnapshot().state).toBe('converged');
    expect(ids(store.store.getSnapshot().items)).toEqual(['a']);
    await h.driver.synced('sales', [sale('a', '2026-09-03', { status: 'PAID' })]); // filtered out, still present
    expect(ids(store.store.getSnapshot().items)).toEqual([]);
    expect(h.driver.presentIdsCalls.at(-1)).toEqual({ table: 'sales', ids: ['a'] }); // one batched probe
    await h.driver.synced('sales', [sale('a', '2026-09-03', { status: 'OPEN' })]); // back in scope: no tombstone
    expect(ids(store.store.getSnapshot().items)).toEqual(['a']);
  });
});

describe('raced list — platform gate & measurement', () => {
  it('T39: native gate issues no API leg', async () => {
    const h = makeHarness({ platform: 'native' });
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    expect(api.inflight).toBe(0);
    expect(store.store.getSnapshot().awaiting.api).toBe(false);
  });

  it('measureRace reports the winner and rows without row contents', async () => {
    const h = makeHarness();
    h.driver.connect();
    const api = apiLeg();
    const store = h.bridge.raced(defineRecent(h, api.fetch), { org: 'o' });
    store.retain();
    await wait();
    await api.resolve([apiRow(sale('a', '2026-09-03'))]);
    expect(h.measurements.length).toBeGreaterThan(0);
    const m = h.measurements[0]!;
    expect(m.list).toBe('recent-sales');
    expect(['local', 'api']).toContain(m.winner);
    expect(Object.keys(m)).not.toContain('rows');
  });
});
