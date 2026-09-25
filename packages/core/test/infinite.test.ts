import { describe, expect, it } from 'vitest';
import { defineInfiniteList } from '../src/index';
import type { PageArgs } from '../src/types';

type SaleCursor = [string, string] | undefined;
import { makeHarness, sale, salesQuery, wait, type Sale, ids } from './harness';

function monthOf(cursor: SaleCursor): string | null {
  return cursor ? cursor[0].slice(0, 7) : '2026-09';
}
function prevMonth(m: string): string {
  const [y, mo] = m.split('-').map(Number) as [number, number];
  const d = new Date(Date.UTC(y, mo - 2, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

function definePartitioned(h: ReturnType<typeof makeHarness>, opts: { floor?: string; serverFloor?: () => Promise<string | null>; cap?: number } = {}) {
  return defineInfiniteList({
    id: 'sales-history',
    table: 'sales',
    pageSize: 2,
    orderKey: ['soldAt', 'id'] as const,
    direction: 'desc',
    query: ({ cursor, limit }: PageArgs<SaleCursor>, _scope: {}) => salesQuery(h.orm, limit, cursor),
    escalate: {
      kind: 'partition',
      stream: 'sales_month',
      partitionOf: monthOf,
      nextPartition: prevMonth,
      partitionParams: (m) => ({ month: m }),
      floor: opts.floor ?? '2026-01',
      ...(opts.serverFloor ? { serverFloor: opts.serverFloor } : {}),
      partitionCap: opts.cap ?? 3,
      timeoutMs: 1000,
      ttl: 300,
    },
  });
}

describe('infinite list — partition escalation (§6.2, R16)', () => {
  it('T4/T6: an empty partition is not the end; rows landing out of order are never left behind', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('s1', '2026-09-20'), sale('s2', '2026-09-10')], { silent: true });
    h.driver.connect();
    const def = definePartitioned(h);
    const store = h.bridge.infinite(def, {});
    store.retain();
    await wait();
    expect(ids(store.store.getSnapshot().items)).toEqual(['s1', 's2']);

    const p = store.loadMore(); // no lookahead → escalate: subscribe 2026-09 (partition of the cursor)
    await wait();
    expect(h.driver.subscribeLog.at(-1)).toEqual({ name: 'sales_month', params: { month: '2026-09' } });
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-09' } }); // nothing new beyond cursor
    await wait();
    expect(h.driver.subscribeLog.at(-1)).toEqual({ name: 'sales_month', params: { month: '2026-08' } }); // empty ≠ end
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-08' } }, () => {
      h.driver.table('sales').set('a1', sale('a1', '2026-08-15'));
    });
    await p;
    await wait();
    const r = store.store.getSnapshot();
    expect(r.boundary).toBe('more');
    expect(ids(r.items)).toEqual(['s1', 's2', 'a1']);
  });

  it('T5: offline escalation yields unknown/offline, never end', async () => {
    const h = makeHarness();
    const store = h.bridge.infinite(definePartitioned(h), {});
    store.retain();
    await wait();
    await store.loadMore();
    expect(store.store.getSnapshot().boundary).toBe('unknown');
    expect(store.store.getSnapshot().boundaryReason).toBe('offline');
  });

  it('T5: a first-sync timeout yields unknown/timeout and unsubscribes', async () => {
    const h = makeHarness();
    h.driver.connect();
    const store = h.bridge.infinite(definePartitioned(h), {});
    store.retain();
    await wait();
    const p = store.loadMore();
    await wait();
    await h.clock.advance(1500);
    await p;
    const r = store.store.getSnapshot();
    expect(r.boundary).toBe('unknown');
    expect(r.boundaryReason).toBe('timeout');
  });

  it('T20/T29/T41: sparse data hits the partition cap, resumes from probeCursor, and reaches end at the floor without re-probing', async () => {
    const h = makeHarness();
    h.driver.connect();
    const store = h.bridge.infinite(definePartitioned(h, { floor: '2026-05', cap: 3 }), {});
    store.retain();
    await wait();

    const drain = async () => {
      // resolve whichever partition was just subscribed, empty
      await wait();
      const last = h.driver.subscribeLog.at(-1);
      if (last && !h.driver.streams.get(`${last.name}:${JSON.stringify(last.params)}`)?.hasSynced) await h.driver.resolveFirstSync(last);
    };

    let p = store.loadMore();
    for (let i = 0; i < 3; i++) await drain();
    await p;
    expect(store.store.getSnapshot().boundaryReason).toBe('partition-cap'); // 09, 08, 07 probed
    const probedSoFar = h.driver.subscribeLog.map((d) => d.params?.month);
    expect(probedSoFar).toEqual(['2026-09', '2026-08', '2026-07']);

    p = store.loadMore(); // resumes from nextPartition(probeCursor) = 06
    for (let i = 0; i < 3; i++) await drain();
    await p;
    const probed = h.driver.subscribeLog.map((d) => d.params?.month);
    expect(probed).toEqual(['2026-09', '2026-08', '2026-07', '2026-06', '2026-05']); // no re-probe of 07
    expect(store.store.getSnapshot().boundary).toBe('end'); // 2026-04 < floor
    const count = h.driver.subscribeLog.length;
    await store.loadMore();
    expect(h.driver.subscribeLog.length).toBe(count); // no subscription once 'end'
  });

  it('server floor is used when present and re-validated once on end', async () => {
    const h = makeHarness();
    h.driver.connect();
    let calls = 0;
    const store = h.bridge.infinite(definePartitioned(h, { floor: '2020-01', serverFloor: async () => (++calls, '2026-09') }), {});
    store.retain();
    await wait();
    const p = store.loadMore();
    await wait();
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-09' } });
    await p;
    expect(store.store.getSnapshot().boundary).toBe('end');
    expect(calls).toBe(2); // initial + one re-validation
  });

  it('T11: empty partitions are unsubscribed immediately; bucket pressure is governed by ttl', async () => {
    const h = makeHarness();
    h.driver.connect();
    const store = h.bridge.infinite(definePartitioned(h, { floor: '2026-07' }), {});
    store.retain();
    await wait();
    const p = store.loadMore();
    await wait();
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-09' } });
    await wait();
    const s = h.driver.streams.get('sales_month:{"month":"2026-09"}')!;
    expect(s.refs).toBe(0);
    expect(s.expiresAt).toBe(h.clock.now() + 300_000); // bridge ttl, not the 24h default
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-08' } });
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-07' } });
    await p;
  });
});

describe('infinite list — removal taxonomy (§6.4)', () => {
  it('T7/T28: a delete before expiry drops the row; a disappearance after expiry is an eviction retained as not present', async () => {
    const h = makeHarness();
    h.driver.connect();
    const evicted: string[][] = [];
    const { createBridge } = await import('../src/index');
    const { FakeAdapter } = await import('../src/testing');
    const bridge = createBridge({ driver: h.driver, adapter: new FakeAdapter(h.driver), clock: h.clock, platform: 'web', onEvicted: (_l, ids) => evicted.push([...ids]) });
    const store = bridge.infinite(definePartitioned(h), {});
    store.retain();
    await wait();
    const p = store.loadMore();
    await wait();
    const aug = { name: 'sales_month', params: { month: '2026-08' } };
    await h.driver.resolveFirstSync({ name: 'sales_month', params: { month: '2026-09' } });
    await wait();
    await h.driver.resolveFirstSync(aug, () => {
      h.driver.table('sales').set('a1', sale('a1', '2026-08-15'));
      h.driver.table('sales').set('a2', sale('a2', '2026-08-10'));
    });
    await p;
    expect(ids(store.store.getSnapshot().items)).toEqual(['a1', 'a2']);

    // server DELETE while the subscription is still tracked (active or TTL-pending) → DELETE
    await h.driver.synced('sales', [sale('a2', '2026-08-10')], { delete: true });
    expect(ids(store.store.getSnapshot().items)).toEqual(['a1']);
    expect(evicted).toEqual([]);

    // unsubscribe (release) → TTL pending; a delete in the TTL window is STILL a delete (F3-005)
    store.release();
    const store2 = bridge.infinite(definePartitioned(h), {});
    store2.retain();
    await wait();
    // Simulate expiry: ttl elapses, rows removed by the SDK
    await h.clock.advance(400_000);
    await h.driver.expireStreams((_t, row) => ({ name: 'sales_month', params: { month: String(row.soldAt).slice(0, 7) } }));
    // store2 loaded a1 via its own initial window (partition still there when it mounted) — verify it retained a1 as evicted
    const r = store2.store.getSnapshot();
    const a1 = r.items.find((i) => i.row.id === 'a1');
    expect(a1?.presentLocally).toBe(false);
    expect(evicted.at(-1)).toEqual(['a1']);
  });
});

describe('infinite list — API escalation', () => {
  it('api pages: appended as api-only, deduped by id, end on an empty page, backoff on failure', async () => {
    const h = makeHarness();
    h.driver.connect();
    await h.driver.synced('sales', [sale('s1', '2026-09-20')], { silent: true });
    let fail = false;
    const pages: Sale[][] = [[sale('s1', '2026-09-20'), sale('p1', '2026-08-01')], []];
    const def = defineInfiniteList({
      id: 'api-history',
      table: 'sales',
      pageSize: 2,
      orderKey: ['soldAt', 'id'] as const,
      direction: 'desc',
      query: ({ cursor, limit }: PageArgs<SaleCursor>, _s: {}) => salesQuery(h.orm, limit, cursor),
      escalate: { kind: 'api', fetchPage: async () => { if (fail) throw new Error('500'); return pages.shift() ?? []; } },
    });
    const store = h.bridge.infinite(def, {});
    store.retain();
    await wait();
    fail = true;
    await store.loadMore();
    expect(store.store.getSnapshot().boundary).toBe('unknown');
    expect(store.store.getSnapshot().boundaryReason).toBe('error');
    fail = false;
    await store.loadMore(); // backoff latch
    expect(store.store.getSnapshot().boundaryReason).toBe('offline');
    await store.retry(); // T37: explicit retry clears backoff
    expect(ids(store.store.getSnapshot().items)).toEqual(['s1', 'p1']); // s1 deduped (local wins)
    expect(store.store.getSnapshot().items[1]?.origin).toBe('api');
    await store.loadMore();
    expect(store.store.getSnapshot().boundary).toBe('end');
  });

  it('T42: api-offset mode drops duplicates from a shifted offset and flags the shift', async () => {
    const h = makeHarness();
    h.driver.connect();
    await h.driver.synced('sales', [sale('s1', '2026-09-20')], { silent: true });
    const server = [sale('s1', '2026-09-20'), sale('p1', '2026-08-01'), sale('p2', '2026-07-01')];
    const def = defineInfiniteList({
      id: 'offset-history',
      table: 'sales',
      pageSize: 2,
      orderKey: ['soldAt', 'id'] as const,
      direction: 'desc',
      query: ({ cursor, limit }: PageArgs<SaleCursor>, _s: {}) => salesQuery(h.orm, limit, cursor),
      escalate: { kind: 'api-offset', fetchPage: async (offset) => server.slice(offset, offset + 2) },
    });
    const store = h.bridge.infinite(def, {});
    store.retain();
    await wait();
    await store.loadMore(); // offset 1 → [p1, p2]
    expect(ids(store.store.getSnapshot().items)).toEqual(['s1', 'p1', 'p2']);
    server.unshift(sale('new', '2026-09-25')); // a live insert shifts everything down
    server.push(sale('p3', '2026-06-01'));
    await store.loadMore(); // offset 3 → [p2, p3]: p2 is a duplicate
    const r = store.store.getSnapshot();
    expect(ids(r.items)).toEqual(['s1', 'p1', 'p2', 'p3']);
    expect(r.warnings).toContain('offset-shift');
  });
});
