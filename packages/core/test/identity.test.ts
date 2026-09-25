import { describe, expect, it } from 'vitest';
import { defineRacedList, identityKeyFromToken } from '../src/index';
import { fakeJwt } from '../src/testing';
import { makeHarness, sale, salesQuery, wait, type Sale, ids } from './harness';

type Api = { id: string; sold_at: string };

describe('identity (R15, [DX])', () => {
  it('T43: identity is derived from the connector token; refresh does not churn; org switch swaps the epoch', async () => {
    const h = makeHarness();
    let token = fakeJwt({ sub: 'u1', org_id: 'A', exp: 1, iat: 1, jti: 'x' });
    const connector = { fetchCredentials: async () => ({ token }), uploadData: async (_db: unknown) => {} };
    const wrapped = h.bridge.wrapConnector(connector);
    const epochs: number[] = [];
    h.bridge.epochStore.subscribe(() => epochs.push(h.bridge.epoch));

    await wrapped.fetchCredentials();
    expect(h.bridge.epoch).toBe(1);
    token = fakeJwt({ sub: 'u1', org_id: 'A', exp: 2, iat: 2, jti: 'y', sid: 'rotated' }); // refresh
    await wrapped.fetchCredentials();
    expect(h.bridge.epoch).toBe(1); // no churn
    token = fakeJwt({ sub: 'u1', org_id: 'B', exp: 3, iat: 3 }); // org switch, same user
    await wrapped.fetchCredentials();
    expect(h.bridge.epoch).toBe(2);
    expect(epochs).toEqual([1, 2]);
    // uploadData still reaches the app's connector
    await expect(wrapped.uploadData({})).resolves.toBeUndefined();
  });

  it('identityKeyFromToken is canonical and deny-listed', () => {
    const a = identityKeyFromToken(fakeJwt({ b: 1, a: [1, 2], exp: 9, session_id: 's1' }));
    const b = identityKeyFromToken(fakeJwt({ a: [1, 2], b: 1, exp: 10, session_id: 's2' }));
    expect(a).toBe(b);
    expect(a).not.toBe(identityKeyFromToken(fakeJwt({ a: [1, 2], b: 2 })));
  });

  it('T8/T30/T35: an identity swap is synchronous — the store renders empty at once, later results are discarded, no cross-tenant row', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    await h.driver.synced('sales', [sale('tenantA', '2026-09-03')], { silent: true });
    h.driver.connect();
    let resolveApi!: (rows: Api[]) => void;
    const def = defineRacedList({
      id: 'sales',
      table: 'sales',
      query: (_scope: {}) => salesQuery(h.orm, 5),
      fetchSnapshot: () => new Promise<{ rows: Api[] }>((r) => (resolveApi = (rows) => r({ rows }))),
      mapApi: (o: Api): Sale => ({ id: o.id, soldAt: o.sold_at, total: 0, status: 'OPEN' }),
      orderKey: 'id',
      limit: 5,
    });
    const store = h.bridge.raced(def, {});
    store.retain();
    await wait();
    expect(ids(store.store.getSnapshot().items)).toEqual(['tenantA']);

    let sawOldRowsAfterSwap = false;
    store.store.subscribe(() => {
      if (h.bridge.epoch >= 1 && ids(store.store.getSnapshot().items).includes('tenantA')) sawOldRowsAfterSwap = true;
    });
    h.bridge.setIdentity({ org: 'B' }); // synchronous swap
    expect(store.store.getSnapshot().state).toBe('pending');
    expect(store.store.getSnapshot().items).toHaveLength(0);
    expect(store.isDisposed).toBe(true);

    // late results for the outgoing tenant arrive after the swap
    resolveApi([{ id: 'tenantA', sold_at: '2026-09-03' }]);
    await h.driver.synced('sales', [sale('tenantA-2', '2026-09-04')]);
    await wait();
    expect(store.store.getSnapshot().items).toHaveLength(0);
    expect(sawOldRowsAfterSwap).toBe(false);

    // the new epoch acquires a fresh store
    const store2 = h.bridge.raced(def, {});
    expect(store2).not.toBe(store);
  });

  it('a clear (hasSynced true→false) is an epoch signal', async () => {
    const h = makeHarness({ warmSince: 900_000 });
    h.driver.connect();
    const before = h.bridge.epoch;
    await h.driver.clear();
    expect(h.bridge.epoch).toBe(before + 1);
  });
});
