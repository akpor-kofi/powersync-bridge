import { describe, expect, it } from 'vitest';
import { makeHarness, wait } from './harness';

describe('useOnDemandStream store (§7 default path)', () => {
  it('T44: subscribes on retain, reports synced after first sync, unsubscribes on release (TTL keeps rows warm)', async () => {
    const h = makeHarness();
    h.driver.connect();
    const s = h.bridge.onDemand('sale_by_id', { sale_id: 'x' }, { ttl: 300, timeoutMs: 1000 });
    s.retain();
    await wait();
    expect(s.store.getSnapshot().status).toBe('syncing');
    await h.driver.resolveFirstSync({ name: 'sale_by_id', params: { sale_id: 'x' } });
    expect(s.store.getSnapshot().status).toBe('synced');
    s.release();
    const st = h.driver.streams.get('sale_by_id:{"sale_id":"x"}')!;
    expect(st.refs).toBe(0);
    expect(st.expiresAt).toBe(h.clock.now() + 300_000);
  });

  it('reports timeout while connected and offline while disconnected', async () => {
    const h = makeHarness();
    h.driver.connect();
    const s = h.bridge.onDemand('sale_by_id', { sale_id: 'y' }, { timeoutMs: 500 });
    s.retain();
    await wait();
    await h.clock.advance(600);
    expect(s.store.getSnapshot().status).toBe('timeout');

    const h2 = makeHarness();
    const s2 = h2.bridge.onDemand('sale_by_id', { sale_id: 'z' }, { timeoutMs: 500 });
    s2.retain();
    await wait();
    expect(s2.store.getSnapshot().status).toBe('offline');
  });
});
