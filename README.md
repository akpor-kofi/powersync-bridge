# powersync-bridge

A client-side bridge between a [PowerSync](https://powersync.com) local SQLite database and a
conventional HTTP API, for web and React Native.

The right data source, local synced SQLite or the live API, is a runtime, per-screen decision.
This package makes it automatic:

- **`useRacedList`** — paint the first screen from whichever source is *correct* first. A
  first-ever visit has an empty local table; a warm re-mount has yesterday's checkpoint. The
  bridge fetches one API snapshot, merges by id with pending-write and freshness awareness,
  and hands the list to local sync as soon as local is fresh in this session.
- **`useInfiniteList`** — scroll past the synced window. Older data arrives either through
  PowerSync sync streams (one equality partition at a time, bounded) or through your API's
  pages. Row removals are classified as filtered-out, delete, or eviction, so the list never
  resurrects deletes and never loses content mid-scroll.
- **`useOnDemandStream`** — pull one record into the offline database by id.

Status: **v0 alpha**. Core engine and invariant tests are complete (35 tests on a virtual-time
fake driver); the PowerSync driver is written against `@powersync/common` 2.3 and needs a running
integration before GA. Sizes (minified + gzip): core 8.2 kB, React 0.6 kB, driver 1.8 kB.

## What your backend must change

**Nothing.** No new tables, no new columns, no change to existing sync streams. The one rule is an
alignment, not a change: your list endpoint sorts rows the same way your local query does, with
`id` as the last tiebreaker.

| What you want | Streams you need | Routes you need |
|---|---|---|
| Fast, correct first paint of any list | none beyond what you have | your existing list endpoint |
| Scroll past the synced window, older pages from the API | none | the same endpoint accepting `cursor` (or `offset`, degraded) |
| Scroll past the synced window, older months into the offline DB | one stream with `subscription.parameter('month')`, partition computed with `substring()` on your existing timestamp | optional `GET …/floor` |
| Open one old record by id, offline-capable | one stream with `id = subscription.parameter('id')` | none |

## Install

```bash
pnpm add powersync-bridge @powersync-bridge/react @powersync-bridge/drizzle @powersync-bridge/powersync
```

Peers: `@powersync/common ^2.3`, `@powersync/web ^2.4` or `@powersync/react-native ^2.3`,
`@powersync/drizzle-driver`, `drizzle-orm`, `react ^18.2 || ^19`. TanStack Query is optional.

## Setup (once, in a plain module)

```ts
// bridge.ts
import { createBridge } from 'powersync-bridge';
import { createPowerSyncDriver } from '@powersync-bridge/powersync';
import { drizzleAdapter } from '@powersync-bridge/drizzle';
import { db, drizzle } from './db/powersync';

const adapter = drizzleAdapter(drizzle);
export const bridge = createBridge({
  driver: createPowerSyncDriver(db, { adapter, onlineHint: () => navigator.onLine }),
  adapter,
  platformGates: { native: 'local-first', web: 'race' },
});
```

```ts
// db/boot.ts — connect THROUGH the bridge: identity is derived from the JWT your connector
// already returns. Never pass claims yourself.
await db.init();
await db.connect(bridge.wrapConnector(connector));
```

```tsx
// App.tsx
<BridgeProvider bridge={bridge}>
  <Router />
</BridgeProvider>
```

## A raced list (page 1, with joins)

```ts
import { z } from 'zod';
import { desc, eq } from 'drizzle-orm';
import { defineRacedList } from 'powersync-bridge';
import { drizzle } from '../db/powersync';
import { sales, customers, users } from '../db/schema';

const SaleApi = z.object({
  id: z.string(), sold_at: z.string(), total: z.string(),
  status: z.enum(['OPEN', 'PAID', 'VOID']), item_count: z.number().int(),
  customer_name: z.string(), seller_name: z.string(),
});
type SaleApi = z.infer<typeof SaleApi>;

export const RecentSales = defineRacedList({
  id: 'recent-sales',
  table: 'sales',
  // The row type is inferred from this query. Annotate the scope; close over your drizzle instance.
  query: ({ orgId }: { orgId: string }) =>
    drizzle
      .select({
        id: sales.id, soldAt: sales.soldAt, total: sales.total, status: sales.status,
        itemCount: sales.itemCount, customerName: customers.name, sellerName: users.displayName,
      })
      .from(sales)
      .innerJoin(customers, eq(customers.id, sales.customerId))
      .innerJoin(users, eq(users.id, sales.sellerId))
      .where(eq(sales.orgId, orgId))
      .orderBy(desc(sales.soldAt), desc(sales.id))
      .limit(50),
  fetchSnapshot: async (_scope, signal) => {
    const res = await api.get('/api/sales?limit=50', { signal });
    return { rows: z.array(SaleApi).parse(res.items) };
  },
  mapApi: (o: SaleApi) => ({            // return type is checked against the query's row type
    id: o.id, soldAt: o.sold_at, total: o.total, status: o.status,
    itemCount: o.item_count, customerName: o.customer_name, sellerName: o.seller_name,
  }),
  orderKey: ['soldAt', 'id'] as const,  // `id` is always the implicit tiebreaker
  direction: 'desc',
  limit: 50,
  stream: { name: 'sales_window', params: null },
});
```

```tsx
function RecentSalesScreen({ orgId }: { orgId: string }) {
  const { items, state, errors } = useRacedList(RecentSales, { orgId });
  if (state === 'pending') return <Skeleton />;
  return (
    <FlatList
      data={items}
      keyExtractor={(r) => r.row.id}
      renderItem={({ item }) => (
        <SaleRow
          sale={item.row}
          muted={item.stale === true || !item.presentLocally}
          onVoid={item.presentLocally ? () => voidSale(item.row.id) : undefined}   // presence-gated writes
        />
      )}
    />
  );
}
```

## An infinite list (older months via sync streams)

```ts
import { defineInfiniteList, type Cursor, type PageArgs } from 'powersync-bridge';

type SaleCursor = Cursor<SaleRow, ['soldAt', 'id']>;   // [string, string] | undefined

export const SalesHistory = defineInfiniteList({
  id: 'sales-history',
  table: 'sales',
  pageSize: 50,
  orderKey: ['soldAt', 'id'] as const,
  direction: 'desc',
  // Annotate the page argument with the Cursor type; the row type is inferred from the return.
  query: ({ cursor, limit }: PageArgs<SaleCursor>, { orgId }: { orgId: string }) =>
    drizzle.select(projection).from(sales) /* …joins… */
      .where(and(eq(sales.orgId, orgId), cursor ? or(lt(sales.soldAt, cursor[0]), and(eq(sales.soldAt, cursor[0]), lt(sales.id, cursor[1]))) : undefined))
      .orderBy(desc(sales.soldAt), desc(sales.id))
      .limit(limit),
  escalate: {
    kind: 'partition',
    stream: 'sales_month',
    partitionOf: (c) => (c ? c[0].slice(0, 7) : currentMonth()),
    nextPartition: prevMonth,
    partitionParams: (m) => ({ month: m }),
    floor: '2015-01',                                              // static floor, always required
    serverFloor: async () => (await api.get('/api/sales/floor')).month,   // optional exact floor
  },
});
```

```yaml
# sync_streams.yaml — no partition column; the month is computed from your existing timestamp
sales_month:
  query: >
    SELECT * FROM sales
    WHERE org_id = auth.parameter('org_id')
      AND substring(sold_at::text, 1, 7) = subscription.parameter('month')
```

Or escalate through your API instead (`kind: 'api'` with a cursor, or `kind: 'api-offset'` for
offset-only APIs, where duplicates are dropped and a shift is reported in `warnings`).

## Rules that make it safe

- **Pending writes always win** — the live upload queue, a seeded snapshot-lifetime window, and
  recently-acked writes shield your edits from an older API snapshot.
- **Freshness is session-scoped** — PowerSync's persisted `hasSynced` never counts as "fresh";
  a checkpoint is stamped at its download start and the stamp is monotonic.
- **Deletes never resurrect** — a pending delete tombstones its id for the snapshot's lifetime.
- **Removals are classified** — a batched presence probe distinguishes filtered-out from
  deleted; stream expiry distinguishes deleted from evicted; evicted rows stay visible but
  are marked `presentLocally: false` and cannot be written.
- **Identity is derived** — from the connector's JWT, deny-listing volatile claims; an org
  switch swaps every store's root synchronously, so no frame renders the outgoing tenant.

Sorting constraints: the sort column must compare identically on Postgres and SQLite.
ISO-8601 UTC strings, integers and uuidv7 are safe. For text use `ORDER BY col COLLATE "C"` on
the API; for decimals declare the column as `column.real` in the client schema.

## Testing your own lists

`powersync-bridge/testing` ships a `FakeDriver` with virtual time: script sync status,
checkpoints, uploads, stream first-syncs and API responses, then assert on interleavings. See
`packages/core/test` for the invariant suite (T1–T44 in ARCHITECTURE.md §12.2).

## Layout

```
packages/core              powersync-bridge            engine, definitions, testing driver
packages/react             @powersync-bridge/react     BridgeProvider + hooks
packages/adapters/drizzle  @powersync-bridge/drizzle   builder → PreparedQuery (decoded rows)
packages/powersync         @powersync-bridge/powersync BridgeDriver over @powersync/common
```

Design: `ARCHITECTURE.md`. Build brief and worked example: `BUILD-READINESS.md`.
