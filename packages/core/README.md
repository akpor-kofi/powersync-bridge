# powersync-bridge

Local SQLite and your HTTP API, behind one list interface for React and React Native.

The application defines its SQL, API calls, filters and permissions. The bridge owns racing,
pagination, pending-write protection, reconciliation, subscriptions, connectivity, retries and
bounded memory. API results stay in memory; only PowerSync makes records durable offline.

## Start with `useInfiniteQuery`

```tsx
import { useInfiniteQuery } from '@powersync-bridge/react';

type Transaction = { id: string; date: string; amount: string };

const transactions = useInfiniteQuery<Transaction>({
  queryKey: ['transactions', organizationId, filters, permissionVersion],
  orderBy: ['date', 'id'],
  // Descending order, 25 rows per page and a 500-row memory window are defaults.
  query: ({ cursor, limit }) =>
    db.select().from(transaction)
      .where(and(
        eq(transaction.organizationId, organizationId),
        cursor ? or(
          lt(transaction.date, cursor.date),
          and(eq(transaction.date, cursor.date), lt(transaction.id, cursor.id)),
        ) : undefined,
      ))
      .orderBy(desc(transaction.date), desc(transaction.id))
      .limit(limit),
  fetchPage: async ({ cursor, limit, signal }) =>
    api.transactions.list({ organizationId, filters,
      cursor: cursor ? { date: cursor.date, id: cursor.id } : undefined,
      limit, signal,
    }),
});

// FlatList / virtualized list:
// data={transactions.data}
// onEndReached={() => transactions.hasNextPage && transactions.fetchNextPage()}
// refreshing={transactions.isFetching}
// onRefresh={transactions.refresh}
```

The local and API rows must have the same decoded shape and a strict, stable sort order.
Honor the requested `limit`: the bridge requests one extra row to detect another page.
`cursor` is the previous API row, never a cursor guessed from a sparse local feed. Local reads
can fill a requested page while its API request is still pending. Every requested page races;
API requests follow a single cursor chain, including when scrolling outpaces the network.

`data` contains ordinary rows. `items` additionally exposes each row's source and local presence
for edit guards. `error` reports an error preventing the requested range from being filled;
`errors` retains independent local/API diagnostics even when usable rows are displayed.
`retry()` retries the pending range. `refresh()` / `refetch()` starts again at the current window.
At `maxRows`, `needsOlderWindow` becomes true when more history may exist; `continueOlder()`
opens the next bounded window. Changing `queryKey` discards the previous scope.

Include every organization, filter and permission predicate in `queryKey`; the bridge does not
invent business authorization. It infers the business table for single-table queries. Supply `table`
for joins or ambiguous queries, or `getRowIdentity(row) => ({ table, id })` for a heterogeneous feed whose display
IDs differ from the underlying business IDs.

`mapQuery(builder, decode)` preserves ORM metadata when decoding rows. `rawQuery(db,
{ text, params }, tables, decode)` supports parameterized SQL feeds with explicit source tables.
Both come from `@powersync-bridge/drizzle`; no application adapter or custom watch is needed.

## Connectivity

Browser connectivity is observed automatically. In React Native, pass the existing network
hook's boolean once at the provider:

```tsx
<BridgeProvider bridge={bridge} online={online}>
  <App />
</BridgeProvider>
```

`online` is optional and may be `undefined`. The bridge also handles HTTP failures when a
connectivity hint incorrectly says online. A disconnected sync socket does not by itself mean
the API is unavailable. Downloaded rows continue to paginate offline; additional archive rows
require a connection. Reconnection resumes the pending API range without clearing visible rows.

The lower-level `useRacedList`, `useInfiniteList`, `useOnDemandStream` and `useAutoSync` APIs
remain available for specialized workflows. Ordinary paginated lists need only `useInfiniteQuery`.

## Hidden screens and checkpoint bursts

Wrap a retained screen or tab in `BridgeActivity` and pass its existing visibility state:

```tsx
import { BridgeActivity } from '@powersync-bridge/react';

<BridgeActivity active={isVisible}>
  <ServicesScreen />
</BridgeActivity>
```

This applies to `useInfiniteQuery` and `useRacedList`. Pausing stops their SQLite watches,
checkpoint reads and pending API requests while keeping the last result and loaded page window.
Returning refreshes the local baseline, including rows changed or deleted while hidden.
An active consumer elsewhere keeps a shared query running. Nested boundaries cannot reactivate
an inactive parent. Identity, permissions and query-key changes still clear the outgoing scope.
Use `enabled` for query eligibility and `BridgeActivity` for temporary screen visibility.
On-demand sync streams and uploads continue independently.

Checkpoint refreshes are single-flight per observer. A synchronous burst produces one read;
changes during that read produce one follow-up, and superseded results cannot claim freshness.
Checkpoint maintenance shares one background-read slot across the bridge; first reads and
pagination bypass that queue. Queued maintenance is cancelled when its observer pauses.
Unscoped queries refresh only for global checkpoints. A query declaring `stream` also refreshes
for its own stream, rather than every unrelated stream. Closing a watch cancels its queued
follow-up. Applications need no checkpoint debounce, refresh queue or cache workaround.

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

Peers: `@powersync/common ^1.57.3 || ^2.3`, with your compatible web or native SDK,
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
