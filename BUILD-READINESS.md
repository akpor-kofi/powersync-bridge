# powersync-bridge — Build Readiness & Usage Walkthrough

Written 2026-09-25 after the review loop closed (ARCHITECTURE.md v6). Two parts:
Part A judges whether ARCHITECTURE.md is a good prompt for a fresh build session and lists what
must be added. Part B is end-to-end pseudocode for a typical adopter: a **sales list with joins**
on a Go backend + React web/RN frontend.

---

## Part A — Is ARCHITECTURE.md a good build prompt?

### Verdict

**As a specification: yes, unusually good.** As a *task prompt on its own*: **not yet.** It needs a
one-to-two page build brief in front of it that (1) pins the API surfaces the document still
leaves implicit, (2) fixes scaffolding decisions, and (3) tells the builder how to read the
document (rules and tests are the contract; review history is not).

### What is strong (a builder can rely on it)

- **Normative rules R1–R17 and tests T1–T41** are effectively acceptance criteria and a test plan.
  A builder can implement against the tests; the interleavings in the review files double as
  test fixtures.
- **Driver interface (§9)** is concrete enough to implement the PowerSync adapter and a fake
  driver for tests.
- **Type contracts (§8)** are precise, including the tuple-cursor conditional type.
- **Security invariants (§10)** are stated as mechanisms, not slogans.
- **Phasing (§12.3)**: v0 alpha = core + raced + infinite + probe; v0.1 = warm-tab probe +
  kysely; v1 = React GA + example app. Tell the builder to stop at v0 first.
- **Grounding**: SDK facts (persisted `hasSynced`, `getCrudBatch` limit, stream TTL refcount,
  `waitForFirstSync(abort?)`) are cited and were verified against `powersync-js@main`.

### Gaps a fresh session would guess wrong (pin these in the build brief)

> **Update (v7):** gaps 1–5, 11 and the §7/offset/floor relaxations are now folded into
> ARCHITECTURE.md (marked [DX]). The list below is kept as the record of what changed.

1. **No top-level bridge instance.** Nothing says how the engine receives the driver, the TanStack
   `QueryClient`, `platformGates`, identity, and the crud-delta subscription that must exist
   *before any list mounts* (F6-001). Define `createBridge({ driver, queryClient, platformGates })`
   and a `<BridgeProvider bridge={…}>`.
2. **The API leg's fetch function is missing from `RacedListDefinition`.** §5.1 has `mapApi` but no
   `fetchSnapshot(scope, signal)`. Same for `staleTime`. Add both.
3. **Two conflicting `query` signatures.** §5.1 says `query: ({ scope }) => PreparedQuery`, §8.1
   shows `query: (db) => db.select()…`. Pick one: `query: (db, scope) => DrizzleSelect`, and the
   Drizzle adapter turns it into a `PreparedQuery`.
4. **`PartitionPolicy` is incomplete.** §6.2 uses `partitionParams(p)`, a stream name, and
   `serverFloor(p)`; §6.1 lists only `partitionOf/nextPartition/floor`. Add
   `stream: string`, `partitionParams(p): Params`, `serverFloor?: () => Promise<TPartition>`.
5. **Hook signatures for `useInfiniteList` and `useAutoSync` are not written down** (return shape:
   `items, loadMore, boundary, boundaryReason, awaitingSync, errors`).
6. **Scaffolding is unspecified**: package manager, monorepo tool, bundler, test runner, lint,
   TS strictness. A builder will pick something; specify (recommendation below).
7. **Exact dependency versions** appear only in evidence paragraphs. Pin peers:
   `@powersync/common ^2.3`, `@powersync/web ^2.4`, `@powersync/react-native ^2.3`,
   `@powersync/drizzle-driver` (current), `drizzle-orm` (current), `@tanstack/query-core ^5`,
   `react ^18.2 || ^19`, `zod` optional.
8. **SDK internals the driver touches** must be verified against the *published* types at build
   time, not `main`: `registerListener({ statusChanged })`, `status.dataFlowStatus.downloading`,
   `db.onChange({ tables: ['ps_crud'], rawTableNames: true })`,
   `db.query(toCompilableQuery(q)).differentialWatch()`, `await db.syncStream(name, params)
   .subscribe({ ttl, priority })`. Tell the builder to read `node_modules/@powersync/common`
   types and adapt, and to fail loudly at init if `ps_crud` shape differs (§14.4).
9. **Test harness is implied, not specified.** T1–T41 need a `FakeBridgeDriver` with virtual time:
   scriptable sync status, checkpoint completions with stamps, crud deltas, stream first-sync
   resolution, and a scriptable API leg. T21 needs Playwright; T13 needs drizzle-zod; T14 needs a
   YAML parser. Say so.
10. **Example backend contract is prose.** Write the four endpoints as an OpenAPI stub
    (Part B gives them) and include a sample `sync_streams.yaml`.
11. **Identity source.** The app must never pass claims into `createBridge` (a plain module,
    pre-login, not a hook). The driver derives identity from the JWT the connector already
    returns: `db.connect(bridge.wrapConnector(connector))`; key = canonical JSON of the
    deny-listed claims (no hashing); `bridge.setIdentity()` is the rare escape hatch.
    §9/R15 must be reworded accordingly.
12. **Document weight.** Six rounds of `[R2,F2-001]` tags and §2/§3/§15–§17 history are context,
    not instructions. The brief should say: "read §5–§12 as the spec, R1–R17 as acceptance,
    T1–T41 as the test plan; skip §2–§3, §13–§17."

### Recommended build brief (paste above ARCHITECTURE.md for the next session)

```
TASK: Implement powersync-bridge v0 alpha per ARCHITECTURE.md (§5–§12).
Definition of done: packages/core, packages/react, packages/adapters/drizzle build;
T1–T20, T22–T41 pass on a FakeBridgeDriver with virtual time (T21 is Playwright, stub it);
static assertions for R6/R11; bundle budgets (§11) checked with size-limit.

READ: §5–§12 are the spec. §5.3 rules are acceptance criteria. §12.2 tests are the plan.
Skip §2–§3, §13–§17 (history). Review files are optional fixtures for interleavings.

DECISIONS (do not re-open):
- pnpm workspaces + changesets; tsup (esm+cjs, d.ts); vitest; biome; tsconfig strict +
  exactOptionalPropertyTypes + noUncheckedIndexedAccess.
- Peers: @powersync/common ^2.3, @powersync/web ^2.4, @powersync/react-native ^2.3,
  @powersync/drizzle-driver current, drizzle-orm current, @tanstack/query-core ^5,
  react ^18.2||^19, zod optional.
- Public API additions (fill the gaps in §5.1/§6.1): createBridge(), BridgeProvider,
  RacedListDefinition.fetchSnapshot(scope, signal) + staleTime,
  query(db, scope) signature, PartitionPolicy.{stream, partitionParams, serverFloor},
  useInfiniteList / useAutoSync return shapes as in BUILD-READINESS.md Part B.
- identityKey is DERIVED by the driver from the connector's JWT via bridge.wrapConnector();
  key = canonical JSON of deny-listed claims ⊕ opted-in connection params; setIdentity()
  is the escape hatch. Never ask the app for claims.
- §7 autoSyncFlag is OPTIONAL (bucket economy / server-decided membership only); ship
  useOnDemandStream() as the default "pull one row" primitive.
- Verify SDK member names against installed @powersync/common types; adapt, do not assume.

ORDER: core engine (store, epochs, freshness, shields, reconcile) → fake driver + T-tests →
drizzle adapter → PowerSync driver → react hooks → example app skeleton.
```

---

## Part B — Usage walkthrough (v2): a sales list with joins

Revised after DX review: (1) identity is derived by the driver from the connector's own JWT —
the app never passes claims; (2) the sales example needs no `sync_requests` table, no flag
endpoint and no `autoSyncFlag` — "open one old sale" is an on-demand equality-parameter
stream. `autoSyncFlag` (§7) is an optional primitive for bucket economy or server-decided
membership, not part of a typical setup.

Scenario: multi-tenant POS. `sales` (large, month-partitioned), `customers` and `users`
(small, always synced), `sale_items` (detail, on demand). Screen: "Recent sales" showing sale,
customer name, seller name, item count. Go + Postgres + PowerSync Service; React web + RN.

Surfaces marked `PROPOSED` are the gaps from Part A, written the way the brief should pin them.

### B.1 Backend — Postgres

```sql
CREATE TABLE customers (id uuid PRIMARY KEY, org_id uuid NOT NULL, name text NOT NULL);
CREATE TABLE users     (id uuid PRIMARY KEY, org_id uuid NOT NULL, display_name text NOT NULL);

CREATE TABLE sales (
  id             uuid PRIMARY KEY,          -- uuidv7: monotonic + unique → valid orderKey
  org_id         uuid NOT NULL,
  customer_id    uuid NOT NULL REFERENCES customers(id),
  seller_id      uuid NOT NULL REFERENCES users(id),
  sold_at        timestamptz NOT NULL,
  total          numeric(12,2) NOT NULL,
  status         text NOT NULL,             -- 'OPEN' | 'PAID' | 'VOID'
  item_count     int NOT NULL DEFAULT 0     -- OPTIONAL optimization; a count subquery also works
);
CREATE INDEX ON sales (org_id, sold_at DESC, id DESC);
-- No partition column: the month is computed inside the sync stream (B.2) and on the client.

CREATE TABLE sale_items (
  id      uuid PRIMARY KEY,
  sale_id uuid NOT NULL REFERENCES sales(id),
  sku     text, qty int, price numeric(12,2)
);
```

### B.2 Backend — PowerSync `sync_streams.yaml`

```yaml
streams:
  org_customers:                         # small reference table: always on, first
    auto_subscribe: true
    priority: 1
    query: SELECT * FROM customers WHERE org_id = auth.parameter('org_id')

  org_users:
    auto_subscribe: true
    priority: 1
    query: SELECT * FROM users WHERE org_id = auth.parameter('org_id')

  sales_window:                          # the synced window: months granted by a signed claim
    auto_subscribe: true
    priority: 2
    query: >
      SELECT * FROM sales
      WHERE org_id = auth.parameter('org_id')
        AND substring(sold_at::text, 1, 7) IN auth.parameter('sync_months')

  sales_month:                           # escalation partition: client picks the month,
    query: >                             # auth still gates the org (selection ≠ authorization)
      SELECT * FROM sales
      WHERE org_id = auth.parameter('org_id')
        AND substring(sold_at::text, 1, 7) = subscription.parameter('month')   # no column needed

  sale_by_id:                            # on demand: one arbitrary old sale (replaces §7 here)
    query: >
      SELECT * FROM sales
      WHERE org_id = auth.parameter('org_id')
        AND id = subscription.parameter('sale_id')

  sale_detail:                           # on demand: line items for one sale
    query: >
      SELECT si.* FROM sale_items si
      JOIN sales s ON s.id = si.sale_id
      WHERE s.org_id = auth.parameter('org_id')
        AND si.sale_id = subscription.parameter('sale_id')
```

JWT claims minted by the auth layer:

```json
{ "sub": "user-123", "org_id": "org-9", "sync_months": ["2026-09","2026-08","2026-07","2026-06"] }
```

### B.3 Backend — Go API

```go
// cursor.go — opaque tuple cursor; same orderKey as the local query: (sold_at DESC, id DESC)
type SalesCursor struct {
	SoldAt time.Time `json:"s"`
	ID     string    `json:"i"`
}

func decodeCursor(raw string) (*SalesCursor, error) {
	if raw == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var c SalesCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(c.ID); err != nil { // validate shape + operand types before SQL (§12.1)
		return nil, err
	}
	return &c, nil
}

func encodeCursor(c SalesCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}
```

```go
// sales_handler.go
type SaleRow struct {
	ID           string `json:"id"`
	SoldAt       string `json:"sold_at"`       // ISO-8601 UTC
	Total        string `json:"total"`         // numeric as string — matches Drizzle
	Status       string `json:"status"`
	ItemCount    int    `json:"item_count"`
	CustomerName string `json:"customer_name"`
	SellerName   string `json:"seller_name"`
}

type SalesPage struct {
	Items      []SaleRow `json:"items"`
	NextCursor string    `json:"nextCursor,omitempty"`
	AsOf       string    `json:"asOf,omitempty"` // optional PowerSync write checkpoint (§10.2)
}

// GET /api/sales?cursor=<opaque>&limit=50
// Serves BOTH the raced list's page-1 snapshot and ApiPolicy pages.
// Reads from the PRIMARY (read-your-writes, §10.2). Same tenant scope as the streams.
func (h *Handler) ListSales(w http.ResponseWriter, r *http.Request) {
	claims := auth.ClaimsFrom(r)
	limit := clampInt(r.URL.Query().Get("limit"), 1, 200, 50)
	after, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, "bad cursor", http.StatusBadRequest)
		return
	}
	var soldAt *time.Time
	var id *string
	if after != nil {
		soldAt, id = &after.SoldAt, &after.ID
	}

	rows, err := h.primary.Query(r.Context(), `
		SELECT s.id, s.sold_at, s.total::text, s.status, s.item_count,
		       c.name, u.display_name
		FROM sales s
		JOIN customers c ON c.id = s.customer_id
		JOIN users     u ON u.id = s.seller_id
		WHERE s.org_id = $1
		  AND ($2::timestamptz IS NULL OR (s.sold_at, s.id) < ($2, $3::uuid))
		ORDER BY s.sold_at DESC, s.id DESC
		LIMIT $4`, claims.OrgID, soldAt, id, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	page := SalesPage{}
	for rows.Next() {
		var sr SaleRow
		var t time.Time
		rows.Scan(&sr.ID, &t, &sr.Total, &sr.Status, &sr.ItemCount, &sr.CustomerName, &sr.SellerName)
		sr.SoldAt = t.UTC().Format(time.RFC3339Nano)
		page.Items = append(page.Items, sr)
	}
	if n := len(page.Items); n == limit {
		last := page.Items[n-1]
		t, _ := time.Parse(time.RFC3339Nano, last.SoldAt)
		page.NextCursor = encodeCursor(SalesCursor{SoldAt: t, ID: last.ID})
	}
	writeJSON(w, page)
}

// GET /api/sales/floor — OPTIONAL. PartitionPolicy server floor (R16): the oldest month for
// the tenant, so the scroller stops probing exactly where data ends. Without it, the static
// `floor` in the list definition is used. A LOWER BOUND, re-validated on 'end'.
func (h *Handler) SalesFloor(w http.ResponseWriter, r *http.Request) {
	claims := auth.ClaimsFrom(r)
	var month *string
	h.primary.QueryRow(r.Context(),
		`SELECT to_char(min(sold_at), 'YYYY-MM') FROM sales WHERE org_id = $1`, claims.OrgID).Scan(&month)
	writeJSON(w, map[string]any{"month": month}) // null when the tenant has no sales
}

// POST /api/powersync/upload — the standard PowerSync upload endpoint; unchanged by the bridge.
```

### B.4 Frontend — schema and PowerSync database

```ts
// db/app-schema.ts — PowerSync schema (what SQLite actually holds)
import { column, Schema, Table } from '@powersync/web';

export const AppSchema = new Schema({
  sales: new Table(
    {
      org_id: column.text, customer_id: column.text, seller_id: column.text,
      sold_at: column.text, total: column.text,   // use column.real instead if you sort by total
      status: column.text, item_count: column.integer,
    },
    { indexes: { by_sold: ['sold_at', 'id'] } },
  ),
  customers:  new Table({ org_id: column.text, name: column.text }),
  users:      new Table({ org_id: column.text, display_name: column.text }),
  sale_items: new Table({ sale_id: column.text, sku: column.text, qty: column.integer, price: column.text }),
});
```

```ts
// db/schema.ts — Drizzle tables mirroring AppSchema; row types are born here
import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

export const sales = sqliteTable('sales', {
  id: text('id').primaryKey(),
  orgId: text('org_id').notNull(),
  customerId: text('customer_id').notNull(),
  sellerId: text('seller_id').notNull(),
  soldAt: text('sold_at').notNull(),          // ISO string: orders identically in PG and SQLite (§8.3)
  total: text('total').notNull(),             // numeric → string on both sides
  status: text('status', { enum: ['OPEN', 'PAID', 'VOID'] }).notNull(),
  itemCount: integer('item_count').notNull(),
});

export const customers = sqliteTable('customers', {
  id: text('id').primaryKey(),
  orgId: text('org_id').notNull(),
  name: text('name').notNull(),
});

export const users = sqliteTable('users', {
  id: text('id').primaryKey(),
  orgId: text('org_id').notNull(),
  displayName: text('display_name').notNull(),
});
```

```ts
// db/powersync.ts
import { PowerSyncDatabase } from '@powersync/web';               // '@powersync/react-native' on RN
import { wrapPowerSyncWithDrizzle } from '@powersync/drizzle-driver';
import { AppSchema } from './app-schema';
import * as schema from './schema';

export const db = new PowerSyncDatabase({
  schema: AppSchema,
  database: { dbFilename: 'pos.db' },
});

export const drizzle = wrapPowerSyncWithDrizzle(db, { schema });
```

```ts
// db/connector.ts — the standard PowerSync connector, unchanged. The bridge never touches
// uploads; it only observes ps_crud draining when batch.complete() runs, and it reads the
// token this connector returns to derive identity (B.5).
import type { AbstractPowerSyncDatabase, PowerSyncBackendConnector } from '@powersync/web';
import { getJwt } from '../auth';          // whatever your auth library exposes (Supabase, Clerk, custom)
import { api } from '../api';

export const connector: PowerSyncBackendConnector = {
  async fetchCredentials() {
    return { endpoint: import.meta.env.VITE_POWERSYNC_URL, token: await getJwt() };
  },

  async uploadData(database: AbstractPowerSyncDatabase) {
    let batch = await database.getCrudBatch(100);
    while (batch) {
      await api.post('/api/powersync/upload', batch.crud);
      await batch.complete();                                 // drains ps_crud → "completed" delta
      batch = batch.haveMore ? await database.getCrudBatch(100) : null;
    }
  },
};
```

### B.5 Frontend — create the bridge once; identity comes from the connector

```ts
// bridge.ts — a plain module. No auth import, no claims, no hooks.
import { QueryClient } from '@tanstack/query-core';
import { createBridge } from 'powersync-bridge';
import { createPowerSyncDriver } from '@powersync-bridge/powersync';
import { drizzleAdapter } from '@powersync-bridge/drizzle';
import { db, drizzle } from './db/powersync';

export const queryClient = new QueryClient();

const adapter = drizzleAdapter(drizzle);                 // runs queries through the ORM → decoded rows (§9)
export const bridge = createBridge({
  driver: createPowerSyncDriver(db, {
    adapter,
    onlineHint: () => navigator.onLine,                  // NetInfo.isConnected on RN; omit → undefined
  }),
  adapter,
  queryClient,
  platformGates: { native: 'local-first', web: 'race' }, // R8
  measure: { sink: (m) => telemetry.track('race', m) },  // §5.4, optional
});
// createBridge subscribes to crud deltas and sync status NOW — before any list mounts —
// so an upload acked before the first screen renders is still shielded (F6-001).
```

```ts
// db/boot.ts — connect THROUGH the bridge so it sees every token the connector produces
import { db } from './powersync';
import { connector } from './connector';
import { bridge } from '../bridge';

export async function bootDatabase() {
  await db.init();
  await db.connect(bridge.wrapConnector(connector));      // PROPOSED
}
```

What `wrapConnector` does inside the package (the app never writes this):

```ts
// packages/core/src/identity.ts
const VOLATILE_CLAIMS = new Set([
  'exp', 'iat', 'nbf', 'jti', 'auth_time', 'at_hash', 'nonce', 'azp',   // OIDC volatile set
  'sid', 'session_id', 'rat',                                             // vendor session claims
]);

function decodeJwtPayload(token: string): Record<string, unknown> {
  const b64 = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
  return JSON.parse(atob(b64));            // no verification: PowerSync already trusts this token
}

export function identityKeyFromToken(token: string, denyList = VOLATILE_CLAIMS): string {
  const claims = decodeJwtPayload(token);
  const stable = Object.keys(claims).filter((k) => !denyList.has(k)).sort();
  return JSON.stringify(stable.map((k) => [k, claims[k]]));   // canonical; a key, not a hash
}
```

```ts
// packages/core/src/bridge.ts (excerpt)
wrapConnector(connector: PowerSyncBackendConnector): PowerSyncBackendConnector {
  return {
    ...connector,
    fetchCredentials: async () => {
      const creds = await connector.fetchCredentials();
      if (creds?.token) this.identity.observe(identityKeyFromToken(creds.token, this.denyList));
      // observe(): key unchanged → no-op (token refreshes never churn the epoch);
      //            key changed   → synchronous epoch bump + root swap (R15)
      return creds;
    },
  };
}
// Logout: disconnectAndClear() flips SyncStatus.hasSynced → false; the driver treats that as
// an epoch signal. No app code.
```

Escape hatch, only for setups where scope is not in the JWT (rare):

```ts
bridge.setIdentity({ userId: user.id, orgId: user.activeOrgId });   // PROPOSED; imperative, from auth flow
```

### B.6 Frontend — the raced "recent sales" list (page 1, with joins)

```ts
// lists/recentSales.ts
import { z } from 'zod';
import { desc, eq } from 'drizzle-orm';
import { defineRacedList } from 'powersync-bridge';
import { drizzle } from '../db/powersync';
import { sales, customers, users } from '../db/schema';
import { api } from '../api';

// Raw API shape is schema-derived (§8.2). Never Record<string, unknown>.
const SaleApi = z.object({
  id: z.string(),
  sold_at: z.string(),
  total: z.string(),
  status: z.enum(['OPEN', 'PAID', 'VOID']),
  item_count: z.number().int(),
  customer_name: z.string(),
  seller_name: z.string(),
});
type SaleApi = z.infer<typeof SaleApi>;

export const RecentSales = defineRacedList({
  // TItem is inferred from THIS query's decoded result (§8.1): a joined, column-subset row.
  // The definition closes over the app's drizzle instance; annotate the scope.
  query: ({ orgId }: { orgId: string }) =>
    drizzle
      .select({
        id: sales.id,
        soldAt: sales.soldAt,
        total: sales.total,
        status: sales.status,
        itemCount: sales.itemCount,
        customerName: customers.name,
        sellerName: users.displayName,
      })
      .from(sales)
      .innerJoin(customers, eq(customers.id, sales.customerId))
      .innerJoin(users, eq(users.id, sales.sellerId))
      .where(eq(sales.orgId, orgId))
      .orderBy(desc(sales.soldAt), desc(sales.id))
      .limit(50),
  // The adapter derives watch tables from the builder: ['sales', 'customers', 'users'].

  fetchSnapshot: async ({ orgId }, signal) => {                                 // PROPOSED
    const res = await api.get('/api/sales?limit=50', { signal });
    return { rows: z.array(SaleApi).parse(res.items), asOf: res.asOf };
  },

  // Return annotation is the compile-time check against TItem. Zero assertions.
  mapApi: (o: SaleApi) => ({
    id: o.id,
    soldAt: o.sold_at,
    total: o.total,
    status: o.status,
    itemCount: o.item_count,
    customerName: o.customer_name,
    sellerName: o.seller_name,
  }),

  orderKey: ['soldAt', 'id'] as const,   // tuple: sold_at is not unique (§6.3)
  limit: 50,                             // R7: the list is a window
  staleTime: 10 * 60_000,                // PROPOSED: one snapshot per epoch × key × window (R9)
  stream: { name: 'sales_window' },      // owning stream gates freshness per-stream (R5)
  table: sales,                          // optional: enables toDbRow + presence-gated writes
});
```

### B.7 Frontend — the infinite history list (escalation past the window)

```ts
// lists/salesHistory.ts
import { and, desc, eq, lt, or } from 'drizzle-orm';
import { defineInfiniteList, type Cursor, type PageArgs } from 'powersync-bridge';
import { drizzle } from '../db/powersync';
import { sales, customers, users } from '../db/schema';

type SaleRow = Awaited<ReturnType<typeof RecentSales.query>>[number];
type SaleCursor = Cursor<SaleRow, ['soldAt', 'id']>;   // [string, string] | undefined
import { api } from '../api';
import { RecentSales } from './recentSales';

const saleProjection = {
  id: sales.id,
  soldAt: sales.soldAt,
  total: sales.total,
  status: sales.status,
  itemCount: sales.itemCount,
  customerName: customers.name,
  sellerName: users.displayName,
};

function prevMonth(m: string): string {
  const [y, mo] = m.split('-').map(Number);
  const d = new Date(Date.UTC(y, mo - 2, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

export const SalesHistory = defineInfiniteList({
  pageSize: 50,
  orderKey: ['soldAt', 'id'] as const,        // TCursor = [string, string]

  // page argument FIRST, annotated with the Cursor type (includes undefined = first page)
  query: ({ cursor, limit }: PageArgs<SaleCursor>, { orgId }: { orgId: string }) =>
    drizzle
      .select(saleProjection)
      .from(sales)
      .innerJoin(customers, eq(customers.id, sales.customerId))
      .innerJoin(users, eq(users.id, sales.sellerId))
      .where(
        and(
          eq(sales.orgId, orgId),
          cursor
            ? or(                                                     // tuple keyset (§8.3)
                lt(sales.soldAt, cursor[0]),
                and(eq(sales.soldAt, cursor[0]), lt(sales.id, cursor[1])),
              )
            : undefined,
        ),
      )
      .orderBy(desc(sales.soldAt), desc(sales.id))
      .limit(limit),                            // engine passes pageSize + 1 (lookahead)

  // Stream escalation = equality partitions only (§6.1). The month is the partition.
  escalate: {
    kind: 'partition',
    stream: 'sales_month',                                            // PROPOSED field
    partitionOf: (cursor) => (cursor ? cursor[0].slice(0, 7) : currentMonth()),  // undefined = first page
    nextPartition: prevMonth,
    partitionParams: (m) => ({ month: m }),                           // PROPOSED → subscription.parameter('month')
    floor: '2015-01',                                                 // static floor: always required
    serverFloor: async () => (await api.get('/api/sales/floor')).month, // OPTIONAL exact floor (R16)
    ttl: 5 * 60,                                                      // seconds (§6.5)
    partitionCap: 3,
  },
});

// Alternative: page-granular API escalation instead of streams (no extra sync scope).
export const SalesHistoryViaApi = defineInfiniteList({
  pageSize: 50,
  orderKey: ['soldAt', 'id'] as const,
  query: SalesHistory.query,
  escalate: {
    kind: 'api',
    fetchPage: async (cursor, signal) => {
      const res = await api.get(`/api/sales?limit=50&cursor=${encodeCursor(cursor)}`, { signal });
      return res.items.map(RecentSales.mapApi);
    },
  },
});
```

### B.8 Frontend — components

```tsx
// App.tsx
import { BridgeProvider } from '@powersync-bridge/react';
import { bridge } from './bridge';

export function App() {
  return (
    <BridgeProvider bridge={bridge}>
      <Router />
    </BridgeProvider>
  );
}
```

```tsx
// screens/RecentSalesScreen.tsx
import { FlatList } from 'react-native';
import { useRacedList } from '@powersync-bridge/react';
import { RecentSales } from '../lists/recentSales';
import { voidSale } from '../mutations/sales';

export function RecentSalesScreen({ orgId }: { orgId: string }) {
  const { items, state, awaiting, errors } = useRacedList(RecentSales, { orgId });
  // items: PagedRow<{ id; soldAt; total; status; itemCount; customerName; sellerName }>[]
  // state: 'pending' | 'api' | 'local' | 'converged'

  if (state === 'pending') return <Skeleton rows={8} />;

  return (
    <>
      {errors.api && state === 'local' && <Banner>Showing last synced data</Banner>}
      {awaiting.local && <SyncIndicator />}

      <FlatList
        data={items}
        keyExtractor={(r) => r.row.id}                     // stable ids → no remounts (R6)
        renderItem={({ item }) => (
          <SaleRow
            sale={item.row}                                 // fully typed, no casts
            muted={item.stale === true || !item.presentLocally}
            onVoid={item.presentLocally ? () => voidSale(item.row.id) : undefined} // R14 presence gate
          />
        )}
      />
    </>
  );
}
```

```tsx
// screens/SalesHistoryScreen.tsx
import { FlatList, Text } from 'react-native';
import { useInfiniteList } from '@powersync-bridge/react';
import { SalesHistory } from '../lists/salesHistory';

export function SalesHistoryScreen({ orgId }: { orgId: string }) {
  const { items, loadMore, boundary, boundaryReason, awaitingSync } =
    useInfiniteList(SalesHistory, { orgId });                                   // PROPOSED return shape

  return (
    <FlatList
      data={items}
      keyExtractor={(r) => r.row.id}
      onEndReached={() => boundary !== 'end' && loadMore()}
      renderItem={({ item }) => <SaleRow sale={item.row} muted={!item.presentLocally} />}
      ListFooterComponent={
        awaitingSync ? <Spinner label="Syncing older months…" />
        : boundary === 'unknown' ? <RetryRow reason={boundaryReason} onPress={loadMore} />  // offline | timeout | partition-cap
        : boundary === 'end' ? <Text>No more sales</Text>
        : null
      }
    />
  );
}
```

```ts
// mutations/sales.ts — writes go through Drizzle/PowerSync as usual
import { eq } from 'drizzle-orm';
import { drizzle } from '../db/powersync';
import { sales } from '../db/schema';

export async function voidSale(id: string) {
  await drizzle.update(sales).set({ status: 'VOID' }).where(eq(sales.id, id));
  // → ps_crud UPDATE → livePending shields the row (R1)
  // → upload → batch.complete() → recentlyAcked until a checkpoint with stamp ≥ ack completes
  // → the local value wins the whole way; the API snapshot can never revert it
}
```

```tsx
// screens/SaleSearchResult.tsx — open ONE arbitrary old sale: an on-demand stream,
// no server table, no endpoint, no flagger.
import { useOnDemandStream } from '@powersync-bridge/react';   // thin wrapper over
import { SaleDetail } from './SaleDetail';                     // db.syncStream(name, params).subscribe()

export function SaleSearchResult({ saleId }: { saleId: string }) {
  const { status } = useOnDemandStream('sale_by_id', { sale_id: saleId }, { ttl: 300 });
  // status: 'subscribing' | 'syncing' | 'synced' | 'timeout' | 'offline'
  // subscription is held while mounted; TTL keeps the rows warm for 5 min after unmount

  if (status !== 'synced') return <Spinner label="Fetching this sale…" />;
  return <SaleDetail saleId={saleId} />;                    // reads from local SQLite, live
}
```

```tsx
// screens/SaleDetail.tsx — line items: same pattern, different stream
import { useQuery } from '@powersync/react';
import { toCompilableQuery } from '@powersync/drizzle-driver';
import { useOnDemandStream } from '@powersync-bridge/react';

export function SaleDetail({ saleId }: { saleId: string }) {
  useOnDemandStream('sale_detail', { sale_id: saleId }, { ttl: 300 });
  const { data: items } = useQuery(
    toCompilableQuery(drizzle.select().from(saleItems).where(eq(saleItems.saleId, saleId))),
  );
  return <ItemList items={items} />;
}
```

### B.9 When you would add `autoSyncFlag` (§7) — not in this example

Only two triggers justify the server-side flag table:

```text
1. Bucket economy: each (stream, params) subscription is ONE bucket; the cap is ~1,000/user.
   A user who opens 800 old sales one by one via sale_by_id holds 800 buckets for the TTL.
   A per-user "flagged" bucket holds all of them in ONE.
2. Server-decided membership: the set cannot be written as an equality parameter
   ("everything related to this case", computed by the backend).
```

If either applies, the recipe is the §7 contract: a `sync_requests(user_id, row_id)` table
joined inside a stream gated by `auth.user_id()`, a purge job, and
`createAutoSyncFlagger({ requestFlag, stream, timeoutMs })`. Otherwise skip it.

### B.10 Adopter checklist (goes in the README)

```md
1. orderKey must order identically in Postgres and SQLite: ISO-8601 UTC strings, uuidv7,
   integers. Keep numeric as string on both sides.
2. Every table the local query JOINs must be synced (customers, users at priority 1),
   or local rows render with missing names until they land.
3. The list endpoint reads from the primary (or echoes asOf), uses the same tenant scope
   as the streams, and pages by the same tuple key with an opaque cursor.
4. Every stream using subscription.parameter must also filter on auth.*; run
   check-sync-streams in CI.
5. Connect through bridge.wrapConnector(connector) so identity is derived from the JWT.
   Only call bridge.setIdentity() when scope is not in the token.
6. Mount <BridgeProvider> once at app root so crud-delta observation starts before any
   list mounts. Leave platformGates.native = 'local-first'.
```
