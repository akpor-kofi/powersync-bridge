# powersync-bridge — Architecture & Review Brief

> **Status:** Pre-implementation design **v7 — REVIEW CLOSED + DX PASS** (Sep 2026). Six
> rounds of adversarial review integrated: 77 findings (25 + 17 + 13 + 12 + 7 + 3), all
> accepted, zero rejected, zero deferred. v7 folds in a **developer-experience pass** (marked
> **[DX]**): backend requirements reduced to an explicit adopter contract (§4.1), identity
> derived from the connector's own token (§9, R15), `autoSyncFlag` made optional (§7),
> partition streams computed from existing columns (§6.1/§6.3), offset APIs accepted as a
> degraded mode (§6.1, R16), and the public API gaps closed (§5.1, §6.1, §9).
> Round 1: `REVIEW-round1.md` → `REVISION-round1.md` · Round 2: `REVIEW-round2.md` →
> `REVISION-round2.md` · Round 3: `REVIEW-round3.md` → `REVISION-round3.md` · Round 4:
> `REVIEW-round4.md` → `REVISION-round4.md` · Round 5 (verification): `REVIEW-round5.md` →
> `REVISION-round5.md` — *"no further adversarial round is warranted"* · Round 6
> (post-convergence verification): `REVIEW-round6.md` → `REVISION-round6.md` — *"the
> design is converged and no further review round is warranted."*
> Sections marked **[R1-n]** … **[R6-n]** carry round invariants.
>
> **Implementation status (2026-09-25):** v0 alpha built in this repo — `packages/core`
> (engine + `powersync-bridge/testing` fake driver), `packages/react`, `packages/adapters/drizzle`,
> `packages/powersync`. Invariant tests T1–T44 that are expressible on the fake driver pass
> (33 engine tests + adapter tests); T13/T14/T21 need external tooling and are not yet written.
> Two definition-shape decisions were forced by TypeScript inference and are recorded in
> §5.1/§6.1: definitions close over the app's ORM instance (no `db` callback parameter) and the
> infinite query takes the page argument first, annotated with `Cursor<TItem, K>` (which
> includes `undefined`). The PowerSync driver derives differential streams from `onChange` +
> decoded adapter execution (§9 note) because the SDK's `differentialWatch` cannot decode joined
> ORM rows. Bundle budgets (§11) hold: core 8.2 kB, React adapter 0.6 kB, PowerSync driver
> 1.8 kB (minified + gzip).
>
> This document is written to be **self-contained**: a reviewer (human or model) with **no
> prior context** should be able to evaluate the architecture from this file alone.
>
> **Reference thread:** T3 Code (OpenCode harness) thread `3603da2f-cc41-46c9-bd4b-7aa5bfe48dcc`,
> environment `5f6f39a5-a95d-4f37-9b47-bd61f38b3b79`. The full design conversation lives there.
> Share link: _pending — enable thread sharing in T3 Code and paste the URL here._
>
> **Companion docs (planned, not yet written):** `README.md` (user-facing), `CONTRIBUTING.md`.

---

## 0. How to use this document

- Sections 1–14 are the architecture itself: goals, problem statement, the three primitives,
  type system, security model, and open questions.
- **Terminology is defined exactly once** [R1]: row shape (`§5.1`), list state (`§5.1`),
  snapshot record & freshness (`§5.2`), boundary (`§6.2`), error taxonomy (`§5.1`).
- **Four round corrections shape everything downstream:** (1) `SyncStatus.hasSynced` is
  *persisted* — true at open on any previously synced DB — so "answered" is never derived
  from it; arbitration is **session-scoped freshness** (§5.2). (2) Pending shielding is a
  **three-set union** — live set, a **seeded** snapshot-lifetime window, and
  `recentlyAcked` (§5.2). (3) Freshness stamps the checkpoint at **download start**, is
  **derivable per checkpoint** (no transition observation needed), and is kept
  **monotonic** (§5.2). (4) Row removal is a **four-way taxonomy** resolved by a batched
  presence probe and stream expiry, not by "active" alone (§6.4).
- **Adopter contract first [DX]:** §4.1 states what a backend must change (nothing), must
  align (sort order), and may add per feature. Read it before §5–§7 so optional features are
  not mistaken for setup steps.
- **Normative wording:** the rules (§5.3) are normative; every other section must agree
  with them. Drift between them was found twice in round 4 (§10.5, §9) and once in
  round 5 (T3) — §16 makes a drift pass mandatory at the end of every integration round.
- Section 15 records the **review convergence** (the per-round reviewer prompts are
  preserved in the round files).
- Section 16 is the **agent-to-agent feedback protocol** (round mechanics, file naming).
- Section 17 is the research index (all sources consulted, incl. rounds 1–2 verification).

---

## 1. Vision & thesis

**One-line thesis:** *The right data source — local synced SQLite vs. live API — is a runtime,
per-screen decision, not an architectural one. This package makes that decision automatic and
invisible.*

**Scope of the claim [R1, F-012].** The bridge does *not* claim "API beats local" in general.
It claims exactly two defensible value propositions, and ships the instrument to verify them
per app (`measureRace()`, §5.4):

- **V1 — First visit:** a first-ever visitor's local table answers *instantly with an empty
  table*; without an API leg the UI renders "nothing" until first sync completes. A live API
  snapshot is the only correct fast answer.
- **V2 — Large DB / recurring re-mounts:** on web, the first watched query after page load —
  and every re-mount/return-to-foreground that re-runs it — can cost seconds at moderate DB
  sizes (measured in PR #1101's thread: ~7.9 s re-mount at 50 MB; §2.2). A persisted or
  fetched snapshot paints in milliseconds.

Once local is warm **and fresh in this session**, local wins decisively (5–25 ms reads,
offline, real-time) and the bridge must get out of the way (native gate R8, local ownership
§5.2). PowerSync maintainers are actively improving the query path; the bridge is designed to
**shrink gracefully** when they do (§3), which is why the arbitration core stays small and the
value-tracking probe is first-class.

**Three primitives:**

| Primitive | Solves | PowerSync docs analog |
|---|---|---|
| `racedQuery` / `useRacedList` | Correct first paint when either source can be stale or slow; reconcile by id with pending-write awareness and freshness arbitration | none (gap) |
| `infiniteList` | Scrolling past the synced window via **bounded partition escalation** (streams) or API pages, with eviction-vs-deletion safety | [Infinite Scrolling](https://docs.powersync.com/client-sdks/infinite-scrolling) (prose only) |
| `autoSyncFlag` | Client-triggered, per-user server-side flagging of rows to sync | doc option 4 (prose only) |

**Positioning:** NOT an "infinite scroll" package. The race/reconcile core is the headline;
the infinite list and flag helper are sibling primitives sharing one core (provenance
tracking, id-keyed reconciliation, pending-write arbitration, identity epochs).

**Name:** `powersync-bridge` (working title; alternatives: `powersync-race`, `powersync-liaison`).

---

## 2. Problem statement (with evidence)

### 2.1 The web read path, stated precisely [R1, F-012]

PowerSync JS starts reads **before** `connect()` completes — local reads are not serialized
behind sync by design. The real regime is:

```
First visit:   table exists immediately, but is EMPTY until first sync completes.
               → "fast" local answer is fast AND WRONG (renders nothing).
Warm remount:  DB open + first watched query on a large store costs seconds
               (IndexedDB VFS scan; re-mounts re-pay it), while an API round
               trip is one RTT. The local rows are LAST SESSION's — fresh to
               the eye, stale to the server. [R2, F2-002]
Warm & fresh:  5–25 ms reads, offline-capable, real-time. Local wins. Period.
```

The variable is not "which engine is faster" but **which state each source is in**: empty,
stale-but-populated, or current — and *freshness is session-scoped*, because a warm DB
carries yesterday's checkpoint. That is a *runtime* state, hence a runtime decision.

### 2.2 Community-measured evidence (as of Sep 2026) [R1, F-011, F-012]

- **PR [powersync-js#1101](https://github.com/powersync-ja/powersync-js/pull/1101)** (open,
  **plugin API withdrawn by the author on Sep 19, 2026**). What survives is the measurement
  record and the maintainer position:
  - First watched query after page load: **~1.3 s at 10 MB → ~23–29 s at 200 MB**
    (IndexedDB VFS; OPFS WAL similar for the query itself).
  - **Re-mount cost recurs:** in-session navigation re-runs the watched query —
    **113 ms @ 10 MB → 7.9 s @ 50 MB → 26.2 s @ 200 MB** (IndexedDB, uncached) vs
    **0.14–0.42 ms** from a memory-layer cache. The gap is categorical: the cached path runs
    no query at all.
  - Caveats logged by the author: headless-Chromium harness, unindexed `ORDER BY` query
    (an index would cut the uncached column), OPFS "cold boot" rows compare worker reuse not
    file systems.
  - Maintainer position (simolus3, Sep 17): the plugin approach is rejected (bundle cost for
    everyone, IoC complexity); *"putting a cache in front of a local SQLite database is
    absurd — if we're slow enough to make that seem like an option, we must look into ways to
    improve this"*; preferred composition is `onChange` + `AsyncIterable` combinators. The
    author's reduced ask (Sep 19): `differentialWatch({ initialData })` **usable before the
    database is ready**. This is the seam our design aligns with (§3).
- **Issue [#1114](https://github.com/powersync-ja/powersync-js/issues/1114)** (open) —
  reported ~30 s post-connect read stall on a ~6.4k-row web store. **Single reporter**; the
  maintainer replied the same day that a mostly-up-to-date sync "should be very fast without
  holding any database locks that long" — i.e. a bug, not steady-state behavior.
  `@powersync/web` 2.4.0/2.4.1 shipped shared-worker/checkpoint fixes afterward. The bridge
  treats this as **tracked, not load-bearing**.
- **Issue [#1081](https://github.com/powersync-ja/powersync-js/issues/1081)** (open) —
  iOS 26.2 WebKit kills pages once the WASM module loads (main thread). **The bridge cannot
  mitigate this**: a page kill takes the API leg down with the local leg. Listed for
  substrate awareness only.
- **Issue [#1063](https://github.com/powersync-ja/powersync-js/issues/1063)** (open) —
  fixed ~21 s first-sync stall over NDJSON-HTTP on Capacitor iOS; a transport-buffering issue
  (solved by WebSocket), not by racing. Not load-bearing.
- **SDK velocity:** `@powersync/web@2.4.1` (2026-09-23), `@powersync/common@2.3.0`,
  `@powersync/tanstack-react-query@0.3.4` (2026-09-21) — the query path is moving; §3
  positions the bridge relative to it and §5.4 instruments the delta.

### 2.3 The gap

PowerSync's [Infinite Scrolling doc](https://docs.powersync.com/client-sdks/infinite-scrolling)
lists four approaches (pre-sync everything; subscription-parameter paging; API fallback;
server-side flags) as prose with pros/cons. No reusable abstraction exists for:

1. **First-visit correctness** — rendering a live API snapshot and then reconciling it
   against the first sync without stale rows, resurrected deletes, or flicker (§5);
2. **The sync-window handoff** — "rows already in SQLite" → "rows that must come from the
   server," including what it means when a row disappears (delete vs eviction, §6.4);
3. **Per-user flag-driven sync UX** (§7).

Closest prior art is [`powersync-query-cache`](https://github.com/gartz/powersync-query-cache)
(persisted last-result paint). It does not cover first-ever visits (no cache yet) and does not
reconcile a live API snapshot against local rows. The official
`@powersync/tanstack-react-query` covers TanStack binding of *local* watches, not
source arbitration. That intersection is this package.

---

## 3. Prior art & ecosystem position [R1, F-011]

| Project / API | What it does | Our relationship |
|---|---|---|
| `differentialWatch` (in-SDK) | Id-keyed diffs over watched queries (`rowComparator {keyBy, compareBy}`), reference-stable rows | **Consume, never re-implement.** The bridge's reconcile consumes these diffs; the bridge never re-diffs rows (R6, §11). Core keeps no comparator of its own. |
| `differentialWatch({ initialData })` (proposed upstream, PR #1101 thread) | Seed the keyed snapshot so an early paint isn't re-diffed as inserts; must work pre-DB-ready | **Track.** If/when it lands, seeding the SDK watch with the API snapshot becomes a possible fast path for §5; until then the bridge seeds **its own** overlay store (zero SDK dependency). §14.7 tracks it. |
| `@powersync/tanstack-react-query` 0.3.4 (official) | TanStack Query bindings over PowerSync watches | Our React adapter builds **alongside** it (same patterns: `useSyncExternalStore`, throttled watches); we do not wrap it, because our data is an arbitrated overlay, not a raw watch. |
| `powersync-query-cache` (gartz) | Persisted last-result paint before DB opens | Complementary, different point in the pipeline (persisted cache vs live API snapshot). Can be stacked: cache → API leg → local. |
| PR #1101 plugin API | ~~Core seed/observe hooks~~ **withdrawn** Sep 19, 2026 | Dead end; do not design against it. Accurate history kept in §2.2. |
| ElectricSQL reads | Read-your-writes over sync | Pattern inspiration only. |

**What remains in core that the SDK will not do:** the SDK diffs rows *within one source*;
it does not decide anything *between sources*. Core owns: API-leg lifecycle (snapshot with
`fetchedAt`, fetched once per epoch × queryKey × staleTime window), **freshness-based
per-id arbitration** (§5.2), snapshot-relative pending/tombstone sets (§5.2), quarantine
(R4), bounded partition escalation + boundary semantics (§6.2), eviction-vs-delete
provenance via `SyncStatus.syncStreams` (§6.4), flag state machine (§7), identity epochs
(§9/§10.5), and the React sugar.

**Strategic path:** standalone npm first; demo PR to `powersync-js/demos`; a docs recipe;
coordinate in Discord *framed as* "arbitration + partition escalation for the doc's option 2+3
gap," explicitly acknowledging the maintainers' in-SDK query-path work; promote to official
add-on only if adopted. No engine changes are required or requested; upstream `initialData`
landing would let core *delete* code (the overlay store), which is the desired direction.

---

## 4. Scope & non-goals

**In scope:** client-side data-source arbitration, reconciliation, provenance, partition
escalation, sync flagging UX, typed query/list contracts, identity epochs, the
`measureRace()` diagnostic probe.

**Non-goals:**
- No sync engine changes; no server component shipped (the API/flag endpoints are the app's).
- No replacement for TanStack Query / SWR (we integrate; React adapter is sugar).
- No offline write queue (PowerSync owns uploads; we only *read* pending-op refs, §9).
- No SSR data fetching in v1 (web worker/WASM makes SSR of local DB impossible; API leg may
  stream from server later — open question §14.6).
- No claim of universal "API beats local" superiority (§1); the API leg is off by default on
  platforms where it cannot win, and `measureRace()` exists to prove value per app.

### 4.1 Adopter contract — what the backend must, must not, and may do [DX]

**Must change: nothing.** No new tables, no new columns, no change to existing sync streams.
The raced list works against whatever the app already syncs and whatever list endpoint it
already serves.

**Must align (not a change, a check):** the API list endpoint sorts rows the way the local
query does, with `id` as the final tiebreaker. The bridge merges the two answers by id and
keeps the top `limit`; if the two sides disagree on order, the merged window is undefined and
the client cannot detect it. Every table already has an `id`, so the tiebreaker costs nothing.

**Per feature — what you want vs what you need:**

| What you want | Streams you need | Routes you need | Schema |
|---|---|---|---|
| Fast, correct first paint of any list | none beyond what you have | your existing list endpoint (same sort, same auth scope) | none |
| Scroll past the synced window; older pages from the API | none beyond what you have | the same endpoint accepting a `cursor` (or `offset`, degraded — §6.1) | none |
| Scroll past the synced window; older *months* pulled into the offline DB | one stream with `subscription.parameter('month')`, partition computed with `substring()` on your existing timestamp (§6.3) | optional `GET …/floor` (static floor otherwise, §6.2) | none |
| Open one old record by id, offline-capable | one stream with `id = subscription.parameter('id')` (`useOnDemandStream`, §7) | none | none |
| Server-decided or bucket-economical "pull these rows for me" | §7 `autoSyncFlag` (optional) | `POST …/sync/flag` | `sync_requests` table |

**Routes, concretely.** Route 1 is required and is the endpoint every app already has:

```
GET /api/<list>?limit=50
→ { "items": [ { id, …same columns the local query projects… } ] }
  1. scoped by the caller's token exactly as the sync streams are
  2. ORDER BY identical to the local query, id last
  3. preferably served from the primary database (§10.2) — a lagging replica can briefly
     resurrect a just-deleted row; the bridge shields that for the few seconds sync needs,
     primary reads remove the window entirely
```

Route 1 extended (optional, API-page escalation only): the same endpoint with
`?cursor=<opaque>` returning `nextCursor`; or `?offset=` in the documented degraded mode.
Route 2 (optional, partition escalation only): `GET /api/<list>/floor → { "month": "2019-03" }`,
i.e. `to_char(min(<sort timestamp>), 'YYYY-MM')` for the tenant. Route 3: the PowerSync upload
endpoint, which exists already and is untouched.

**Three things that cannot be relaxed** (they follow from having two sources, not from taste):

1. Same sort on both sides, `id` last.
2. The sort column must compare identically on Postgres and SQLite (§8.3 gives the one-line
   fixes: `COLLATE "C"` for text, `column.real` on the client for decimals).
3. Client-requested sync data can only be selected by **equality** (PowerSync's rule): "older
   data into the offline DB" means a bucket such as a month, never "everything before X". The
   API-page path has no such limit.

---

## 5. Primitive 1 — `racedQuery` / `useRacedList`

### 5.1 Contract (shared vocabulary — defined here, referenced everywhere) [R1, F-021]

```ts
type RowOrigin = 'local' | 'api';             // a row's producing source, post-arbitration

interface PagedRow<T> {
  row: T;
  origin: RowOrigin;
  presentLocally: boolean;   // a LOCAL row exists in SQLite right now — write guard keys
                             // on this, never on origin [R2, F2-008]
  stale?: true;              // rendered from local before any fresh snapshot covered it
}

type ListState = 'pending' | 'api' | 'local' | 'converged';
//   pending   = no source has answered yet
//   api/local = one source answered first and is rendering
//   converged = local is fresh and owns the list (§5.2)

interface RacedListDefinition<TItem, TScope, TApi, K extends OrderKey<TItem>> {
  query: (scope: TScope) => OrmSelect<TItem>;             // builder; closes over the app's ORM
                                                          // instance; adapter executes it
                                                          // decoded (§9). ORDER BY … LIMIT [DX]
                                                          // Annotate `scope`: a callback whose
                                                          // params are all annotated is not
                                                          // context-sensitive, which is what lets
                                                          // TS infer TItem from the return.
  fetchSnapshot: (scope: TScope, signal: AbortSignal)     // the API leg [DX]
    => Promise<{ rows: TApi[]; asOf?: string }>;          //   asOf optional (§10.2)
  mapApi: (raw: TApi) => TItem;                           // §8.2; return type checked vs TItem
  orderKey: K;                                            // scalar or 2-tuple (§8.3); `id` is
                                                          //   always the implicit tiebreaker
  limit: number;                                          // the list is a LIMITed window (R7)
  staleTime?: number;                                     // ms; default 10 min — one snapshot per
                                                          //   epoch × queryKey × window (R9) [DX]
  table?: DrizzleTable;                                   // optional: toDbRow / write guards (§8.1)
  stream?: { name: string; params?: Params };             // optional: owning stream gates
}                                                         //   freshness per-stream (R5)

useRacedList(RecentSales, { orgId }) => {
  items: PagedRow<TItem>[];
  state: ListState;
  awaiting: { local: boolean; api: boolean };  // which legs have not answered
  errors: { local?: Error; api?: Error; stream?: Error };
}
```

**Root object [DX].** Everything hangs off one bridge instance created once, in a plain
module, before login; identity is never passed in (see §9 / R15):

```ts
const adapter = drizzleAdapter(drizzle);
const bridge = createBridge({
  driver: createPowerSyncDriver(db, { adapter, onlineHint }),
  adapter,
  queryClient,                                   // TanStack QueryClient (fetchQuery + keys only, R11)
  platformGates: { native: 'local-first', web: 'race' },   // R8
  measure?: { sink },                            // §5.4
  identity?: { denyList?: string[]; connectionParams?: string[] },   // R15 overrides
});
await db.connect(bridge.wrapConnector(connector));   // identity is read from the JWT here
// React: <BridgeProvider bridge={bridge}> at app root — crud-delta observation starts at
// createBridge(), before any list mounts (F6-001), not at provider mount.
```

### 5.2 Arbitration engine [R1, F-001, F-002, R2, F2-001, F2-002, F2-003, F2-005, R3, F3-001–F3-004, F3-009, R4, F4-001–F4-005, F4-009, R5, F5-001–F5-004, R6, F6-001]

The engine is an **external store** with a stable `getSnapshot` (consumed via
`useSyncExternalStore`). Every leg is tagged with a monotonic **epoch** = (mount, identity);
any result whose `epoch !== current` is discarded. Per-epoch state lives at two scopes:

- **Identity-scoped engine state** — created at **engine init**, not at list mount, and
  shared by every list store: the **crud-delta mirror** (subscribed to `onCrudChange`
  from engine init, so ops acked before any list mounts are observed) and the
  **`recentlyAcked` set** it feeds. Nothing identity-scoped lives inside a list root.
- **Per-list store roots** — the overlay, `pendingSince(S)`, list-instance tombstones,
  `probeCursor`, and quarantine sets.

On identity change, **bumping the epoch and swapping the engine state plus every list
root to fresh empty `'pending'` state is one synchronous operation** with a single
notification — no frame can render the outgoing tenant's rows under the incoming identity,
no shield set can survive across tenants (integer-id `(table, id)` collisions would
otherwise carry a shield across), and the crud-delta subscription is torn down and
re-created as part of the swap [R4, F4-004][R5, F5-004][R6, F6-001].

**Snapshot record.** Each API fetch produces `S = { rows, requestedAt, fetchedAt, asOf? }` —
fetched **on mount only**, once per **(identity epoch, queryKey, staleTime window)**, and
**never after local ownership within the same epoch** (fresh local has nothing the API leg
adds) [R3, F3-009]. `asOf` (optional — §10.2) is the PowerSync write checkpoint the backend
served the snapshot at. **S retires at ownership**: post-ownership S can never win a
conflict (R9), so at ownership S, its `pendingSince`, and its tombstones are dropped;
surviving api-only rows become plain overlay entries [R4, F4-005].

**Pending shielding [R2, F2-001][R3, F3-001][R4, F4-001, F4-002] — three sets, unioned:**

```
pending(id)  ⇜  id ∈ livePending  ∨  id ∈ pendingSince(S)  ∨  id ∈ recentlyAcked

livePending     = pendingUploadIds() at reconcile time — re-read on EVERY local diff
                  emission (a write that matters always produces one, including another
                  tab's write in the shared DB) and on statusChanged        [R3, F3-012]
pendingSince(S) = SEEDED: livePending(at S.requestedAt) ∪ {refs observed from onCrudChange
                  during S's lifetime, up to S's retirement}. Seeding matters because
                  completions drain ps_crud and listeners carry no payload — an op already
                  pending before requestedAt that completes mid-flight (the reconnect-
                  flush regime) is otherwise in NO set. The driver DERIVES completions by
                  diffing consecutive ps_crud snapshots and emits {added, completed}.
                  Seeds and derived completions all come from that delta. [R4, F4-001][R5, F5-003]
recentlyAcked   = IDENTITY-SCOPED ENGINE STATE (fed by the crud-delta mirror from engine
                  init — an ack landing BEFORE any list mounts is still observed): refs
                  whose upload completed since the last in-session checkpoint whose stamp
                  could carry them; each ref is retained until the FIRST CHECKPOINT WHOSE
                  STAMP ≥ ackTime completes (a checkpoint whose download began before the
                  ack cannot carry the write, however late it completes) — or, when asOf
                  is available, until the client has synced past the write checkpoint
                  returned by complete(writeCheckpoint)  (exact). The stamp is a lower
                  bound up to one network delay, so a ref can retire early by that delay —
                  harmless at ms scale, stated here for symmetry with the freshness rule.
                  This is the write→checkpoint gap a lagging replica exposes; treated
                  like livePending in reconcile step 1. Client-derivable default
                  read-your-writes guard (§10.2).   [R4, F4-002][R5, F5-001][R6, F6-001, F6-003]
```

All sets are keyed `(table, id)` — integer ids collide across tables [R2, F2-004].
Rationale: the live set alone misses snapshots still being consumed; a window closed at
consume misses the normal optimistic-UI case (edit after consume, pre-ownership — the
round-3 critical finding); an unseeded window misses ops that predate the request (the
round-4 reconnect-flush finding).

**Freshness (session-scoped, monotonic, transition-independent) [R2, F2-002][R3, F3-002, F3-003][R4, F4-003].**
`SyncStatus.hasSynced` is persisted — `true` at open on any previously synced DB — so it
never means "synced in this session" and is never used as "answered". `lastSyncedAt` is the
time a checkpoint finished **applying**, but the data it carries reflects the server
position when the download **started** — and it is reset to null on service restart
(SDK source). A transition-based stamp (observing `downloading` false→true) fails twice:
app boot mounts lists while the first download is already running (no transition observed),
and continuous churn never idles (no new transition). Therefore stamps are **derivable per
checkpoint**:

```
stamp(checkpoint_n) = max(observed downloadStarts ≤ its completion,
                          lastSyncedAt of checkpoint n−1,
                          sessionConnectAt when no other bound exists)
  // a checkpoint's download cannot have started before the previous checkpoint
  // completed ⇒ the stamp is a lower bound (up to one network delay) and
  // ADVANCES ON EVERY COMPLETION
on init with downloading === true: the provisional stamp
  max(sessionConnectAt, lastSyncedAt at init) is assigned to the FIRST COMPLETION
  OBSERVED — never to localFreshAt at init (localFresh cannot be true before an
  in-session completion)                                              [R5, F5-007]
localFreshAt       = MONOTONIC per identity epoch: max(previous, stamp(·));
                     a null/undefined observation NEVER lowers it           [R3, F3-003]
sessionConnectAt(epoch) = the epoch's START TIME when the connection is already up at
                     epoch start (token-refresh org switch on a live connection), else
                     the first connect() after it — NEVER per reconnect
                                                      [R3, F3-003][R4, F4-009]
apiFreshAt         = S.fetchedAt
localFresh         = (localFreshAt ≥ sessionConnectAt
                      || owning stream's hasSynced transitioned in-session)
                     && ≥1 local emission received
```

The stamp is a conservative lower bound; the exact fix is the optional `asOf` contract
(§10.2): when both sides carry a PowerSync write checkpoint, compare positions instead of
clocks.

**Reconcile of (S, localRows):**

```
for each id in (S.rows ∪ localRows):
  1. pending(id) (three-set union above):
       op = INSERT/UPDATE → local state wins unconditionally (optimistic value renders)
       op = DELETE        → TOMBSTONED: absent locally + present in S ⇒ SUPPRESSED.
                            Never rendered, never ephemeral. A tombstone retires when a
                            local diff AFTER a subsequent IN-SESSION CHECKPOINT confirms
                            absence — never on snapshot replacement      [R3, F3-004]
  2. in both, not pending → THE FRESHER SOURCE WINS:
       apiFreshAt > localFreshAt → API row wins   (local may be last session's checkpoint,
                                     or mid-download data)
       localFreshAt ≥ apiFreshAt → local row wins (local has seen at least as much)
  3. local-only, not pending:
       no snapshot yet            → render origin 'local', stale: true   (default;
                                     holdPending:'pending' opts into blank-until-answer)
       row ARRIVED IN-SESSION (via a checkpoint) → NEVER quarantined: it is real, and a
                                     lagging snapshot's silence proves nothing [R3, F3-004]
       row is PRE-SESSION (last session's checkpoint) AND S fresh (apiFreshAt >
                                     localFreshAt) and absent → QUARANTINE (withheld)
       otherwise                  → render (local is the freshest known truth)
  4. api-only, not tombstoned → render origin 'api', presentLocally: false.
       No wall-clock decay [R2, F2-012]. AT OWNERSHIP, overlay rows with no local
       presence are handled by classification [R5, F5-002]:
         never seen locally this session → DROPPED (deleted assumption), unless the
           definition sets apiScopeSupersetOfLocal: true (out-of-scope assumption —
           amends F2-012; a deleted row that can never be detected beats a zombie)
           [R4, F4-002]
         seen locally and since removed → the §6.4 outcome applies: classified DELETE
           ⇒ drop; classified eviction ⇒ retain (presentLocally: false)

ordering: sort by definition.orderKey; truncate to definition.limit   [R1, F-008]
```

**Consistency dependency [R3, F3-004].** Steps 2–3 assume the API is **read-your-writes
with respect to the upload path** (§10.2). Under replica lag, freshness comparisons and
quarantine decisions against a lagging snapshot are unsafe; the in-session guards above are
defence in depth, not a substitute for the contract.

**Ownership.** Local owns the list once `localFresh && localFreshAt > apiFreshAt`; from then
on each `differentialWatch` emission is applied to the overlay directly (no re-diff, R6), no
further snapshots are fetched in this epoch (R9), and each row's freshness standing is
re-evaluated only if a *newer, mount-triggered* snapshot ever arrives.

Why freshness and not a boolean: v1's `localAnswered = hasSynced` was already `true` at open
on warm DBs — the V2 regime would have handed every conflict to *yesterday's* checkpoint and
produced a three-state flicker (fresh → yesterday → fresh) that violates the no-flicker
budget. Freshness makes the warm-remount case converge in one direction.

### 5.3 Rules that must hold (reviewer: attack these)

- **R1 — Pending-write supremacy, three-set union [R1,F-001][R2,F2-001][R3,F3-001][R4,F4-001,F4-002][R5,F5-001].**
  Any `(table, id)` in the **live** pending set, in the **seeded** `pendingSince(S)`
  window, or in `recentlyAcked` resolves to local state. The driver derives completions by
  diffing consecutive `ps_crud` snapshots (completion drains the table). `recentlyAcked`
  retires only on a completed checkpoint with **`stamp ≥ ackTime`** (or, with `asOf`, on
  syncing past the returned write checkpoint) — never on completion time alone. Tests
  T15/T16/T22/T23/T31/T32/T38.
- **R2 — No resurrection of deletes [R1,F-002][R2,F2-001][R3,F3-004][R4,F4-005][R5,F5-002].**
  A tombstoned id (pending DELETE) never re-enters the list from a snapshot it predates.
  **Pre-ownership**, tombstones retire when a local diff **after a subsequent in-session
  checkpoint** confirms absence — never on snapshot replacement — or when any in-session
  diff **re-adds** the id (restore / re-create). **At ownership**, S's shields (its
  `pendingSince` and tombstones) retire wholesale with S. Test: T1, T16, T23, T32.
- **R3 — Freshness arbitration [R1,F-001][R2,F2-002][R3,F3-002][R4,F4-003].** Non-pending
  conflicts resolve to the fresher source (`fetchedAt` vs **per-checkpoint lower-bound
  stamps**, monotonic `localFreshAt`) — stamps are derivable without observing
  `downloading` transitions, so boot-mid-download and continuous churn still transfer
  ownership. Persisted `hasSynced` is never the tiebreaker; apply-time timestamps never
  overrate a checkpoint. Test: T2, T24 (parametrized), T33, T34.
- **R4 — Quarantine is pre-session-only [R1,F-001][R2,F2-003][R3,F3-004].** Only rows that
  existed locally **before this session** are quarantine-eligible, and only when a **fresh**
  snapshot positively excludes them; in-session checkpoint arrivals are never quarantined.
  Requires the §10.2 read-your-writes contract. Pre-snapshot, local rows render flagged
  `stale: true` (default) — never a blank list. Tests: T3, T17.
- **R5 — Answered = fresh, not emitted; freshness is monotonic [R1,F-001][R2,F2-002][R3,F3-003].**
  `localFresh` requires in-session freshness evidence (per-stream / per-priority when
  declared) *and* an emission; empty pre-sync tables and warm-stale checkpoints never count
  as answers. `localFreshAt` never regresses (null observations, service restarts, and
  reconnects cannot demote local). Tests: T25, T26.
- **R6 — Diffs in, never re-diff; no clock-based decay [R1,F-019][R2,F2-012][R4,F4-002][R6,F6-002].**
  Local emissions enter as `differentialWatch` diffs; the bridge runs no comparator of its
  own. No clock-based decay. At ownership, overlay rows with no local presence follow
  §5.2 step 4: never seen ⇒ drop unless `apiScopeSupersetOfLocal: true`; seen-and-removed
  ⇒ the §6.4 outcome (DELETE ⇒ drop, eviction ⇒ retain).
- **R7 — Windowed lists are windows [R1,F-008].** `orderKey` + `limit` required; reconciled
  list truncated to `limit` by `orderKey`; post-ownership rows below the local cutoff are
  dropped. Test: T12.
- **R8 — Platform gate [R1].** Native defaults to `local-first` (no API leg): 
  `platformGates: { native: 'local-first' }`. Configurable.
- **R9 — API leg lifecycle [R1,F-013][R2,F2-005][R3,F3-009].** Fetches are **mount-triggered
  only**, one per **(identity epoch, queryKey, staleTime window)**, and **never issued after
  local ownership within the same epoch** — no timer-driven refetch, so ownership cannot
  ping-pong. `fetchedAt` recorded and threaded into arbitration; the AbortSignal is threaded
  end-to-end; no live `useQuery` wraps the API leg; dedup across components via fetchQuery
  keying. Test: T2.
- **R10 — Epoch discipline [R1,F-018].** Late results from a cancelled leg, StrictMode
  double-effects, and identity switches can never write into the live store. Tests: T9, T10.
- **R11 — TanStack contract [R1,F-013].** TanStack is used for `fetchQuery` + cache keying
  only; the React adapter renders from the engine store via `useSyncExternalStore`;
  `structuralSharing` is bypassed (the bridge owns row identity via diffs).
- **R12 — Failure modes [R1][R2,F2-003].** API leg fails (or never returns before local)
  → local renders `stale: true` (`state:'local'`, `errors.api` set) — the list is never
  blank because of a missing snapshot. Local never answers → api-only rendering persists;
  convergence happens whenever local reaches freshness. Both fail → `errors` carries both.
  Test: T17.
- **R13 — Composition dedupe [R1,F-017].** A single global id set per list instance is the
  source of truth; pages/rendering windows are derived views.
- **R14 — Presence-gated writes, probe-backed presence [R2,F2-008][R3,F3-005][R4,F4-006].**
  `presentLocally` is computed by an indexed **presence probe — one batched statement per
  emission** (`SELECT id FROM t WHERE id IN (…)` chunked ≤ 500; absent = removed-set minus
  returned), not inferred from diff membership and not per-row round-trips — only the
  probe can distinguish filter-out from delete, and bulk expiries must not block the
  worker. The write guard keys on `presentLocally`, never on `origin` — retained evicted
  rows are not writable (a PowerSync UPDATE on a missing row is a silent no-op). Removal
  classification follows the four-way taxonomy (§6.4); `onEvicted(ids[])` fires once per
  emission. Test: T7, T27, T28, T36.
- **R15 — Identity contract & atomic epoch-first teardown [R1,F-009][R2,F2-009][R3,F3-007][R4,F4-004,F4-008,F4-009][DX].**
  `identityKey()` is **derived by the bridge from the JWT the connector returns**
  (`bridge.wrapConnector(connector)`; `setIdentity()` only for scope absent from the token)
  = canonical key of (**all JWT claims minus a configurable volatile deny-list** —
  standard OIDC set `exp/iat/nbf/jti/auth_time/at_hash/nonce/azp` plus known vendor
  session claims `sid`/`session_id`/`rat`) **⊕ connection parameters opted in by key**
  (rotating values must be excluded explicitly) **⊕ clear events** — observed via
  `SyncStatus.hasSynced` transitioning `true → false/undefined`, the only client-visible
  effect of `disconnectAndClear()` (no SDK event exists). Teardown on identity change:
  **bump the epoch AND swap the identity-scoped engine state (crud-delta mirror +
  `recentlyAcked`) and every list store's root to fresh empty `'pending'` state in one
  synchronous operation** (single notification — no committed frame can render the
  outgoing tenant), then asynchronously: unsubscribe bridge-held stream handles, tear down
  `diffs()` subscriptions, cancel API legs, re-subscribe the crud-delta mirror for the new
  identity, start the new epoch's legs. One shared epoch per identity scope.
  Tests: T8, T30, T35.
- **R16 — Bounded, progressive partition loop [R2,F2-007][R3,F3-006,F3-008][R4,F4-007][R6,F6-003].**
  Floor is server-provided when available (`min(orderKey)` per tenant — endpoint preferred
  over a signed claim, and documented as a **lower bound** [R3,F3-013]; static fallback);
  partitions per `loadMore` capped (default 3) → `boundary:'unknown',
  reason:'partition-cap'`; a **`probeCursor`** (last partition proven empty) makes the cap
  progressive rather than a livelock; the sync-socket `connected` gate applies to
  `PartitionPolicy` only; `ApiPolicy` gates on a **bounded backoff** (`errorCount`,
  `nextRetryAt`) that clears on success or explicit user retry — never a latched error —
  accepts keyset cursors or, in `api-offset` mode, offsets (degraded: the id set removes
  duplicates, skips remain possible under live inserts — documented, T42) [DX]; `serverFloor`
  is optional and the static `floor` is the default [DX];
  and treats `onlineHint() === false` (§9 — `navigator.onLine` on web, NetInfo on React
  Native) as a hint that shortens the attempt (short-timeout request), not a skip; empty
  partitions are unsubscribed immediately. Tests: T20, T29, T37.
- **R17 — SDK is the source of truth for streams [R2,F2-010].** Provenance and accounting
  read `SyncStatus.syncStreams` / `forStream()` (`active`, `expiresAt`, `hasSynced`,
  `lastSyncedAt`) — never bridge-local guesses (TTL is refcounted across tabs). The bridge
  **never calls `unsubscribeAll()`** (it resets TTL for every tab).

### 5.4 `measureRace()` — the value probe [R1,F-012]

Ships in v0 core. Wraps any raced definition and logs per invocation:
`{ winner, localMs, apiMs, sessionFreshnessMs, rowsRendered, source }` to an injectable sink
(console in dev; the app's telemetry in prod). `sessionFreshnessMs` measures from
`connect()` to the first in-session `localFreshAt` — *not* to a persisted `hasSynced`, which
would always report ~0 ms on warm DBs. Purpose: adopters verify V1/V2 (§1) on *their* app
before enabling the API leg — and keep evidence as the SDK's query path improves (§2.2).
No data leaves the device unless the app wires a sink.

---

## 6. Primitive 2 — `infiniteList`

### 6.1 Contract [R1, F-003, F-014, F-022, R2, F2-006, R3, F3-010]

```ts
type KeyOf<T> = keyof T & string;
type OrderKey<T> = KeyOf<T> | readonly [KeyOf<T>, KeyOf<T>];   // scalar or 2-tuple
type CursorOf<T, K> = K extends readonly [infer A extends KeyOf<T>, infer B extends KeyOf<T>]
  ? [T[A], T[B]] : K extends KeyOf<T> ? T[K] : never;          // conditional: TItem[K] is
                                                               // ill-formed for tuple K [F3-010]

defineInfiniteList<TItem, K extends OrderKey<TItem> = 'id'>({
  pageSize,
  query: ({ cursor, limit }: PageArgs<TCursor>, scope: TScope) => OrmSelect<TItem>,
                        // page FIRST (scope-less lists omit the 2nd param); annotate the page
                        // argument with `Cursor<TItem, K>` = CursorOf<TItem, K> | undefined —
                        // every cursor position carries that one type, so TCursor infers
  orderKey: K,
  initialCursor?: TCursor,
  escalate: PartitionPolicy<TCursor> | ApiPolicy<TCursor, TItem> | ApiOffsetPolicy<TItem>,
  isInWindow?: (row: TItem) => boolean,               // required for predicate windows (§6.4)
  watch?: string[],                                   // OVERRIDE only; derived by default [F-022]
})
// PartitionPolicy: equality partitions — the ONLY stream-expressible escalation [F-003]
//   kind: 'partition'
//   stream: string                          // e.g. 'sales_month' [DX]
//   partitionOf(cursor: TCursor): TPartition | null      // TCursor includes undefined: the
//                                                        // first page maps to the current month
//   nextPartition(p: TPartition): TPartition | null
//   partitionParams(p: TPartition): Params  // → subscription.parameter(...) [DX]
//   floor: TPartition                       // static floor; always required (§6.2)
//   serverFloor?(): Promise<TPartition | null>   // OPTIONAL exact floor (R16) [DX]
//   ttl?: number; partitionCap?: number     // defaults 300 s, 3 (§6.5, R16)
// ApiPolicy: page-granular escalation
//   kind: 'api'
//   fetchPage(cursor: TCursor | OpaqueCursor, signal): Promise<TItem[]>
//   // OpaqueCursor: base64-encoded tuple — the API never unpacks it (§12.1) [R2, F2-006]
//   kind: 'api-offset'                      // DEGRADED MODE for offset-only APIs [DX]
//   fetchPage(offset: number, signal): Promise<TItem[]>
//   // offset = rows loaded so far; the global id set drops duplicates, so the only
//   // symptom under live inserts is an occasional skipped row. Documented, tested (T42).
```

`pageSize` governs **rendering**, not sync volume. The keyset predicate for a tuple key is
`(a > c1) OR (a = c1 AND b > c2)` — no skips/duplicates at ties (test T19). **No partition
column is required [DX]:** the stream computes the bucket from an existing timestamp
(`substring(sold_at::text, 1, 7) = subscription.parameter('month')`, per PowerSync's
"buckets per date" pattern); `partitionOf` applies the same slice client-side.

```ts
useInfiniteList(SalesHistory, { orgId }) => {                           // [DX] return shape
  items: PagedRow<TItem>[];
  loadMore(): void;
  boundary: 'more' | 'end' | 'unknown';
  boundaryReason?: 'offline' | 'timeout' | 'error' | 'partition-cap';
  awaitingSync: boolean;
  errors: { local?: Error; api?: Error; stream?: Error };
}
```

### 6.2 Engine (pseudo-code) [R1, F-003, F-006, F-007, F-017, F-021, R2, F2-007, F2-011]

```
state: idSet (source of truth [F-017]); cursor (high-water mark of LOADED keys);
       probeCursor (last partition proven empty — survives across loadMore calls,
                    reset only by a newer server floor or a row absorb  [R3,F3-006]);
       boundary: 'more' | 'end' | 'unknown'; unknownReason?: 'offline'|'timeout'|'error'
                                                 |'partition-cap'

loadMore():
  PartitionPolicy gate: !driver.syncStatus().connected →                    [R3,F3-008]
     boundary='unknown' (offline); return          // 'connected' is the SYNC socket
  ApiPolicy gate: bounded backoff (errorCount, nextRetryAt — clears on       [R4,F4-007]
     success or user retry); onlineHint()===false is a HINT that       [R6,F6-003]
     shortens the attempt (short-timeout request), never a skip →
     else boundary='unknown' (offline); return       // sync-down ≠ HTTP-down [R3,F3-008]

  page = runLocal({ keyset > cursor, limit: pageSize + 1 })   // lookahead row
  if page.length > pageSize: absorb(page); boundary='more'; done

  // ambiguous boundary: end of server data, or end of SYNCED window?
  PartitionPolicy:
    if (probeCursor != null) {                       // resume AFTER the last proven-empty [R5,F5-005]
      p = nextPartition(probeCursor)
      if (p == null): boundary='end'; done           // all partitions proven empty —
    } else {                                          // never re-probe the cursor's own
      p = partitionOf(cursor)                         // exhausted partition
    }
    cap = 0
    loop:
      if beyond serverFloor(p): boundary='end'; done         // server floor first [R16]
      if p < staticFloor or p == null: boundary='end'; done  // static fallback [F-003]
      if cap++ == partitionCap (default 3):
         boundary='unknown' (partition-cap); return          // never binge [R16]
         // probeCursor is PERSISTED, so the next loadMore resumes from
         // nextPartition(probeCursor) — re-probing the same empties is a livelock [R3,F3-006]
      sub = driver.syncStream(stream, partitionParams(p), { ttl: defaultTtl })
      // SDK signature is waitForFirstSync(abort?) — the DRIVER wraps:
      //   AbortSignal.timeout(timeoutMs) ∩ leg epoch signal      [R2,F2-011]
      aborted → boundary='unknown' (timeout); unsubscribe(sub); return
      firstSync = await sub.waitForFirstSync(wrappedSignal)
      rows = runLocal({ floor(key) <= key <= cursor_high })    // FULL loaded range [F-006]
      if rows landed beyond cursor: absorb; probeCursor=null; boundary='more'; done
      else: probeCursor = p;                                   // proven empty [R3,F3-006]
            unsubscribe(sub) IMMEDIATELY (empty partitions hold buckets for nothing [R16])
            p = nextPartition(p); continue              // empty partition ≠ end

  ApiPolicy:
    try rows = await fetchPage(cursor, signal)
    catch offline/abort → boundary='unknown' (reason)      // NEVER 'end' [F-007]
    rows empty && online → boundary='end'
    else absorb(rows, origin 'api'); boundary='more'

absorb(rows): idSet ← rows; renderedWindow re-derived by re-running the FULL loaded
  key-range query via differentialWatch diffs — late-landing partitions (out-of-order
  first-syncs) can never fall behind the cursor.                          [F-006]
```

**Boundary taxonomy [R1,F-007,F-021]:** `'end'` requires positive knowledge — the
**server-provided floor** reached (preferred; documented as a **lower bound** [R3,F3-013] —
backfills can introduce rows below it, so on `'end'` the floor is **re-validated once**
before the terminal state is trusted), the static floor reached, or an actually-empty
online API page, each with the owning sync completed. Anything else is `'more'` or
`'unknown'` + reason. The hook surfaces `awaitingSync` and `boundaryReason`.

### 6.3 The subset is pluggable — but the key is not [R1, F-014, R2, F2-006]

Requirements for a boundary:

1. a **unique monotonic order key** — a scalar (uuidv7 / integer serial / ISO-8601 UTC
   string) **or an explicit 2-tuple** (`(created_at, id)`); bare `created_at` is NOT
   acceptable (ties skip/duplicate under keyset pagination);
2. a **boundary predicate**: **partitionable windows** (current + N months via signed
   claims, ISO weeks, days) → `PartitionPolicy`; **predicate windows** (newest-100, status
   filter) → must supply `isInWindow` (§6.4);
3. an **escalation mapping**: partitions for streams (equality-only params — range operators
   are disallowed on stream parameters; the partition is **computed in the stream query**
   from an existing column, never a required schema column [DX]), opaque-cursor API pages,
   or offset API pages in the degraded `api-offset` mode.

The type system enforces (1) — `OrderKey<T>` admits the tuple; "docs enforce the rest" was
a round-1 gap, closed in round 2 (F2-006).

### 6.4 Retention: the four-way removal taxonomy [R1, F-004, F-016, R2, F2-008, F2-010, F2-017, R3, F3-005, F3-013]

A row can vanish from a diff in **four** ways that surface identically at the watch level:
server DELETE, TTL expiry, window slide (no longer matches the stream), and **query
filter-out** (row still exists locally, no longer matches *the list query* — e.g. status
OPEN→CLOSED in an "open orders" list). Classification is a pipeline, per removed id:

```
on removal(id):
  1. PRESENCE PROBE: SELECT id FROM t WHERE id IN (…) chunked ≤ 500
                — ONE batched statement per emission [R4,F4-006]
       present  → FILTERED-OUT: drop from the list, NO tombstone. The row is real; a
                  fresher snapshot or a scope change may legitimately show it again.
                  (The probe is also the implementation of presentLocally for R14.)
       absent   → continue:
  2. provenance: which (stream, params) produced this row? Read from the SDK —
     SyncStatus.syncStreams / forStream() (`active`, `expiresAt`) — never bridge
     bookkeeping (TTL is refcounted across tabs)                        [R2,F2-010]
       stream still tracked AND now < expiresAt  → DELETE: removed from the id set,
                  tombstoned for this list instance.
                  // NOT 'active': after unsubscribe the data "continues syncing for the
                  // TTL duration" — a server DELETE landing in that window is a delete,
                  // not an eviction.                                    [R3,F3-005]
       stream gone OR now ≥ expiresAt            → EVICTION: retained in the overlay for
                  the **mount lifetime** [F-016], with
                  origin:'local', presentLocally:false [R2,F2-008] — visible, not writable
                  (R14). onEvicted(ids[]) fires ONCE PER EMISSION (a TTL expiry dropping a
                  partition is one batched call) [R3,F3-013]. The retention window is a
                  documented **zombie period**: a server DELETE during retention is not
                  observed until re-subscription or unmount.
```

- **Predicate windows** (`newest-N`) skip step 2 and use `isInWindow(row)`: an
  **in-window** row disappearing (post-probe) = DELETE; **out-of-window** = eviction.
  **Hysteresis [R2,F2-017]:** rows within ± skew of the boundary are treated as
  out-of-window (client clock vs server window); a **throwing predicate** is treated as
  "unknown → retain" — never as a delete.

Required test: T7.

### 6.5 Subscription & bucket accounting [R1, F-005, R2, F2-007, F2-010]

The documented cap is ~1,000 buckets/user/connection (one bucket per unique
(stream, params)).

- **`ttl` is the primary knob** (default **5 min** for bridge-owned escalation
  subscriptions). TTL is **refcounted across tabs** and starts only after the last
  subscription with those params unsubscribes — including other tabs' (SDK source,
  `sync-streams.ts`). "First TTL wins" remains a documented hazard.
- **Accounting reads `SyncStatus.syncStreams`** (per-stream `active`, `expiresAt`) — not
  bridge-local registries. The engine counts active + TTL-pending streams and warns at a
  configurable ceiling (default 500).
- **The bridge never calls `unsubscribeAll()`** — it resets TTL for every tab [R17].
- `keepPagesAlive` (default 3) bounds only *bridge-active* subscriptions and the rendered
  window — never bucket pressure.
- Loop bounds (R16) prevent the sparse-data blow-up: a conservative static floor + empty
  partitions would otherwise subscribe hundreds of buckets and burn `timeoutMs` per
  partition offline. Test: T11, T20.

---

## 7. Primitive 3 (optional) — `autoSyncFlag` [R1, F-015][DX]

**Not part of a typical setup.** The default way to pull one arbitrary row into the offline
database is a plain on-demand stream with an equality parameter — no server table, no
endpoint:

```yaml
sale_by_id:
  query: SELECT * FROM sales WHERE org_id = auth.parameter('org_id') AND id = subscription.parameter('sale_id')
```

```ts
const { status } = useOnDemandStream('sale_by_id', { sale_id }, { ttl: 300 });   // [DX]
// status: 'subscribing' | 'syncing' | 'synced' | 'timeout' | 'offline'
// held while mounted; the TTL keeps rows warm after unmount; driver-wrapped waitForFirstSync
```

Reach for `autoSyncFlag` only when one of two things is true: (1) **bucket economy** — every
`(stream, params)` subscription is one bucket against the ~1,000/user cap, so a user who opens
hundreds of records one by one should get them in ONE per-user bucket; (2) **server-decided
membership** — the set cannot be written as an equality parameter ("everything related to
this case"). Otherwise skip this section entirely.

Client-triggered server-side flagging of rows to sync, as a UX state machine over a
pluggable backend call:

```ts
createAutoSyncFlagger({
  requestFlag,        // user-supplied: (scope) => Promise<void>  — e.g. POST /sync/flag
  stream,             // stream whose JOIN reads the sync_requests table
  timeoutMs,
})
// states: idle → flagging → syncing → synced | timeout | error
// timeout ≠ failure: keep the request alive, retry on reconnect
```

**Server contract (per-(user, row) — a row-level flag column is a cross-user side channel
and a bucket-growth leak [F-015]):**

```sql
CREATE TABLE sync_requests (
  user_id      text NOT NULL,
  row_id       text NOT NULL,
  requested_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, row_id)
);
-- stream WHERE: JOIN sync_requests sr
--   ON sr.row_id = t.id AND sr.user_id = auth.user_id()
```

Why not `UPDATE t SET sync_requested_at = now()`: (a) any other user whose stream filters on
that column would receive the flagged row; (b) flag flips generate permanent REMOVE/ADD
churn until bucket defragmentation (discussion powersync-ja#445). The contract requires an
**expiry job** (e.g. purge `sync_requests` older than 24 h) as part of the backend recipe.

**Synced detection:** the state machine determines `synced` by **watching the specific
requested row ids** locally, *not* by `waitForFirstSync()` (which covers a subscription's
initial sync only) and *not* by persisted `hasSynced` [R2].

---

## 8. Type system architecture

### 8.1 DB-first, query-inferred [R1, F-010, R2, F2-014]

**The local schema remains the source of truth; the query is the inference point.** Queries
are built from the Drizzle table (so schema types flow end-to-end), but `TItem` is inferred
from the **query's decoded result type only** — a column-subset or joined select is then a
legal `TItem`, and `table` becomes **optional** (used solely for `toDbRow` and write
guards):

```ts
import { orders } from './db/schema';
defineList({
  query: (db) => db.select().from(orders).orderBy(desc(orders.id)).limit(50), // TItem ← result
  table: orders,      // optional: toDbRow + write guards
});
```

**Truth rule [R1,F-010]:** rows are decoded by the ORM's result mapper (mode-typed columns:
`timestamp → Date`, `boolean → boolean`, `json → parsed`, `numeric → string`); adapters
execute through the ORM and hand core decoded `TItem[]` (§9). A raw row never crosses a
public boundary (`RawRow = never`).

### 8.2 Assertion-free API decoding

The raw HTTP body is untyped at runtime; **raw types must be schema-derived, never
`Record<string, unknown>`**:

```ts
const OrderApi = z.object({ id: z.string(), total: z.string(), status: z.enum(['OPEN','CLOSED']) });
type  ApiOrder  = z.infer<typeof OrderApi>;

mapApi: (o: ApiOrder): Order => ({ id: o.id, total: o.total, status: o.status });
//                    ^^^^^^ return annotation = compile check against TItem
```

- Accepted raw-type sources: Zod schema, or OpenAPI codegen (Go oapi-codegen → TS). Without
  one, assertion-free + type-safe is *not* achievable; schema-derived raw types are the
  documented floor.
- **JSON columns [R1,F-010]:** `.$type<Shape>()` is compile-only; `mapApi` validates JSON
  payloads at the decode boundary.
- **Null/undefined normalization [R1,F-024]:** the Drizzle adapter ships
  `toDbRow<T>(table, patch)` — normalizes `undefined → null` for declared-nullable columns,
  typed from the table.

### 8.3 Generic chain (reviewer: verify no escape hatches) [R1, F-010, F-014, R2, F2-006]

```
TItem   ← inferred from the query's decoded result type (§8.1)
K       ← orderKey: OrderKey<TItem> = KeyOf<TItem> | readonly [KeyOf<TItem>, KeyOf<TItem>]
TCursor ← CursorOf<TItem, K>  (conditional type — see §6.1; never TItem[K] on a tuple)
PagedRow<T> = { row: T; origin: RowOrigin; presentLocally: boolean; stale?: true }
hook return → items: PagedRow<TItem>[] → renderItem fully typed
```

- **No `any`/`unknown` in public signatures.** The only `unknown` allowed is the *inbound*
  raw payload of a decode function, immediately narrowed by a schema type (§8.2).
- **`RawRow` is `never` in public surface** [R1,F-010].
- **Composite keys are first-class** [R2,F2-006]: keyset predicate
  `(a > c1) OR (a = c1 AND b > c2)`; API cursors are opaque base64 tuples.
- **`orderKey` must order identically in Postgres and SQLite:** uuidv7, integers,
  ISO-8601 UTC strings are safe — enforced by a CI ordering fixture in the Go example
  (§12.1), since client-side tests cannot prove server collation [R2,F2-015]. The two
  common traps and their one-line fixes [DX]: **text** — Postgres locale collation differs
  from SQLite `BINARY`; use `ORDER BY col COLLATE "C"` on the API side (no sort column
  needed); **decimals** — PowerSync delivers `numeric` as text, so declare the column as
  `column.real` in the *client* schema to sort numerically (server schema untouched).
- **`id` is always the implicit tiebreaker** [DX]: `orderKey: 'soldAt'` is treated as
  `['soldAt', 'id']`; a tuple is only written explicitly when the second column is not `id`.
  uuidv7 ids are recommended, not required — any unique `id` works.

---

## 9. Driver / adapter architecture [R1, F-001, F-002, F-009, F-010, F-022, R2, F2-004, F2-009, F2-011]

Core is framework-agnostic and speaks **decoded rows + prepared queries** only:

```ts
interface PreparedQuery<T> {
  tables: string[];                         // derived by the adapter from the builder [F-022]
  run(signal?: AbortSignal): Promise<T[]>;  // EXECUTED BY THE ORM — decoded rows [F-010]
}

type PendingOpRef = { table: string; id: string; op: 'INSERT' | 'UPDATE' | 'DELETE' };

interface BridgeDriver {
  query<T>(q: PreparedQuery<T>): Promise<T[]>;
  diffs<T>(q: PreparedQuery<T>, opts?: { throttleMs?: number }): DifferentialStream<T>;
                                // IMPLEMENTATION NOTE (v0): the SDK's differentialWatch accepts
                                // raw SQL + a per-row mapper only, which cannot decode joined
                                // ORM rows (result columns collide; Drizzle maps positionally).
                                // The PowerSync driver listens with onChange(tables) and loads
                                // DECODED rows through the adapter, diffing by id once per
                                // change — the SDK's single O(n) pass is REPLACED, not
                                // duplicated (§11). Delegate to differentialWatch once it
                                // accepts compilable queries.
  pendingUploadIds(): Promise<Set<PendingOpRef>>;
  // getCrudBatch() iterated until haveMore === false (default batch limit is 100),
  // or a direct ps_crud read; keyed (table, id). [R2, F2-004]
  // The driver DIFFS CONSECUTIVE ps_crud snapshots so COMPLETIONS are derivable:
  // complete() drains the table and change listeners carry no payload. [R4, F4-001]
  onCrudChange(cb: (delta: { added: PendingOpRef[]; completed: PendingOpRef[] }) => void):
      () => void;
  // the driver diffs consecutive ps_crud snapshots and EMITS the delta — core never
  // re-diffs. [R4, F4-001][R5, F5-003]
  presentIds(table: string, ids: string[]): Promise<Set<string>>;
  // batched presence probe backing §6.4/R14 — core cannot issue raw SQL (it never
  // imports a query builder). [R5, F5-003]
  onlineHint(): boolean | undefined;
  // navigator.onLine on web; NetInfo on React Native; undefined when unknown.
  // A HINT for the ApiPolicy gate (R16), never a skip. [R5, F5-003]
  // Pending re-poll triggers: every local diff emission + statusChanged (multi-tab:
  // another tab's write lands in the SHARED ps_crud). [R3, F3-012][R4, F4-010]
  syncStream(name: string, params?: Params, opts?: { ttl?: Duration }): StreamHandle;
  // StreamHandle: waitForFirstSync() — driver wraps the SDK's abort-only signature with
  // AbortSignal.timeout ∩ epoch signal [F2-011]; exposes description via SyncStatus.
  syncStatus(): { connected: boolean;
                  downloading: boolean;                // freshness stamps [F3-002, F4-003]
                  hasSynced: boolean | undefined;      // persisted; true→false = clear [F4-009]
                  lastSyncedAt?: Date;
                  forStream(desc): { active: boolean; expiresAt: Date | null;
                                     hasSynced: boolean; lastSyncedAt: Date | null };
                  statusForPriority(p): { hasSynced: boolean; lastSyncedAt?: Date } };
  onStatusChanged(cb: () => void): () => void;
  identityKey(): string;        // fed by bridge.wrapConnector() — never by the app [DX]
  onIdentityChange(cb: () => void): () => void;
}
```

**Identity is derived, never supplied [DX][R15].** `createBridge` runs in a plain module
before login and is not a hook, so it must never be handed claims. The PowerSync connector
already returns the JWT on every connect and refresh; the bridge wraps it:

```ts
await db.connect(bridge.wrapConnector(connector));

// inside core — the app never writes this
wrapConnector(c) {
  return { ...c, fetchCredentials: async () => {
    const creds = await c.fetchCredentials();
    if (creds?.token) this.identity.observe(identityKeyFromToken(creds.token, this.denyList));
    return creds;   // same key → no-op (refreshes never churn); new key → epoch swap (R15)
  } };
}
identityKeyFromToken(token, denyList) =
  canonical JSON of the JWT payload minus the deny-list, sorted by claim name   // a key, not a hash
```

Deny-list default: OIDC `exp/iat/nbf/jti/auth_time/at_hash/nonce/azp` + vendor session
claims `sid`/`session_id`/`rat`; connection parameters opted in by key. Logout is observed
as `hasSynced` flipping `true → false` (the effect of `disconnectAndClear()`). Escape hatch
for scope that is not in the token: `bridge.setIdentity({ userId, orgId })`, called from the
app's auth flow. Adapters (~thin):
`@powersync-bridge/drizzle` — executes builders via the ORM
(`toCompilableQuery`-style), so result mappers decode; derives `tables` from builder
metadata; `.../kysely`, `.../raw` later, each owning decode. Core never imports a query
builder; adapters never contain engine logic; **core never touches raw rows**.

---

## 10. Security model & invariants

1. **Selection ≠ authorization.** Every stream query the docs/examples ship MUST gate on
   signed auth parameters (`auth.parameter(...)`, `auth.user_id()`). [R1,F-023] No
   name-based lint; instead a docs checklist **and** an optional CI helper
   (`check-sync-streams`) that parses the *app's* stream YAML and asserts every
   `subscription.parameter` use is gated by `auth.`.
2. **API leg is a first-class auth surface — and a consistency surface [R3,F3-004][R4,F4-002].**
   Same tenant scope and permission checks as the sync streams; the package never fetches
   with ambient credentials the app didn't configure. Documented contract: "the API returns
   the same tenant scope the stream would." **Consistency:** the default
   read-your-writes guard is **client-derived** — `recentlyAcked` (§5.2) shields acked
   writes until the first checkpoint that carries them, with the ownership-drop rule
   closing the deleted-row gap. The optional **`asOf` field is an optimization, not a
   safety requirement**: the snapshot echoes the **PowerSync write checkpoint** the backend
   observed when serving (comparable client-side against the checkpoint the client has
   synced past — `CrudBatch.complete(writeCheckpoint?)` is the carrier; LSNs are
   backend-internal and never cross the client boundary [R4,F4-011]). Without `asOf`,
   correctness rests on the client-derived shields; with it, freshness comparisons become
   exact.
3. **Writes are presence-gated [R2,F2-008].** The write path accepts only rows with
   `presentLocally: true` — regardless of `origin`. Retained evicted rows
   (`presentLocally: false`) and api-only rows are not writable; tombstoned ids are
   excluded from re-insertion. (A PowerSync UPDATE on a missing row silently affects 0
   rows — the guard exists because that failure is invisible.)
4. **No secrets in caches/telemetry.** Any v1 persistence stores only row data the app
   already synced/fetched, never credentials/tokens. `measureRace()` logs timings and
   counts, never row contents.
5. **Multi-tenant hygiene = identity epochs [R1,F-009][R2,F2-009][R4,F4-004,F4-008,F4-009].**
   `identityKey()` (§9) is part of **every** internal cache key and every TanStack
   `queryKey` the React adapter builds. Because scope can change *without* the user
   changing (org switch, signed window claims, connection parameters), identityKey is the
   deny-listed canonical claim key ⊕ opted-in connection parameters ⊕ clear events
   (`hasSynced` true→false/undefined), **derived by the bridge from the connector's JWT
   via `wrapConnector()` — the app never passes claims** [DX] — see R15. Teardown on
   change: **the epoch bump and the store-root swap to an empty `'pending'` snapshot are
   ONE synchronous operation** (single notification — no committed frame can render the
   outgoing tenant), then asynchronously: unsubscribe bridge-held stream handles → tear
   down `diffs()` subscriptions → cancel in-flight API legs (signal) → start the new
   epoch's legs. A stale-tenant reconciliation or a surviving tenant-A subscription under
   tenant-B's token is a data leak; the contract makes both structurally impossible.
   Tests: T8, T30, T35.
6. **Supply chain:** zero required runtime deps for core (peer-deps on `@powersync/common`
   / React only in the React adapter; zod optional peer).

---

## 11. Platform matrix & performance budgets [R1, F-020, R2, F2-013, F2-016]

| Platform | Local stack | Race policy (default) | Notes |
|---|---|---|---|
| Web (Chromium/Firefox) | WASM SQLite (wa-sqlite), SharedWorker multi-tab, IndexedDB VFS | **race** (API leg on) | Headline use case (§2); warm-tab probe skips the API leg when the DB is already open elsewhere (v0.1) |
| Web (Safari) | WASM SQLite; multi-tab **off by default**; OPFS sync handles unavailable in SharedWorker | race, gated by warm-tab probe | Page-kill fragility: #1081 (unmitigable) |
| Web (RNW) | same as web | race | shares web code path |
| iOS/Android native | op-sqlite / expo-sqlite | local-first gate | local is warm & fresh; race = wasted request |
| Capacitor | native sqlite plugin | configurable | first-sync stalls are transport (#1063), not arbitration |

Shared-worker multi-tab is **Chromium/Firefox**; Safari runs per-tab with multi-tab sync
disabled by default (PowerSync blog, May 2026). Warm-tab probe: `navigator.locks`/
`BroadcastChannel` → "DB already open in another tab" → skip API leg [R1,F-020].
**Cross-tab pending correctness** [R2,F2-016][R3,F3-012][R4,F4-010]: in shared-worker mode
another tab's write lands in the shared `ps_crud`; the driver re-reads
`pendingUploadIds()` on **every local diff emission** (the write that matters always
produces one, in any tab) and on `statusChanged` — removing any dependency on
internal-table event propagation (T21).

**Budgets (reviewer: judge against these):**
- **The bridge adds no second O(n) pass** [R2,F2-013]. Honest pipeline note: the SDK's
  differential watch itself re-runs the query and compares all rows per dependent-table
  change — that O(n) is inside the SDK and outside our budget. The bridge's own cost is
  O(changes) diff application; per-row work happens once, in the SDK.
- Exactly **one throttle layer**: the SDK watch's `throttleMs`; notification batching is
  `useSyncExternalStore`'s (synchronous — document that 10k-row diff applications block,
  so **windowed/virtualized rendering is a documented requirement** for large lists).
- Freshness gating is per-stream / per-priority (`statusForPriority`) — a priority-1 list
  never waits on priority-3 data [R2,F2-013].
- React: zero re-renders when a diff changes nothing; id-keyed row identity preserved.
  **Flicker is an observable, tested property** [R2,F2-015]: "a rendered row id whose
  displayed value changes more than once between first paint and convergence with no
  intervening local write."
- Bundle: core < 10 kB gzip; React adapter < 5 kB; TanStack is a peer, not bundled.

---

## 12. Package layout, tests & release strategy

### 12.1 Layout

```
packages/
  core/            # engine: epochs, freshness, arbitration, tombstones, partitions,
                   #         provenance (via syncStreams), identity, probe
  react/           # BridgeProvider, useRacedList, useInfiniteList, useOnDemandStream,
                   # useAutoSync (optional) — TanStack peer, uSES-based
  adapters/
    drizzle/ kysely/ raw/        # each owns ORM execution + decode + table derivation
  presets/         # timeWindow(), newestN(), flagged() boundary predicates
examples/
  react-web-go/    # Go backend contract (decided round 1 [F-025], refined rounds 2–3):
                   #   keyset pages — opaque base64 tuple cursor, same orderKey as local;
                   #   server VALIDATES cursors (decode, shape, operand types) before
                   #   SQL binding [F3-010]; optional `since` snapshot param;
                   #   `?offset=` accepted in api-offset mode [DX];
                   #   OPTIONAL floor endpoint (`to_char(min(sold_at),'YYYY-MM')`) — a signed
                   #   claim goes stale on backfill/import [F3-013]; static floor default [DX];
                   #   sync_streams.yaml sample computes the month with substring() —
                   #   no partition column [DX];
                   #   list endpoints read from the primary, or echo `asOf` (§10.2) [F3-004];
                   #   CI fixture asserting Postgres vs SQLite ordering agreement.
```

### 12.2 Required tests (invariants made executable)

| # | Proves | Source |
|---|---|---|
| T1 | Pending-DELETE id in API snapshot never resurrects | R2 / F-002 |
| T2 | Warm-remount freshness: stale checkpoint never wins a conflict; convergence is one-directional — **flicker = a rendered id whose value changes more than once per distinct fresher input** (a legitimately fresher snapshot may flip a row once; oscillation is the bug) | R3, R9 / F2-002, F3-011 |
| T3 | Suspect local-only rows quarantined only by a *fresh* snapshot; pre-snapshot rows render `stale: true` (or blank under `holdPending: 'pending'`) | R4 / F-001, F5-006 |
| T4 | Empty partition does not produce `boundary='end'`; server floor does; static floor as fallback | §6.2 / F-003 |
| T5 | Offline/timeout escalation yields `unknown` + reason, never `end` | §6.2 / F-007 |
| T6 | Out-of-order partition first-syncs never leave a gap (full-range re-derive) | §6.2 / F-006 |
| T7 | Eviction (expired sub per `syncStreams`) retained as `presentLocally:false`; in-window disappearance treated as delete; `onEvicted` fires; write guard blocks retained rows | §6.4 / F-004, F2-008 |
| T8 | Identity switch, **epoch-first teardown**: an in-flight `diffs()` emission arriving mid-teardown is discarded; stream handles unsubscribed; the WHOLE per-epoch root (incl. `pendingSince`, `recentlyAcked`, tombstones, `probeCursor`) is swapped; no cross-tenant row ever renders — an integer-id `(table, id)` collision across tenants carries no shield | §10.5 / F2-009, F3-007, F4-004, F5-004 |
| T9 | StrictMode double-mount produces one epoch-clean store | R10 / F-018 |
| T10 | Late result from a cancelled/superseded leg is discarded | R10 / F-013 |
| T11 | TTL-pending accounting (via `syncStreams`) warns at ceiling; `keepPagesAlive` alone does not bound buckets; `unsubscribeAll` never called | §6.5 / F-005, F2-010 |
| T12 | raced `limit` truncation + below-cutoff drop at ownership | R7 / F-008 |
| T13 | Adapter-decoded rows satisfy `$inferSelect` — asserted at **runtime** via a `drizzle-zod`-style generated guard (a type assertion alone is untestable) | §8.1 / F-010, F2-015 |
| T14 | `check-sync-streams` flags ungated `subscription.parameter` in sample YAML | §10.1 / F-023 |
| T15 | Upload of an UPDATE completes between `requestedAt` and reconcile → local value still wins (pendingSince stickiness) | R1 / F2-001 |
| T16 | Upload of a DELETE completes mid-flight → row stays suppressed for the snapshot's lifetime | R2 / F2-001 |
| T17 | API leg fails pre-snapshot → local rows render `stale: true`; never a blank list | R12 / F2-003 |
| T18 | >100 pending ops: full `ps_crud` read (iterate `haveMore`), `(table,id)` keying — integer ids across tables don't collide | §9 / F2-004 |
| T19 | Tie-heavy tuple key (`created_at, id`) pages produce no skips/duplicates; opaque cursor round-trips | §6.1 / F2-006 |
| T20 | Sparse data: 100 empty partitions → partition-cap `unknown` after 3; no bucket-ceiling breach; **sync-socket-down** short-circuits PartitionPolicy; ApiPolicy proceeds when only HTTP is up | R16 / F2-007, F3-008 |
| T21 | **Browser E2E (Playwright)** — two tabs, shared worker: write in tab B visible in tab A's pending set; a **unit-level stub test** of the diff-emission re-read path runs beside it | §9, §11 / F2-016, F3-012 |
| T22 | Optimistic edit made **after** the snapshot was consumed still wins pre-ownership (live ∪ window union) | R1 / F3-001 |
| T23 | Snapshot reused across remounts (staleTime window): an edit + completed upload between mounts is not reverted by the reused snapshot | R1, R9 / F3-001 |
| T24 | Snapshot fetched **mid-download** (fetchedAt > stamp, data older) does not win the conflict; no fresh→old→fresh oscillation — **parametrized**: transition observed / not observed (boot mid-download) / continuous downloading | R3 / F3-002, F4-003 |
| T25 | Service restart resets `lastSyncedAt` to null mid-session → `localFreshAt` does not regress; local rows are not demoted | R5 / F3-003 |
| T26 | Reconnect: `sessionConnectAt` is the first connect of the epoch; a pre-blip checkpoint keeps its freshness standing | R5 / F3-003 |
| T27 | Row filtered out of the query (still present locally) is dropped **without** tombstone; a later snapshot showing it again is not suppressed | §6.4 / F3-005 |
| T28 | Server DELETE landing in the TTL-pending window (unsubscribed, pre-`expiresAt`) is classified DELETE, not eviction-zombie | §6.4 / F3-005 |
| T29 | After a `partition-cap` return, the next `loadMore` resumes from `probeCursor` and **makes progress** across consecutive calls; once all partitions are proven empty, `'end'` fires with **no subscription issued** | R16 / F3-006, F5-005 |
| T30 | `diffs()` emission for the outgoing tenant arriving mid-teardown is discarded (epoch-first ordering) | R15 / F3-007 |
| T31 | Op pending **before** `requestedAt` completes mid-flight (reconnect flush) → still shielded by the seeded `pendingSince(S)` | R1 / F4-001 |
| T32 | Delete acked **before** `requestedAt` — and **before the list mounts** — + lagging replica → the identity-scoped `recentlyAcked` (subscribed from engine init) shields it; post-ownership, never-seen-locally api-only ids drop (or persist under `apiScopeSupersetOfLocal`) | R1, R6 / F4-002, F6-001 |
| T33 | Bridge boots mid-download (no transition observed) → provisional stamp set, ownership transfers | R3 / F4-003 |
| T34 | Continuous downloading (never idles) → stamp advances on every checkpoint completion | R3 / F4-003 |
| T35 | No committed frame after the atomic epoch-bump-and-root-swap can observe outgoing-tenant rows (they are one operation) | R15 / F4-004 |
| T36 | 5k-row TTL expiry: presence probe is one batched chunked statement per emission; worker not blocked by per-row round-trips | R14 / F4-006 |
| T37 | `ApiPolicy` backoff clears on success and on user retry; `onLine===false` (or `onlineHint()`) shortens the attempt but does not skip it | R16 / F4-007 |
| T38 | Ack lands during a download that started **before** it → `recentlyAcked` holds until a checkpoint with `stamp ≥ ackTime` completes (or past the `asOf` write checkpoint); the intervening completion does NOT retire the shield | R1 / F5-001 |
| T39 | Native gate: no API leg is issued when the platform gate resolves local-first | R8 / F5-006 |
| T40 | Raced page 1 + escalation pages ≥ 2 share one id set — no duplicate ids render | R13 / F5-006 |
| T41 | `nextPartition(probeCursor) === null` → `'end'` without re-probing the cursor's own partition | R16 / F5-005 |
| T42 | `api-offset` mode: duplicates from shifted offsets are dropped by the id set; a skipped row under live inserts is reported, never a crash or duplicate | R16 / DX |
| T43 | `wrapConnector`: identity derived from the JWT; a token refresh with the same stable claims causes no epoch swap; an org-switch token swaps the epoch; `setIdentity()` overrides | R15 / DX |
| T44 | `useOnDemandStream`: subscribes on mount, holds while mounted, reports `synced` via the driver-wrapped `waitForFirstSync`, `timeout`/`offline` via the gate; unsubscribes on unmount (TTL keeps rows warm) | §7 / DX |
| — | **Static assertions:** no `useQuery` import in the React adapter (R11); no deep-equal comparator in core (R6) | R6, R11 / F5-006 |

### 12.3 Release

Changesets, semver, `@alpha` tags for race semantics until validated; docs site = §2/§5/§6/§7
rewritten user-first. Open-source path: standalone npm → demo PR to powersync-js/demos →
docs recipe proposal → (if adopted) promote. Phasing: v0 alpha = core + raced + infinite +
probe; v0.1 = warm-tab probe + kysely adapter; v1 = React GA + example app.

---

## 13. Reference implementation pointers (production precedent)

The author's production app (Nukodes, private repo) hand-rolls the raw pieces this package
generalizes. If the reviewer has access, inspect:

- `apps/client/src/powersync/hooks/usePowerSyncRawInfiniteQuery.ts` — keyset + lookahead +
  throttled `onChange` invalidation (the §6 local leg, in production).
- `apps/client/src/powersync/hooks/usePowerSyncOnDemandStream.ts` — stream subscribe →
  waitForFirstSync → TTL → unsubscribe lifecycle (the §6 partition escalation precursor).
- `apps/client/src/powersync/syncWindow/syncMonthsStore.ts` + `apps/client/src/powersync/Connector.ts` —
  server-signed history window carried to screens.
- `config/sync_streams.yaml` — month-partitioned history streams
  (`pos_history_month`, `finance_history_month`, …) filtering
  `"businessMonth" IN auth.parameter('sync_months')`; on-demand `*_detail` streams using
  equality `subscription.parameter(...)` gated by signed claims.
- `packages/trpc/src/server/routers/powersync.ts` (`deriveHistoryMonths`) — the signed
  4-business-month window (current + 3 prior, per-org business calendar).

If no access: the patterns are fully described in §5–§7 and were validated in the reference
thread cited at the top of this file.

---

## 14. Open questions

**Resolved (kept for the record):**
- ~~raced × infinite composition~~ → race page 1 + escalation over one id set (R13).
- ~~TanStack coupling depth~~ → `fetchQuery` + engine store (R9/R11).
- ~~API snapshot / backend contract~~ → opaque tuple-cursor keyset pages + `since` + tenant
  floor (§12.1).
- ~~Stream-escalation latency UX~~ → three-valued boundary + `awaitingSync` (§6.2).
- ~~PR #1101 compatibility~~ → track upstream `differentialWatch({ initialData })` (below).
- ~~"Answered" predicate~~ → session-scoped freshness; persisted `hasSynced` never gates
  arbitration [R2,F2-002].
- ~~Freshness tie-breaks across snapshot replacement~~ → fetches are mount-triggered only
  and never issued after local ownership; no timer-driven refetch, so ownership cannot
  ping-pong (R9) [R3,F3-009].
- ~~Presence-probe amortization~~ → one batched chunked statement per emission (R14)
  [R4,F4-006].
- ~~Volatile-claim deny-list configurability~~ → configurable default covering the OIDC
  volatile set + vendor session claims; connection parameters opted in by key (R15)
  [R4,F4-008].

**Open (reviewer: prioritize):**

1. **TTL defaults** — escalation subscription `ttl: 5 min` (§6.5) needs validation against
   real scroll patterns; may need per-list tuning or an adaptive policy.
2. **Bucket ceiling (default 500)** — warn-only in v0; hard-gate later? Fold in partition-cap
   ergonomics (auto-extend on user action vs "load more" affordance).
3. **Quarantine / `stale` UX** — is the per-row `stale: true` flag enough, or should
   `state` expose a `'local-stale'` variant so apps can banner instead of per-row styling?
4. **`ps_crud` coupling** — internal upstream table; pin the schema assumption behind
   `pendingUploadIds()` with a startup guard so upstream changes fail loudly at init.
5. **Divergence surfacing** — row updated locally AND differently via API pre-ownership:
   freshness decides the render; should the package emit a `divergence` event for UX or
   stay silent? (carried)
6. **SSR/streaming** — server-render the API leg's HTML, hydrate the race client-side;
   design now or defer. (carried)
7. **Upstream `differentialWatch({ initialData })`** — track the proposal (PR #1101 thread);
   if it lands pre-ready, the §5.2 overlay store can be replaced by SDK seeding (code
   deletion is the goal). (carried)
8. **Vue/Svelte adapters** — demand-gated; core is store-agnostic so adapters are mechanical.
   (carried)
9. **`asOf` = PowerSync write checkpoint [R4,F4-011]** — promoted to a v1 **optional**
   contract field, reframed: the snapshot echoes the write checkpoint the backend observed
   when serving (comparable against the client's synced-past checkpoint); LSNs are
   backend-internal. Still open: backend adoption recipe for Go, and degradation UX when
   absent (the client-derived shields are the default).
10. **Zombie-window policy** — is mount-lifetime retention of evicted rows always right,
    or should very long retentions trigger a proactive re-subscribe when the user scrolls
    near the retained range? (carried from R2)
11. **Ownership-shrink UX [new, R4]** — with the F4-002 default, lists whose API scope
    exceeds the sync window shrink at ownership unless `apiScopeSupersetOfLocal` is set;
    is a docs-level decision matrix enough, or should the bridge emit a one-shot
    `ownershipShrunk(count)` diagnostic?
12. **Offset-mode skip UX [new, DX]** — in `api-offset` mode a skipped row under live inserts
    is silent to the user; should the bridge surface a one-shot "list may have shifted,
    refresh" hint, or is documentation enough?

---

## 15. Review status — CLOSED (converged)

The adversarial review loop closed after six rounds (77 findings, all accepted — see the
header and `REVISION-round{5,6}.md`).

- **Round 5** (verification pass) confirmed the round-4 amendments close what they claimed
  (F4-001, F4-003, F4-004, F4-006, F4-007), fixed the one runtime-behavior residual
  (F5-001: `recentlyAcked` retirement is stamp-based, not completion-time-based), and
  reported: *"no further adversarial round is warranted; a final drift pass by the
  implementing agent is sufficient."*
- **Round 6** (post-convergence verification) verified all seven round-5 amendments as
  applied, resolved the one residual ambiguity (F6-001: `recentlyAcked` and the crud-delta
  mirror are **identity-scoped engine state**, subscribed from engine init and shared by
  every list store — not per-list), fixed the two remaining wording drifts, and reported:
  *"the design is converged and no further review round is warranted."*

**What convergence means here:** rules R1–R17 and the test matrix T1–T41 are the invariant
set that graduates into the implementation, the README, and CI. §14's open questions are
design decisions deferred to implementation, not unresolved flaws. If implementation
invalidates a rule, the protocol of §16 applies to the amendment, not to a re-review of
the whole design.

---

## 16. Agent-to-agent feedback protocol (how reviews feed back)

The implementing agent (author of this file) integrates reviews in **rounds**. File naming:
`REVIEW-roundN.md` (reviewer output, verbatim) → `REVISION-roundN.md` (implementing agent).

1. **Reviewer returns** the §15 output contract (findings list + questions + verdict).
2. **Implementing agent returns** the round's `REVISION-roundN.md` containing:
   - `Accepted` — each finding id → the section patch applied (quote old/new).
   - `Rejected` — each finding id → technical justification (rejection requires evidence,
     not preference; "agree to disagree" is not a valid reason).
   - `Deferred` — finding id → which open question (§14) it becomes, if legitimate but v2.
   - `New open questions` discovered during integration.
3. **Convergence:** a round is converged when it produces zero critical/major findings on
   previously-reviewed sections. (The loop closes when that holds — this review ran six
   rounds, 77 findings, and is closed.)
4. **Relay mechanics for the human:** paste the reviewer's findings block verbatim into the
   thread; the implementing agent responds with the revision diff and an updated
   `ARCHITECTURE.md`. No paraphrasing needed — the schema is the interface.
5. **Knowledge loop:** accepted findings become permanent invariants (marked **[R1-n]** …
   **[R6-n]**), rules in §5.3/§6, and required tests in §12.2, so each round's lessons
   survive into the README/contributing docs and the eventual test suite.
6. **Drift pass (mandatory since round 4):** after integrating a round, the implementing
   agent re-reads every section that quotes a rule and reconciles prose with the amended
   §5.3 text. (Round 4 found two stale-paragraph drifts — §10.5 and §9 — because this step
   was skipped.)

---

## 17. Research index (sources consulted during design & verification)

**PowerSync docs:** Infinite Scrolling; Sync Streams overview/parameters/bucket-count;
Sync Data by Time (range operators disallowed on parameters); Prioritized Sync (partial
consistency semantics); Pre-Seeded SQLite; Performance Optimization; React hooks; watch
queries (differential watches re-query and compare per emission); Drizzle & Kysely ORM
integrations (`toCompilableQuery` executes through Drizzle — decoded rows); TanStack
integration; React Native Web support. (links inline §2–§6.)

**SDK source (powersync-js@main, fetched rounds 1–4):**
`packages/common/src/db/crud/SyncStatus.ts` — `hasSynced?: boolean` ("undefined when the
state is still being loaded from the database", i.e. persisted), `lastSyncedAt`
(**"reset to null after a restart of the PowerSync service"**, line 82),
`syncStreams`, `forStream()`, `statusForPriority()`;
`packages/common/src/client/sync/bucket/CrudBatch.ts` —
`complete(writeCheckpoint?: string)` — completions drain `ps_crud` (upload-queue
semantics); the write-checkpoint carrier for the `asOf` contract [R4,F4-001,F4-011];
`packages/common/src/client/sync/sync-streams.ts` — `waitForFirstSync(abort?: AbortSignal)`
(no timeout param), async `subscribe()`, cross-tab TTL refcounting ("including subscriptions
created on other tabs"), `unsubscribeAll()` TTL reset, `SyncSubscriptionDescription`
(`active`, `expiresAt`, `hasSynced`, `lastSyncedAt`).

**GitHub (`powersync-ja/powersync-js`):** PR #1101 incl. Sep 17 maintainer review (plugin
API rejected; query-path-first position) and Sep 19 withdrawal + `differentialWatch({initialData})`
proposal with measurement tables (cold boot & re-mount, IndexedDB vs OPFS WAL, 10–200 MB);
issue #1114 (+ maintainer reply; 2.4.0/2.4.1 fixes); issue #1081; issue #1063; PR #889;
issue #698; PR #829 / PR #776; `getCrudBatch` default limit 100 + `CrudBatch.haveMore`.
Releases: `@powersync/web@2.4.1` (2026-09-23), `@powersync/common@2.3.0`,
`@powersync/tanstack-react-query@0.3.4` (2026-09-21, npm registry verified).

**Community:** `powersync-query-cache` (gartz) + `powersync-query-logger`; discussion
powersync-ja#445 (flag-flip REMOVE-op growth); PowerSync blog "SQLite persistence on the web"
(May 2026 — Safari multi-tab / OPFS SharedWorker constraints).

**Production precedent:** Nukodes app — hand-rolled keyset+lookahead infinite queries,
on-demand stream lifecycle, signed month-window claims (§13).
