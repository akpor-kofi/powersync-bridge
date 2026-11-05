# Changelog

## 0.0.1-alpha.1

- Add `useInfiniteQuery` for a complete raced, paginated list from one query definition.
- Use a typed previous-row cursor, one row generic, familiar loading flags, and direct row data.
- Race every requested page with one loaded-range SQLite watch and an ordered API cursor chain.
- Preserve pending inserts, updates and deletes across API snapshots, including after upload acknowledgement.
- Handle sparse downloaded feeds without advancing remote cursors past unseen server rows.
- Keep downloaded pages usable offline or after HTTP failures; resume on reconnection and support explicit retry.
- Own refresh, cancellation, scope/identity disposal, end detection, bounded memory and continuation windows.
- Add `mapQuery` and `rawQuery` to the Drizzle adapter, preserving source-table invalidation and decoding.
- Align peer ranges with the tested Nukodes SDKs (`@powersync/common` 1.57.3 and Drizzle driver 0.7.4), while retaining the existing newer SDK support.
- Add optional `BridgeProvider online` / `bridge.setOnline` connectivity hints and browser network listeners.
- Reconcile the fixes previously carried as consumer patches, including React store callbacks, StrictMode lifecycle,
  upload-queue iteration and pending operation transitions, snapshot bounds, and local pagination loading state.

- Add `release:publish` for terminal-owned npm 2FA, reproducible artifact checks, and safe resumption after a partial publish.

### Operational notes

All four packages are released together as `0.0.1-alpha.1`. Consumers can remove the alpha.0 core and PowerSync-driver
patches after upgrading. The existing lower-level hooks remain available. API page functions must honor the requested
lookahead limit and return strictly ordered, decoded rows. Native apps can supply a connectivity boolean once at the
provider. No database migration, sync-stream change, environment variable or new runtime dependency is required.
