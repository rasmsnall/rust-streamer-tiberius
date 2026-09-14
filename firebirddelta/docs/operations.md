# Operations

## Status before you deploy this

**Read this section before any other.** Unlike tiberiusdelta, which has been run
against a local SQL Server container and has real throughput numbers, `firebirddelta`
has not been run against any Firebird instance at all as of this document (see
`CLAUDE.md`'s Status and Open items). Treat every number and procedure below that is not
explicitly marked "measured" as a design intent, not a verified fact, and run the live
test suite (`cargo test --test firebird_live`, after `tools/seed.py`) against your own
throwaway instance before trusting this against production.

## Provisioning the source account

- Create a Firebird user with **`SELECT`-only** grants on the tables to sync. Firebird's
  wire protocol has no client-side read-only mode; the grant is where that guarantee
  actually lives (`CLAUDE.md`'s Security requirement 4).
- The account needs no special role for reading `RDB$RELATION_FIELDS`/`RDB$FIELDS`:
  Firebird's system tables are readable by any authenticated user by default.
- Firebird's connection string form is a URL: `firebird://user:password@host:port/database`.
  Store the password in a secret manager (Databricks secret scope, or equivalent), never
  in notebook source or job configuration in the clear.

## Sizing

Not measured for this crate (see above). Two structural facts that will shape sizing
once real numbers exist:

- **The fetch side is synchronous**, dispatched onto Tokio's blocking-thread pool one
  table at a time (see `docs/architecture.md`). A run's fetch throughput is therefore
  bounded by however fast one blocking OS thread can pull rows off one Firebird cursor,
  not by anything this crate's own concurrency could improve within a single table.
  Scaling *across* tables (more workers, `firebirddelta.distributed`) is unaffected by
  this, since each worker's own fetch is independently blocking-but-parallel across
  workers.
- **`fetch_batch_size`** (default 10,000) bounds memory, not just throughput: each batch
  is fully materialised as Arrow arrays before being merged, and the channel between the
  blocking fetch and the async merge holds at most two batches at once. Lowering it
  trades throughput for a smaller memory footprint; raising it does the reverse, up to
  whatever the process's memory budget allows.

## Monitoring

- **`SyncReport.tables[].excluded_columns`** is worth alerting on for a schema you do not
  fully control: a column that becomes excluded between two runs (a `BLOB` sub-type
  changed, a column retyped to `DECFLOAT`) silently stops being synced, with no error,
  unless it happens to be the watermark or primary key column (in which case the whole
  table's sync fails loudly instead; see `docs/api.md`).
- **`ConcurrentWriteError`** (or its Rust counterpart, `Error::ConcurrentWrite`) means two
  syncs of the same table overlapped. On Databricks, set the job's maximum concurrent
  runs to 1; this error is always safe to retry.
- A stalled sync that has not completed within an expected window may not be a hang in
  the usual sense: see the timeout caveat below.

## The timeout caveat, operationally

`login_timeout_sec` and `query_timeout_sec` bound how long this library *waits*, not how
long the underlying Firebird call actually runs (`CLAUDE.md`'s Threat model). If a
network partition or a stuck query causes a timeout to fire, the blocking OS thread
holding that connection is not stopped — it is abandoned, and continues running (and
holding its connection) until Firebird's own network-level timeout releases it, or the
process exits. Operationally, this means:

- A process that experiences repeated timeouts against a flaky network can accumulate
  blocked threads on Tokio's blocking pool. This is bounded (Tokio caps the pool size),
  but a long-running scheduler process syncing many tables against a flaky source could
  in principle exhaust it. Restarting the process clears them.
- A timeout is not proof the source is unreachable; it is proof this library gave up
  waiting. Treat a burst of timeouts as a signal to check the network path and the
  source's own load, not as confirmation the source is down.

## Recovery

Identical to tiberiusdelta's own recovery story, since the checkpoint/merge ordering is
unchanged: a crash or a killed process leaves every already-committed table's checkpoint
correctly advanced and the rest untouched. Simply re-run; a table mid-way through a
merge when the process died re-fetches from its last checkpoint and re-applies safely,
because `MERGE` is an upsert.

## Runbook: adding a new table

1. Confirm the table's actual watermark and primary key columns; Firebird's own
   `RDB$RELATION_FIELDS.RDB$FIELD_POSITION` order is what `preflight` reports columns in.
2. Run `preflight` with the new table added to the configuration and check `ready`,
   `text_fallback_columns`, and any `excluded` columns before adding it to a real sync.
3. If the table has no watermark column at all, it cannot be synced incrementally by
   this library (see `CLAUDE.md`'s "What incremental sync does not do") — it needs a
   full reload, a different tool, or a schema change on the source.
4. For a table too large to seed row-by-row, use `source_watermark`/`set_checkpoint`
   around a bulk export (see `docs/api.md`), capturing the watermark **before** the
   export starts.
