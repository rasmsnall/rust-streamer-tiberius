# tiberiusdelta: Operations

**Document type** Operations manual
**Status** Complete and implemented, and not yet run against a production source; see Chapter VIII.
**Audience** Whoever runs this on a schedule and is called when it fails.
**Companion documents** `architecture.md` for the design, `api.md` for the callable surface.
**Version** 1.0
**Date** 2026-09-13

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. The workload
- II. Deployment
  - 1. Building the wheel
  - 2. Installing on Databricks
  - 3. Network reachability
  - 4. Pre-flight checking
  - 5. Where output may be written
  - 6. The compatibility floor
- III. Provisioning the Source
  - 1. The database account
  - 2. Choosing a watermark column
  - 3. Load on the source
- IV. Sizing
  - 1. What scales and what does not
  - 2. Memory
  - 3. The first run
  - 4. Expected runtime
- V. Storage Maintenance
  - 1. Why storage grows
  - 2. Scheduling VACUUM
  - 3. Choosing a retention window
- VI. Failure and Recovery
  - 1. What a failure leaves behind
  - 2. Re-running
  - 3. Diagnosing by exception
  - 4. Forcing a full re-sync
- VII. Monitoring
  - 1. What to record every run
  - 2. What should page someone
  - 3. The silent failure
- VIII. Change Management
  - 1. Schema drift at the source
  - 2. A table that disappears
  - 3. Onboarding a new table
  - 4. Before production
- References
- Appendix A. Runbook
- Appendix B. Forcing a full re-sync

### List of Tables

- `<Table 3-1>` Watermark column suitability
- `<Table 4-1>` What scales with what
- `<Table 6-1>` Diagnosing by exception
- `<Table 7-1>` What to record every run
- `<Table 7-2>` Alert conditions

### List of Figures

- `[Figure 2-1]` Installing and calling on Databricks
- `[Figure 4-1]` Peak memory
- `[Figure 6-1]` State after a failed run
- `[Figure 7-1]` A run record worth keeping

---

## I. Introduction

### 1. Purpose

How to deploy, size, schedule, monitor and recover this library. Everything here assumes
the architecture in `architecture.md` and the surface in `api.md`.

### 2. The workload

A scheduled job, typically daily or hourly, that connects to a live SQL Server, pulls the
rows that changed since its own last run, and merges them into Delta tables.

Three properties shape every operational decision:

- **It touches production.** The source is serving real users while this runs.
- **Cost is proportional to change, not to size.** A steady-state run is small and fast.
  The exception is a table's first run, which is a full load.
- **Re-running is always safe.** There is no cleanup step and no partial state to
  reconcile.

## II. Deployment

### 1. Building the wheel

```bash
pip install maturin
maturin build --release --features extension-module,azure
```

The output is a single `abi3` wheel, tagged `cp310-abi3`, that loads on CPython 3.10 and
later. Add `gcp` or `s3` in place of `azure` for those object stores; omit all three for a
local path or a `/Volumes` path.

There is no C toolchain, no cmake and no database driver to install. That is a direct
consequence of the connectivity choice: `tiberius` is pure Rust and speaks TDS over a
plain socket.

CI builds the same wheel for `manylinux_2_28` on every push, verifies the `abi3` tag,
installs it on 3.10, 3.12 and 3.13, and runs a real sync against a SQL Server service
container through the installed wheel. A wheel that merely imports proves only that the
extension links.

### 2. Installing on Databricks

Install the wheel on the cluster or as a job library, then:

```python
import tiberiusdelta

connection_string = dbutils.secrets.get(scope="prod", key="sqlserver_connection_string")

report = tiberiusdelta.sync_tables(
    connection_string,
    "/Volumes/main/raw/mssql/",
    TABLES,
)
```

[Figure 2-1] Installing and calling on Databricks

The connection string must come from a secret scope. It carries the password, and a
notebook cell is a place where it is visible in revision history, in cluster logs, and to
anyone with read access to the notebook.

DBR 16.4 LTS and 17.3 LTS both ship Python 3.12, which the wheel covers.

### 3. Network reachability

In a cloud deployment this is usually the hardest part of the whole deployment and the
first thing to test.

The compute running this needs a TCP route to the SQL Server instance, which in practice
means private networking between the Databricks workspace and the database rather than a
public endpoint: VNet or VPC peering, a private endpoint, or equivalent. Firewall rules
must permit the workspace's egress addresses on the instance's port, conventionally 1433.

Prefer `Encrypt=true`. `TrustServerCertificate=true` disables verification of the server's
certificate and should be confined to a throwaway test instance, never used against
production.

### 4. Pre-flight checking

Run `preflight` before the first real sync of any schema, and consider running it before
every scheduled sync.

```python
found = tiberiusdelta.preflight(connection_string, TABLES, output_uri=OUTPUT)
unready = [t.table for t in found if not t.ready]
if unready:
    raise SystemExit(f"not ready: {unready}")
```

It connects, reads each table's columns, and reports whether the configured watermark and
primary key columns still exist, what each column's type would become, and where each
checkpoint stands. It writes nothing.

As a guard before every run it costs one round trip per table and catches the single most
likely production surprise: a column renamed or dropped at the source. Without it, that
surprise arrives mid-run, after some tables have already been merged.

### 5. Where output may be written

- An **external location** or a **`/Volumes/...` path**. Both are fine.
- **Never a Unity Catalog managed table.** External writers can corrupt them.

A qualified table name becomes a subdirectory, so `dbo.customers` lands at
`<output_uri>/dbo/customers` and the checkpoint table at
`<output_uri>/_streamer_checkpoints`.

### 6. The compatibility floor

Output stays at **reader version 1, writer version 2**. No deletion vectors, no column
mapping, no `timestamp_ntz`. Any Databricks Runtime can read it.

This is why every timestamp column carries an explicit UTC zone: an Arrow timestamp
without one maps to `timestamp_ntz`, which requires reader version 3 and writer version 7
and would break the floor for the most common timestamp type there is.

## III. Provisioning the Source

### 1. The database account

**Grant `SELECT` only**, on the tables being synced and on `INFORMATION_SCHEMA`.

This is the actual read-only guarantee. The library issues only `SELECT`, but TDS has no
client-side read-only mode, so nothing in the client can enforce it. The grant is where
the guarantee lives, and it is not optional: this connects to a production system with
credentials that will sit in a scheduler for years.

```sql
CREATE LOGIN svc_delta WITH PASSWORD = '...';
CREATE USER svc_delta FOR LOGIN svc_delta;
GRANT SELECT ON SCHEMA::dbo TO svc_delta;
```

### 2. Choosing a watermark column

The single most consequential configuration decision, and the one with no error message
when it is wrong.

| Column | Suitable | Why |
|---|---|---|
| `IDENTITY` integer | For inserts only | Strictly increasing, never ties. Updates do not change it, so updates are never seen |
| `DATETIME2(7)` set on insert and update | Yes | Precise enough that ties do not occur in practice |
| `DATETIME2(0)`, `SMALLDATETIME` | Risky | Coarse enough that two writes can tie, and a tie loses rows permanently |
| `ROWVERSION` | No | Monotonic, but it is binary, and comparison through a text checkpoint is not meaningful |
| A business date | No | Can be backdated, which moves it downward |
| Set on insert only | No | Updates are never detected |

`<Table 3-1>` Watermark column suitability

The failure mode is silent in every unsuitable case. A row that should have synced simply
never appears, and no count, no error and no log line says so.

### 3. Load on the source

Each table costs one `INFORMATION_SCHEMA` lookup and one indexed range scan per run. In
steady state, that is small.

Two things make it not small:

- **A first run**, which reads the whole table. Onboard large tables deliberately, ideally
  outside business hours.
- **A watermark column with no index.** `WHERE updated_at > ? ORDER BY updated_at` without
  an index on `updated_at` is a full scan and a sort, every run. Index it.

## IV. Sizing

### 1. What scales and what does not

| Quantity | Scales with |
|---|---|
| Peak memory | `fetch_batch_size` and row width. Not table size |
| Runtime, steady state | Rows changed since the last run |
| Runtime, first run | Total rows in the table |
| Source load | Rows returned, plus one catalog lookup per table |
| Delta storage | Rows stored, plus superseded files until vacuumed |

`<Table 4-1>` What scales with what

### 2. Memory

```
peak memory ~ fetch_batch_size x row width + the Arrow batch + Parquet writer buffers
```

[Figure 4-1] Peak memory

At the default of ten thousand rows, a wide row of a few kilobytes is tens of megabytes.
Lower it for very wide rows; raising it does not make the source faster and raises the
footprint proportionally.

### 3. The first run

A table with no checkpoint has no `WHERE` clause and fetches every row. Plan for it:

- Onboard large tables one at a time rather than adding twenty to the configuration at
  once.
- Expect the first run's duration to resemble a full export of that table.
- Every run after it is proportional to change.

### 4. Expected runtime

Dominated by whichever of these is largest: the source's scan, the network transfer, or
the Delta merge. For a steady-state incremental run of a few thousand rows across a few
dozen tables, the merges usually dominate, because each one is a Delta commit with its own
object-store round trips regardless of how few rows it carries.

A run covering many tables that change rarely therefore costs roughly a fixed amount per
table. If that becomes the bottleneck, syncing tables concurrently is the available
improvement and is not currently implemented; see `architecture.md`, Chapter IV, Section 4.

## V. Storage Maintenance

### 1. Why storage grows

A merge rewrites the Parquet files containing the rows it touched. The superseded files
remain in storage, referenced by older versions of the transaction log, until vacuumed.

An incremental sync touches few rows but can touch files across the whole table, so
storage growth is a function of how scattered the changed rows are, not only of how many
there are.

### 2. Scheduling VACUUM

The library cannot run it. Schedule it.

```sql
VACUUM delta.`/Volumes/main/raw/mssql/dbo/customers` RETAIN 168 HOURS;
```

Loop over every synced table, and include `_streamer_checkpoints`, which is rewritten on
every run of every table and accumulates files faster than any data table.

### 3. Choosing a retention window

The window must exceed the longest query a reader might still be running against an older
version, and the longest time-travel window anyone depends on. One week is a common
default. Delta refuses a window under 168 hours without an explicit override; overriding
it is how readers get errors about files that no longer exist.

## VI. Failure and Recovery

### 1. What a failure leaves behind

```
table 1  merged, checkpoint advanced     <- visible, current
table 2  merged, checkpoint advanced     <- visible, current
table 3  failed                          <- unchanged, previous checkpoint intact
table 4  never attempted                 <- unchanged
```

[Figure 6-1] State after a failed run

No table is ever left half-merged: each table's merge is one atomic Delta commit, and its
checkpoint advances only after that commit succeeds.

### 2. Re-running

Run the job again. That is the entire recovery procedure.

Tables already synced have nothing outstanding and fetch zero rows. The table that failed
resumes from its unchanged checkpoint. Re-merging rows already merged updates them in
place rather than duplicating them.

There is no cleanup, no partial state to reconcile, and no case where running twice is
worse than running once.

### 3. Diagnosing by exception

| Exception | Likely cause | Action |
|---|---|---|
| `ConnectionError` | Network path down, credentials rotated, instance restarting | Check reachability and the secret. Safe to retry |
| `ValueError` mentioning a watermark column or primary key | Configuration does not match the source | Run `preflight`; a column was probably renamed or dropped |
| `ValueError` mentioning a value and a type | A schema change landed mid-run | Re-run. If persistent, `preflight` and reconcile the configuration |
| `ValueError` mentioning an unsafe name | A configured name contains a quote, semicolon or backslash | Fix the configuration |
| `RuntimeError` mentioning delta or checkpoint | Object-store failure or a commit conflict | Check storage permissions and whether two syncs ran concurrently. Safe to retry |
| `KeyboardInterrupt` | Cancelled, or the job was killed | Re-run |

`<Table 6-1>` Diagnosing by exception

No exception can contain the connection string or the password; both are removed before
the error leaves the library. An exception that appears to contain one is a bug worth
reporting.

### 4. Forcing a full re-sync

Delete that table's row from `_streamer_checkpoints`; see Appendix B. The next run has no
checkpoint for it and fetches everything, merging it over the existing rows.

This is the right response to a suspected gap, and it is safe: the merge is an upsert, so
a full re-sync repairs rows that are wrong and adds rows that are missing. It does not
remove rows deleted at the source, because nothing in this design can.

## VII. Monitoring

### 1. What to record every run

| Field | From |
|---|---|
| Run start and end, and duration | The scheduler |
| `total_rows_fetched` | `SyncReport` |
| Per table: `rows_fetched`, `rows_inserted`, `rows_updated` | `TableSyncStats` |
| Per table: `text_fallback_columns` | `TableSyncStats` |
| Each table's `last_value` after the run | `_streamer_checkpoints` |
| The exception type and message on failure | The caller |

`<Table 7-1>` What to record every run

```python
report = tiberiusdelta.sync_tables(connection_string, OUTPUT, TABLES)
for t in report.tables:
    log.info(
        "sync table=%s fetched=%d inserted=%d updated=%d fallbacks=%s",
        t.table, t.rows_fetched, t.rows_inserted, t.rows_updated, t.text_fallback_columns,
    )
```

[Figure 7-1] A run record worth keeping

### 2. What should page someone

| Condition | Why it matters |
|---|---|
| The run failed | Data is now stale by at least one interval |
| A table's checkpoint has not advanced in N intervals | Either nothing changed, or the watermark stopped being maintained. Indistinguishable from outside |
| `rows_fetched` is far above the historical norm | A watermark was reset, or a bulk update ran at the source |
| `rows_fetched` is zero across every table, repeatedly | The most likely shape of a silent failure |
| A new column appears in `text_fallback_columns` | Schema changed at the source and fidelity dropped |
| `VACUUM` has not run | Storage is growing without bound |

`<Table 7-2>` Alert conditions

### 3. The silent failure

This design has one genuinely silent failure mode, and monitoring exists mostly for it.

If the watermark column stops being maintained at the source, because a trigger was
dropped, an application path was rewritten, or a bulk load bypassed it, then every
subsequent run succeeds, reports zero rows, and transfers nothing. The Delta tables quietly
stop reflecting the source, and nothing in the pipeline can tell that apart from a quiet
day.

The defence is external: compare row counts against the source periodically.

```sql
-- At the source
SELECT COUNT(*), MAX(updated_at) FROM dbo.customers;
```

```sql
-- In Delta
SELECT COUNT(*), MAX(updated_at) FROM delta.`/Volumes/main/raw/mssql/dbo/customers`;
```

Counts will not match exactly if rows have been deleted at the source, which is expected
and is itself worth quantifying. A growing divergence in `MAX(updated_at)` is the clearer
signal, and it is the one to alert on.

## VIII. Change Management

### 1. Schema drift at the source

The Arrow schema is resolved from the source catalog on every run, so a column added at
the source appears in the next run's schema.

What that means in practice:

- **A new column** is picked up and merged. Rows synced before it appeared keep a NULL for
  it unless they are re-synced.
- **A dropped or renamed column** that is the watermark or part of the primary key fails
  the table. `preflight` catches this before a run rather than during one.
- **A retyped column** may change the Delta schema, which can conflict with the existing
  table. Re-syncing the table into a fresh path is the clean response.

### 2. A table that disappears

A table removed from the source, or from the configuration, is not detected. Its Delta
table remains exactly as the last successful sync left it, going quietly stale, and
nothing reports this. Reconcile the configured table list against the source periodically.

### 3. Onboarding a new table

1. Confirm it has a usable watermark column; see Chapter III, Section 2.
2. Confirm that column is indexed at the source.
3. Add it to the configuration and run `preflight`. Check `ready`, and check
   `text_fallback_columns` for anything that matters downstream.
4. Run the first sync deliberately, outside business hours if the table is large. It is a
   full load.
5. Confirm the row count against the source before relying on it.

### 4. Before production

This library has been verified end to end against a local SQL Server 2022 instance, across
every type it maps, through both the Rust and the Python surface. It has **not** yet run
against a production source.

Before it does:

- Provision the `SELECT`-only account (Chapter III, Section 1).
- Establish the network path and test it with `preflight` alone.
- Confirm the watermark column of every table against the criteria in Chapter III,
  Section 2. This is the step most likely to surface a table that cannot be synced
  incrementally at all.
- Agree what `VACUUM` schedule will exist, and who owns it.
- Agree the reconciliation check in Chapter VII, Section 3, and its alert threshold.

## References

1. Microsoft. *Connection string syntax*.
   <https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax>
2. Microsoft. *GRANT Object Permissions (Transact-SQL)*.
   <https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-object-permissions-transact-sql>
3. The Delta Lake project. *Delta Transaction Log Protocol*.
   <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
4. Databricks. *VACUUM*.
   <https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html>
5. Databricks. *Secret management*.
   <https://docs.databricks.com/en/security/secrets/index.html>
6. Databricks. *Databricks Runtime release notes*.
   <https://docs.databricks.com/en/release-notes/runtime/index.html>

## Appendix A. Runbook

```python
import os
import tiberiusdelta

OUTPUT = "/Volumes/main/raw/mssql/"
TABLES = [
    {"table": "dbo.customers", "watermark_column": "updated_at", "primary_key": "id"},
    {"table": "dbo.invoices", "watermark_column": "modified", "primary_key": "invoice_id"},
]

connection_string = dbutils.secrets.get(scope="prod", key="sqlserver_connection_string")

# Cheap, and catches a schema change before any table is merged. See Chapter II, Section 4.
found = tiberiusdelta.preflight(connection_string, TABLES, output_uri=OUTPUT)
unready = [t.table for t in found if not t.ready]
if unready:
    raise SystemExit(f"configuration does not match the source: {unready}")

report = tiberiusdelta.sync_tables(connection_string, OUTPUT, TABLES)

# Record these. They are the whole audit trail. See Chapter VII, Section 1.
for t in report.tables:
    print(t.table, t.rows_fetched, t.rows_inserted, t.rows_updated, t.text_fallback_columns)

# The silent failure this design has. See Chapter VII, Section 3.
if report.total_rows_fetched == 0:
    print("warning: nothing changed at the source, or a watermark stopped being maintained")
```

## Appendix B. Forcing a full re-sync

Deleting a table's checkpoint row makes the next run fetch that table in full. Safe, and
the right response to a suspected gap.

```python
from deltalake import DeltaTable

checkpoints = DeltaTable("/Volumes/main/raw/mssql/_streamer_checkpoints")
checkpoints.delete("table_name = 'dbo.customers'")
```

Then run the sync as usual. The table has no checkpoint, so it fetches everything and
merges it over the existing rows: wrong rows are corrected, missing rows are added.

Rows deleted at the source are **not** removed by this, because nothing in this design
detects deletions. To rebuild a table so that it exactly matches the source, sync it into
a fresh output path and swap, rather than trying to repair the existing one in place.
