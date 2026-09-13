# tiberiusdelta: API Reference

**Document type** Interface reference
**Status** Complete and implemented. Both surfaces run end to end against a live SQL Server.
**Audience** Anyone calling this library from Python or from Rust.
**Companion documents** `architecture.md` for the design, `operations.md` for running it.
**Version** 1.0
**Date** 2026-09-13

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. Which surface to use
- II. Python Surface
  - 1. Installation and import
  - 2. `sync_tables`
  - 3. Parameters
  - 4. Table configuration
  - 5. Return value
  - 6. Exceptions
  - 7. The progress callback
  - 8. `preflight`
- III. Returned Statistics
  - 1. `SyncReport`
  - 2. `TableSyncStats`
  - 3. `TablePreflight` and `ColumnPreflight`
  - 4. Reading the statistics
- IV. Rust Surface
  - 1. Entry points
  - 2. `SyncConfig`
  - 3. Module map
  - 4. Error type
- V. Semantics That Callers Must Know
  - 1. The watermark contract
  - 2. Deletions
  - 3. Naming and output paths
  - 4. Timestamps and time zones
  - 5. Type fidelity
  - 6. Re-running and idempotency
  - 7. The checkpoint table
- VI. Worked Examples
  - 1. A daily sync
  - 2. Preflight before onboarding a schema
  - 3. Calling from Rust
- References
- Appendix A. Parameter quick reference

### List of Tables

- `<Table 2-1>` `sync_tables` parameters
- `<Table 2-2>` Table configuration keys
- `<Table 2-3>` Exceptions raised
- `<Table 3-1>` `SyncReport` fields
- `<Table 3-2>` `TableSyncStats` fields
- `<Table 3-3>` `TablePreflight` fields
- `<Table 4-1>` Rust module map
- `<Table 4-2>` Error variants
- `<Table A-1>` Parameter quick reference

### List of Figures

- `[Figure 2-1]` Shape of a call. Only the first three arguments are positional.
- `[Figure 6-1]` A daily sync
- `[Figure 6-2]` Preflight output for one table

---

## I. Introduction

### 1. Purpose

This document specifies the callable surface: every parameter, every return value, every
exception, and the semantics a caller has to understand to use the library correctly
rather than merely successfully.

Chapter V is the part worth reading before writing any code. Everything in it is a
property a caller can be wrong about without receiving an error.

### 2. Which surface to use

The **Python surface** is the intended one. It is what the wheel exposes, what Databricks
notebooks and jobs call, and what these examples use.

The **Rust surface** exists because the library is a Rust crate and its internals are
public and documented. Use it when embedding this in another Rust program. It offers one
capability the Python surface does not: `pipeline::sync_table`, a single-table `async`
function for a caller that already has a Tokio runtime.

## II. Python Surface

### 1. Installation and import

```bash
pip install tiberiusdelta-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

One wheel covers CPython 3.10 and later, because the extension is built against the
stable ABI. There is no database driver to install: the library speaks TDS over a socket
itself.

```python
import tiberiusdelta
```

### 2. `sync_tables`

The single entry point for moving data.

```python
report = tiberiusdelta.sync_tables(
    connection_string,
    output_uri,
    tables,
    *,
    checkpoint_uri=None,
    fetch_batch_size=10_000,
    login_timeout_sec=30,
    query_timeout_sec=300,
    progress=None,
)
```

[Figure 2-1] Shape of a call. Only the first three arguments are positional.

It connects once, and for each configured table fetches the rows new or changed since that
table's last successful run, merges them into that table's Delta table, and advances its
checkpoint. It blocks until every table is done or one fails.

### 3. Parameters

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `connection_string` | `str` | required | ADO.NET-style connection string. Carries the password; see Chapter V and the security model |
| `output_uri` | `str` | required | Prefix each table's Delta table is written beneath |
| `tables` | sequence of mappings | required | One entry per table to sync; see Section 4 |
| `checkpoint_uri` | `str` or `None` | derived | Where the checkpoint table lives. Defaults to `<output_uri>/_streamer_checkpoints` |
| `fetch_batch_size` | `int` | `10_000` | Rows accumulated before each merge. Bounds peak memory |
| `login_timeout_sec` | `int` or `None` | `30` | Seconds allowed to establish the connection |
| `query_timeout_sec` | `int` or `None` | `300` | Seconds the source may go without producing the next row |
| `progress` | callable or `None` | `None` | Called after each table finishes; see Section 7 |

`<Table 2-1>` `sync_tables` parameters

`connection_string` is passed to SQL Server's own connection-string parser, so the usual
keys apply: `Server`, `Database`, `User Id`, `Password`, `Encrypt`,
`TrustServerCertificate`, `Integrated Security`.

```
Server=tcp:db.example.internal,1433;Database=collections;User Id=svc_delta;Password=...;Encrypt=true
```

`fetch_batch_size` is a safety limit rather than a throughput knob. Raising it raises peak
memory proportionally and does not make the source faster.

`query_timeout_sec` bounds the wait for the *next row*, not the whole query, so a large
table that is still delivering rows is never killed for being large. A query that has
stopped making progress is.

### 4. Table configuration

Each entry in `tables` is a mapping:

| Key | Type | Meaning |
|---|---|---|
| `table` | `str` | Qualified table name exactly as the source names it, for example `dbo.customers` |
| `watermark_column` | `str` | Column filtered on: `WHERE <watermark_column> > <last_value>` |
| `primary_key` | `str` or sequence of `str` | Column or columns the merge matches rows on. A bare string is accepted as a single-column key |

`<Table 2-2>` Table configuration keys

```python
tables = [
    {"table": "dbo.customers", "watermark_column": "updated_at", "primary_key": "id"},
    {"table": "dbo.invoices", "watermark_column": "modified", "primary_key": ["invoice_id"]},
    {"table": "dbo.invoice_lines", "watermark_column": "modified",
     "primary_key": ["invoice_id", "line_no"]},
]
```

A table not listed is not synced. This library never discovers tables on its own; see the
architecture document, Chapter I, Section 4.

Every entry is validated before any connection is opened, so a typo in the tenth entry
fails before the first table is touched. Listing the same table twice is an error rather
than a silent last-wins.

### 5. Return value

A `SyncReport`; see Chapter III.

### 6. Exceptions

| Exception | Raised when |
|---|---|
| `ConnectionError` | The source could not be reached, the login was refused, or the login timed out |
| `ValueError` | Configuration is wrong, or a fetched value contradicts its column's declared type |
| `RuntimeError` | A Delta write, a checkpoint write, or an internal invariant failed |
| `OSError` | An underlying I/O failure |
| `KeyboardInterrupt` | Ctrl-C was pressed between tables |

`<Table 2-3>` Exceptions raised

`ConnectionError` is separated from `ValueError` deliberately: it is the one class worth
retrying automatically, and a caller should not have to match on message text to find it.

No exception message can contain the connection string or the password. Both are removed
before the error leaves the Rust side.

A failure is not a rollback. Tables synced before the failure keep their advanced
checkpoints, which is what makes re-running cheap; see Chapter V, Section 6.

### 7. The progress callback

Called once per table, after that table's checkpoint has advanced, with a mapping:

```python
{"table": "dbo.customers", "tables_done": 3, "total_tables": 12, "rows_fetched": 4182}
```

`rows_fetched` is cumulative across the run.

The callback runs on the calling thread with the GIL held. Raising from it stops the run,
and the exception propagates to the caller rather than being replaced by a generic
interruption. Returning normally continues.

Python signals are re-checked at the same point, which is what makes Ctrl-C work during a
long sync.

### 8. `preflight`

Checks configuration against the live source without writing anything.

```python
found = tiberiusdelta.preflight(
    connection_string,
    tables,
    *,
    output_uri="",
    checkpoint_uri=None,
    login_timeout_sec=30,
    query_timeout_sec=300,
)
```

It connects, reads each table's columns from the source catalog, and reports what a sync
would do: the Arrow type each column would become, which columns would fall back to text,
whether the configured watermark and primary key columns actually exist, and where each
table's checkpoint stands. No Delta table is created and no checkpoint advances.

Pass the real `output_uri` to have `last_synced_value` populated; leave it empty to skip
reading checkpoints.

This is the cheap thing to run when onboarding an unfamiliar schema, and cheap enough to
run before every scheduled sync as a guard against a schema change; see `operations.md`,
Chapter II, Section 4.

## III. Returned Statistics

### 1. `SyncReport`

| Field | Type | Meaning |
|---|---|---|
| `tables` | `list[TableSyncStats]` | Per-table results, in configured order |
| `total_rows_fetched` | `int` | Rows fetched from the source across every table |

`<Table 3-1>` `SyncReport` fields

### 2. `TableSyncStats`

| Field | Type | Meaning |
|---|---|---|
| `table` | `str` | Qualified table name |
| `rows_fetched` | `int` | Rows fetched in this run |
| `rows_inserted` | `int` | Rows the merge inserted, key not previously present |
| `rows_updated` | `int` | Rows the merge updated, key already present |
| `text_fallback_columns` | `list[str]` | Columns written as text because their type is not mapped |

`<Table 3-2>` `TableSyncStats` fields

### 3. `TablePreflight` and `ColumnPreflight`

| Field | Type | Meaning |
|---|---|---|
| `table` | `str` | Qualified table name, as configured |
| `columns` | `list[ColumnPreflight]` | Every column the sync would read, in ordinal order |
| `watermark_present` | `bool` | False if the configured watermark column does not exist |
| `missing_primary_key_columns` | `list[str]` | Configured key columns that do not exist |
| `last_synced_value` | `str` or `None` | Where this table's checkpoint stands |
| `ready` | `bool` | True if a sync would run: watermark and every key column exist |

`<Table 3-3>` `TablePreflight` fields

Each `ColumnPreflight` carries `name`, `source_type` as the source catalog spells it,
`arrow_type`, and `recognised`. A `recognised` of `False` means the column would be
written as text; it does **not** make the table unready, because degrading to text is by
design.

### 4. Reading the statistics

Three things are worth recording every run.

**`rows_fetched` of zero across every table** means nothing changed at the source. That is
a normal and common outcome for an incremental sync. It is also indistinguishable from a
watermark column that has stopped being maintained, which is why `operations.md` treats a
long run of zeroes as something to alert on rather than to celebrate.

**`rows_updated` far exceeding `rows_inserted`, persistently, on a table that should be
mostly append-only**, suggests the watermark is being touched by something other than real
changes, and the sync is transferring rows that did not need transferring.

**`text_fallback_columns`** is a fidelity report. It is not an error, but a column that
appears there is stored as text and cannot be aggregated or compared numerically
downstream. If one matters, the fix is usually a view or computed column at the source
with a type this library maps.

## IV. Rust Surface

### 1. Entry points

```rust
// Blocking. Drives its own Tokio runtime, so it must not be called from inside one.
pub fn run(
    config: &SyncConfig,
    catalog: &SyncCatalog,
    on_progress: impl FnMut(Progress) -> bool,
) -> Result<SyncReport>;

// Blocking, same constraint. Writes nothing.
pub fn preflight(config: &SyncConfig, catalog: &SyncCatalog) -> Result<Vec<TablePreflight>>;

// Async. For a caller that already has a runtime. One table, one connection.
pub async fn sync_table(config: &SyncConfig, table_sync: &TableSync) -> Result<TableSyncStats>;
```

Returning `false` from `on_progress` stops the run with `Error::Interrupted`. That is the
cancellation mechanism; the Python binding uses it for Ctrl-C.

### 2. `SyncConfig`

```rust
pub struct SyncConfig {
    pub connect: ConnectConfig,      // connection_string, login_timeout_sec
    pub output_uri: String,
    pub checkpoint_uri: String,
    pub fetch_batch_size: usize,
    pub query_timeout_sec: Option<u64>,
}
```

`ConnectConfig` deliberately has a hand-written `Debug` implementation that redacts the
connection string, so an incidental `{:?}` in a caller's own logging cannot leak it.

The catalog is built with `SyncCatalog::new(Vec<TableSync>)`, which validates every entry
and rejects duplicates.

### 3. Module map

| Module | Responsibility |
|---|---|
| `catalog` | Per-table configuration; pure data, no I/O |
| `connect` | TDS connection setup and credential redaction |
| `types` | Source catalog type name to internal type model |
| `builders` | Decoded values to Arrow arrays; the only module naming Arrow |
| `merge` | Opening a Delta table and applying an upsert |
| `checkpoint` | The `_streamer_checkpoints` table, read and written |
| `pipeline` | Orchestration; the entry points above |
| `error` | The crate's single error type |

`<Table 4-1>` Rust module map

### 4. Error type

| Variant | Meaning |
|---|---|
| `Connect` | Could not reach the source or log in. Message is redacted |
| `Query` | A statement failed against an established session. Message is redacted |
| `IncrementalConfigMissing` | A table has no watermark column or no primary key |
| `UnrecognisedColumnType` | A source type with no mapping; reported, not fatal |
| `UnparsableValue` | A value contradicted its column's declared type |
| `UnsafeTableName` | A name unsafe to interpolate into SQL or into a path |
| `Delta` | The Delta write or merge path failed |
| `Arrow` | Arrow rejected an assembled batch; indicates a defect here |
| `Checkpoint` | The checkpoint table could not be read or written |
| `Io` | An underlying I/O failure |
| `Interrupted` | The caller asked the run to stop |
| `Internal` | An invariant between two stages was violated |

`<Table 4-2>` Error variants

No variant carries row data. `UnparsableValue` carries a column name and an expected type,
never the value that failed.

## V. Semantics That Callers Must Know

### 1. The watermark contract

The configured watermark column **must not decrease** as rows are written. This library
cannot verify that and does not try.

A column that satisfies it: an identity column, or a `DATETIME2` set by a trigger or by an
application convention on every insert and update.

A column that does not: a business date that can be backdated, a value an operator can
edit, or a timestamp maintained only on insert while updates leave it alone. Each of those
produces rows that are never synced, silently.

Precision matters as much as monotonicity. A row written with a watermark value exactly
equal to the stored checkpoint is excluded permanently, because the filter is strictly
greater-than. A `DATETIME2(7)` under a normal write rate will not collide; a `DATE`, or a
`DATETIME2(0)` under a burst, can.

### 2. Deletions

Not detected. A row deleted at the source stops appearing in the query result, which is
indistinguishable from it not having changed, and its copy in Delta remains indefinitely.

If deletions matter, the source needs a soft-delete column, which this library then syncs
like any other and which downstream queries filter on.

### 3. Naming and output paths

A qualified table name maps to a subdirectory: `dbo.customers` is written beneath
`<output_uri>/dbo/customers`. A name with no schema stays as one path component.

Names are validated before use. A quote, a semicolon, a backslash or a null byte in a
table or column name is rejected, as is a path component that is empty, `.` or `..`.

### 4. Timestamps and time zones

SQL Server's `DATETIME`, `DATETIME2` and `SMALLDATETIME` carry no time zone, and are
written as Delta `timestamp`, which is microseconds UTC. They are therefore **assumed to
already be UTC**. If the source stores local time in them, the values in Delta will be
wrong by the offset, and nothing in the pipeline can detect that.

`DATETIMEOFFSET` carries a real offset and is converted to the correct UTC instant, not
assumed.

`TIME` is written as text, because Delta has no time-of-day type.

### 5. Type fidelity

Type uncertainty degrades and never fails. A column whose SQL Server type this library does
not map is written as text and listed in `text_fallback_columns`.

Some specifics with consequences:

- `MONEY` and `SMALLMONEY` arrive as floating point from the wire and are stored as
  `double`. Use `DECIMAL` at the source where exactness matters.
- `TINYINT` is unsigned in SQL Server and widens to a 16-bit signed integer.
- T-SQL `TIMESTAMP`, also spelled `ROWVERSION`, is a row version and not a time. It is
  stored as binary.
- A decimal value that would need more fractional digits than its column declares is
  rejected rather than rounded.

### 6. Re-running and idempotency

Running a sync again is always safe. The merge is an upsert keyed on the primary key, so
re-applying a row already present updates it in place rather than duplicating it.

That is what makes recovery from a failure a matter of running the job again, with no
cleanup step: every table is either advanced, and has nothing outstanding, or unchanged,
and resumes from where it stopped.

### 7. The checkpoint table

`_streamer_checkpoints` is an ordinary Delta table and can be queried:

```sql
SELECT table_name, watermark_column, last_value, synced_at
FROM delta.`/Volumes/main/raw/mssql/_streamer_checkpoints`
ORDER BY synced_at DESC
```

Deleting a table's row causes the next run to sync that table from the beginning, which is
the supported way to force a full re-sync. Editing `last_value` by hand is possible and
inadvisable: a value ahead of reality skips rows permanently.

## VI. Worked Examples

### 1. A daily sync

```python
import os
import tiberiusdelta

TABLES = [
    {"table": "dbo.customers", "watermark_column": "updated_at", "primary_key": "id"},
    {"table": "dbo.invoices", "watermark_column": "modified", "primary_key": "invoice_id"},
]

report = tiberiusdelta.sync_tables(
    os.environ["SQLSERVER_CONNECTION_STRING"],
    "/Volumes/main/raw/mssql/",
    TABLES,
    progress=lambda e: print(f"{e['tables_done']}/{e['total_tables']} {e['table']}"),
)

for t in report.tables:
    print(t.table, t.rows_fetched, t.rows_inserted, t.rows_updated, t.text_fallback_columns)
```

[Figure 6-1] A daily sync

The connection string comes from the environment, never from a literal. On Databricks that
means a secret scope, read with `dbutils.secrets.get`.

### 2. Preflight before onboarding a schema

```python
found = tiberiusdelta.preflight(connection_string, TABLES, output_uri=OUTPUT)

for table in found:
    if not table.ready:
        print(f"{table.table}: NOT READY")
        if not table.watermark_present:
            print("  watermark column does not exist")
        for column in table.missing_primary_key_columns:
            print(f"  primary key column does not exist: {column}")
        continue

    fallbacks = [c.name for c in table.columns if not c.recognised]
    print(f"{table.table}: ready, {len(table.columns)} columns, "
          f"checkpoint={table.last_synced_value}")
    if fallbacks:
        print(f"  stored as text: {', '.join(fallbacks)}")
```

```
dbo.customers: ready, 6 columns, checkpoint=2026-09-12 23:41:07.113
dbo.invoices: ready, 14 columns, checkpoint=None
  stored as text: region_shape
dbo.audit_log: NOT READY
  watermark column does not exist
```

[Figure 6-2] Preflight output for one table

### 3. Calling from Rust

```rust
use tiberiusdelta::catalog::{SyncCatalog, TableSync};
use tiberiusdelta::connect::ConnectConfig;
use tiberiusdelta::pipeline::{self, SyncConfig};

let catalog = SyncCatalog::new(vec![TableSync {
    table: "dbo.customers".into(),
    watermark_column: "updated_at".into(),
    primary_key: vec!["id".into()],
}])?;

let output_uri = "file:///mnt/delta/mssql".to_string();
let config = SyncConfig {
    connect: ConnectConfig {
        connection_string: std::env::var("SQLSERVER_CONNECTION_STRING")?,
        login_timeout_sec: Some(30),
    },
    checkpoint_uri: format!("{output_uri}/_streamer_checkpoints"),
    output_uri,
    fetch_batch_size: 10_000,
    query_timeout_sec: Some(300),
};

let report = pipeline::run(&config, &catalog, |_| true)?;
println!("{} rows", report.total_rows_fetched);
```

## References

1. Microsoft. *Connection string syntax*.
   <https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax>
2. Microsoft. *Date and time data types (Transact-SQL)*.
   <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types>
3. The Delta Lake project. *Delta Transaction Log Protocol*.
   <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
4. delta-rs documentation. <https://docs.rs/deltalake/0.32.4/deltalake/>
5. tiberius documentation. <https://docs.rs/tiberius/0.12.3/tiberius/>
6. PyO3 user guide. <https://pyo3.rs/>
7. Databricks. *Secret management*.
   <https://docs.databricks.com/en/security/secrets/index.html>

## Appendix A. Parameter quick reference

| Parameter | Default | Raise it when | Lower it when |
|---|---|---|---|
| `fetch_batch_size` | `10_000` | Rows are narrow and merges dominate runtime | Rows are wide or memory is tight |
| `login_timeout_sec` | `30` | The network path is slow to establish | A fast failure is wanted from a scheduler |
| `query_timeout_sec` | `300` | The source is slow but genuinely working | A hung query must be caught quickly |
| `checkpoint_uri` | derived | Several output prefixes share one checkpoint table | Never; the default is almost always right |

`<Table A-1>` Parameter quick reference
