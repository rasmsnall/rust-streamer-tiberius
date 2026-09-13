# tiberiusdelta: Architecture

**Document type** Technical architecture specification
**Status** Complete and implemented. The pipeline runs end to end against a live SQL Server, behind both the Rust and the Python surface.
**Audience** Anyone integrating, operating, or modifying this library. No prior context assumed.
**Companion documents** `api.md` for the callable surface, `operations.md` for running it.
**Version** 1.0
**Date** 2026-09-13

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. Rationale
  - 3. Relationship to pgdelta
  - 4. Scope and non-goals
- II. Incremental Sync
  - 1. The watermark
  - 2. The checkpoint
  - 3. Ordering of merge and checkpoint
  - 4. What incremental sync cannot do
  - 5. Bulk backfill and handover
- III. Pipeline Architecture
  - 1. Stage overview
  - 2. Connect
  - 3. Describe
  - 4. Fetch
  - 5. Build
  - 6. Merge
  - 7. Checkpoint
- IV. Concurrency Model
  - 1. Execution model by component
  - 2. Rationale for the division
  - 3. The Global Interpreter Lock
  - 4. Scaling out
  - 5. The remaining ceiling
- V. Resource Model
- VI. Failure Model
  - 1. The consistency boundary
  - 2. Classification of faults
  - 3. Recovery
- VII. Type Mapping
  - 1. Why the catalog and not the wire
  - 2. Mapping table
  - 3. Edge cases
  - 4. Timestamps
- VIII. Security Model
- IX. Deployment Constraints
- X. Assessment
  - 1. Advantages
  - 2. Disadvantages
  - 3. Conditions under which this design is inappropriate
- XI. Dependencies
- References
- Appendix A. Glossary

### List of Tables

- `<Table 3-1>` Pipeline stages and responsibilities
- `<Table 4-1>` Execution model by component
- `<Table 6-1>` Classification of faults
- `<Table 7-1>` SQL Server to Arrow type mapping
- `<Table 11-1>` Direct dependencies
- `<Table A-1>` Glossary of terms

### List of Figures

- `[Figure 1-1]` Pipeline superseded by this library
- `[Figure 1-2]` Pipeline implemented by this library
- `[Figure 2-1]` One table's run
- `[Figure 5-1]` Determinants of peak memory
- `[Figure 6-1]` State after a failure mid-run

---

## I. Introduction

### 1. Purpose

`tiberiusdelta` streams a live Microsoft SQL Server database into Delta Lake tables,
incrementally. It is a Rust core with Python bindings, intended for use from Databricks.

Each run pulls only the rows that are new or have changed since that table's own last
successful run, and merges them into a Delta table keyed on the table's primary key.

It exists to remove both an export step and a full reload from the ingestion path.

```
SQL Server -> nightly export to files -> land the files -> reload every row -> Delta
```

[Figure 1-1] Pipeline superseded by this library

That path re-reads and rewrites the entire dataset every night regardless of how little
changed, and the export itself is a separate scheduled system that can fail silently and
leave yesterday's file in place. This library connects to the source directly and moves
only the difference.

```
SQL Server -> SELECT ... WHERE watermark > checkpoint -> Arrow -> Delta MERGE
```

[Figure 1-2] Pipeline implemented by this library

### 2. Rationale

Two properties of the workload drive the design.

The first is that the source is reachable. Unlike a delivered file, a live connection can
be asked precisely what changed, which makes the cost of a run proportional to the change
rather than to the dataset. A table of fifty million rows with a thousand updates a day
transfers a thousand rows.

The second is that the source is trusted and in production. It is the organization's own
system, so the structural paranoia appropriate to a file from a third party is
misdirected here; but it is serving real users, so a runaway query is a far more serious
failure than it would be against a file. The security model in Chapter VIII follows from
that inversion, not from a generic checklist.

### 3. Relationship to pgdelta

This is a sibling project to `pgdelta` (`rust-streamer-pgdb`), not a variant of it. It
shares that project's engineering discipline, its Arrow and Delta write path, and much of
its documentation convention, and deliberately diverges on three foundational
assumptions.

| | pgdelta | tiberiusdelta |
|---|---|---|
| Input | A delivered dump file | A live network connection |
| Credentials | Moot (no subprocess, no connection string) | A first-class concern |
| Load shape | Full reload, all-or-nothing per run | Incremental: only new or changed rows |

Each divergence has consequences that run through the whole design: the threat model
(Chapter VIII), the failure model (Chapter VI), and the fact that this library has a
per-table configuration surface at all (Chapter II).

### 4. Scope and non-goals

In scope: reading tables a caller has explicitly configured, resolving their types,
fetching rows new or changed since the last run, and merging them into Delta.

Out of scope, deliberately:

- **Deletion detection.** See Chapter II, Section 4.
- **Discovering which tables to sync.** A table absent from the caller's configuration is
  not synced. This library never guesses which tables exist or which of their columns
  would make a suitable watermark, because across an unfamiliar schema that guess is
  exactly the kind of silent assumption that produces a table which looks synced and is
  not.
- **Cross-table transactional consistency.** See Chapter VI, Section 1.
- **Change Data Capture.** SQL Server's own CDC and Change Tracking features would give
  deletions and exact change sets, at the cost of requiring them to be enabled on the
  source, which is a change to a production system this library does not assume it can
  make. See Chapter X, Section 3.

## II. Incremental Sync

### 1. The watermark

Each configured table names a **watermark column**: a column that does not decrease as
rows are written, typically a `DATETIME2` updated on every write or a strictly increasing
identity column. The query is always

```
SELECT * FROM <table> WHERE <watermark_column> > <last_value> ORDER BY <watermark_column> ASC
```

The `ORDER BY` is not decoration. Because rows arrive in ascending watermark order, the
watermark of the last row fetched is necessarily the greatest one seen, so this library
never has to compare two watermark values itself. That matters because the watermark
column's type is the caller's choice: it may be a timestamp, an integer, or a string. By
delegating the comparison to the source's own `ORDER BY`, the implementation stays correct
for a column whose type it does not even map natively (Chapter VII).

The library cannot verify that the configured column actually has the monotonic property.
That is the caller's responsibility, and getting it wrong is the sharpest failure mode
this design has: a column that can decrease will silently skip rows forever.

### 2. The checkpoint

Each table's last synced watermark value is recorded in **its own** Delta table, at
`<checkpoint_uri>/<schema>/<table>`, mirroring the layout of the data tables themselves.

One table per source table, rather than one shared table with a row each, is the choice
that makes Chapter IV, Section 4 possible. A shared checkpoint table is a single Delta
table every sync must commit to, so workers running at once contend on it and Delta
resolves that by failing one of them. Per-table checkpoints remove the shared mutable
resource, so nothing needs coordinating. The cost is that one query no longer shows every
checkpoint; each still carries its own `table_name`, so a union view over the prefix
restores that.

| Column | Meaning |
|---|---|
| `table_name` | Qualified source table name; the merge key |
| `watermark_column` | The column that was filtered on to produce `last_value` |
| `last_value` | The watermark value, as text, up to which the table has been synced |
| `synced_at` | When this checkpoint was recorded, microseconds since the epoch, UTC |

`last_value` is stored as text whatever the watermark column's type, and bound back into
the next run's `WHERE` clause as a parameter. SQL Server converts it to the column's own
type for the comparison. Text is used rather than a typed column because the checkpoint
table holds every synced table's watermark in one place, and those tables need not agree
on a type.

### 3. Ordering of merge and checkpoint

For each table, in this order, without exception:

```
read checkpoint -> fetch rows -> MERGE into Delta (commits) -> advance checkpoint
```

[Figure 2-1] One table's run

The ordering is the central safety property of the design, and the reverse is not merely
worse but unsafe. If the process dies between the merge committing and the checkpoint
advancing, the next run re-fetches and re-merges rows that were already applied; because
`MERGE` is an upsert keyed on the primary key, re-applying a row that is already present
updates it in place rather than duplicating it, so the outcome is identical. If the
checkpoint were advanced first, a death in the same window would permanently skip rows
that were never merged at all, with no signal.

Idempotency of the merge is therefore not an incidental property. It is what makes crash
recovery a matter of running the job again.

### 4. What incremental sync cannot do

Stated plainly here rather than discovered during an incident.

- **Deletions are invisible.** A row deleted at the source simply stops appearing in the
  result of a `WHERE watermark > last_value` query, which is indistinguishable from it not
  having changed. Its copy in Delta remains, indefinitely. Detecting deletions requires
  either a soft-delete convention in the source schema, which this library can then sync
  like any other column, or engine-level change tracking, which is out of scope.
- **A tie on the watermark is permanent.** The filter is strictly greater-than, so a row
  whose watermark value is exactly equal to the stored checkpoint is excluded. Within one
  run this is harmless: ascending order plus exhausting the result set means every row
  tied at the maximum is fetched together and the checkpoint advances past all of them at
  once. Across runs it is not: if a row is later written with a watermark value equal to
  an already-recorded checkpoint, which can happen when the column's precision is coarser
  than the write rate, that row is never seen again. This was observed in development, not
  theorised. The mitigation is a caller responsibility: choose a watermark precise enough,
  or strictly monotonic enough, that genuine ties cannot occur.
- **A row updated without touching its watermark is invisible.** Follows from the same
  mechanism, and is the reason a `DATETIME2` maintained by a trigger or by an application
  convention is only as reliable as that trigger or convention.

### 5. Bulk backfill and handover

Seeding a very large table one row at a time is the one case this design is poor at, and
the answer is not to make it parallel but to not do it. The table is loaded by a bulk
export, and this library is then told where that load got to:

```
capture watermark -> bulk export and load by other means -> record checkpoint -> incremental from there
```

`pipeline::source_watermark` and `pipeline::set_checkpoint` are the two ends of that, and
neither syncs anything. Correctness rests entirely on capturing the watermark *before* the
export: read afterwards, it sits ahead of rows written during the export and those rows are
skipped permanently; read first, they are merely re-fetched, which the idempotent merge
makes harmless. The failure is safe in one direction and silent in the other.

## III. Pipeline Architecture

### 1. Stage overview

| Stage | Module | Responsibility |
|---|---|---|
| Connect | `connect` | Open one TDS session; keep credentials out of every error |
| Describe | `pipeline`, `types` | Resolve each column's type from the source catalog |
| Fetch | `pipeline` | Stream rows matching the watermark filter |
| Build | `builders` | Convert decoded values into Arrow arrays |
| Merge | `merge` | Upsert one batch into the table's Delta table |
| Checkpoint | `checkpoint` | Record how far the table has been synced |

`<Table 3-1>` Pipeline stages and responsibilities

`catalog` holds the per-table configuration the whole run is driven by, and `error` the
single error type every stage returns.

### 2. Connect

Connectivity is `tiberius`, a pure-Rust implementation of the TDS protocol SQL Server
speaks natively. There is no driver manager and nothing to install beyond the compiled
binary: the connection is a TCP socket this library opens itself, over which `tiberius`
performs the TDS login and, if configured, a TLS handshake.

An earlier version of this library used ODBC, chosen while the source engine was still
unconfirmed so that one code path would work against any of several engines. Once the
engine was confirmed as SQL Server, that generality bought nothing and cost a
deployment-time driver install plus a class of driver-specific defects: every value
arrived as text, and the legacy Windows driver misreported `DATETIME2` columns as strings.
See Chapter X, Section 1.

One connection serves a whole run. A run covering many tables would otherwise pay a TDS
login, and possibly a TLS handshake, per table.

### 3. Describe

Before any row is fetched, each table's columns are read from
`INFORMATION_SCHEMA.COLUMNS`, in `ORDINAL_POSITION` order, which is the order `SELECT *`
produces them. Chapter VII, Section 1 explains why the catalog rather than the wire type.

The resolved column list determines the Arrow schema, which determines the Delta table's
schema. It is fixed before the first row arrives, which is necessary rather than merely
convenient: the common case for an incremental sync is that nothing changed, and a run
that fetches zero rows must still be able to open, or create, its Delta table.

### 4. Fetch

Rows stream. `tiberius` yields one decoded row at a time rather than materialising a
result set, and this library accumulates them into batches of `fetch_batch_size` rows,
merging each batch before continuing. Peak memory is therefore a function of the batch
size and the row width, and not of the table size (Chapter V).

Each wait for the next row is bounded by `query_timeout_sec`. This is deliberately a
per-row deadline rather than a whole-query one: what needs bounding is a query that has
stopped making progress, whereas a legitimately large table that is still delivering rows
must not be killed for being large.

### 5. Build

Values arrive already decoded into their real types: an `i32`, an `f64`, a decimal as an
unscaled integer with a scale, a `chrono` date or timestamp. There is no text to parse,
which removes an entire category of failure that both pgdelta and this library's own ODBC
predecessor had to handle. What remains is arithmetic: rescaling a decimal to its column's
declared scale, and counting days or microseconds from an epoch. Every one of those
conversions is checked rather than cast, so a value that cannot be represented is reported
rather than silently wrapped.

A column whose type has no native Arrow mapping is written as text, faithfully, and
reported in the run statistics. This is the same fidelity policy pgdelta applies, for the
same reason: across an unfamiliar schema the type zoo is wide, and one unmapped column
must not stop every other table from syncing.

### 6. Merge

Each batch is applied with `DeltaTable::merge`, matching on the configured primary key,
updating every column of a matched row and inserting an unmatched one. This is one atomic
Delta commit.

Using Delta's own merge, rather than hand-rolling the file rewriting it implies, is the
reason this library depends on DataFusion. That is a heavy dependency and a deliberate,
documented exception to the project's minimal-dependency default; reimplementing the
merge by hand would be materially riskier than accepting it.

### 7. Checkpoint

After a table's merges have committed, and only then, its checkpoint advances. The
checkpoint table is itself a Delta table written through the same merge path, keyed on the
table name, so a second run for the same table replaces its row rather than accumulating
history.

Reading it back parses the table's visible Parquet files directly rather than going
through a query engine. DataFusion is already present for the merge, but wiring a table
provider for a table with one row per synced table would be more moving parts for no
benefit.

## IV. Concurrency Model

### 1. Execution model by component

| Component | Executes on |
|---|---|
| `pipeline::run`, `pipeline::preflight` | The calling thread, driving a private Tokio runtime |
| `connect`, `pipeline::sync_table` | Async; a Tokio runtime |
| `merge`, `checkpoint` | Async; the same runtime |
| `types`, `builders`, `catalog` | Synchronous; whichever thread calls them |
| The Python progress callback | The calling thread, with the GIL re-acquired |

`<Table 4-1>` Execution model by component

### 2. Rationale for the division

The whole pipeline is asynchronous, which is a simplification relative to both pgdelta and
this library's ODBC predecessor. delta-rs exposes an async-only API, so a Tokio runtime
exists regardless; `tiberius` is also async, so the read side runs on that same runtime
instead of being a blocking call dispatched from inside async code.

Tables are synced sequentially. Nothing prevents syncing several concurrently, and a run
covering hundreds of small tables would benefit, but sequential execution is what makes
the failure model in Chapter VI simple to state and simple to reason about during an
incident, and no workload has yet required otherwise.

### 3. The Global Interpreter Lock

The Python binding releases the GIL for the duration of the sync and re-acquires it only
to invoke the caller's progress callback, once per table. The same callback re-checks
Python's signal state, which is what allows Ctrl-C to stop a sync that would otherwise run
for hours. An exception raised by the callback, or a pending signal, is preserved and
re-raised rather than being flattened into a generic interruption, so the caller sees the
real cause.

### 4. Scaling out

One process syncs tables sequentially, which measurement says is enough for most
workloads: several hundred unchanged tables in seconds, and rows at a few hundred thousand
a second (see `operations.md`, Chapter IV, Section 2).

When it is not enough, throughput scales with workers rather than with new code.
`tiberiusdelta.distributed` spreads the table list across Spark executors, each syncing
its share exactly as a single process would. Nothing in the sync engine knows about it.

That works because the design is shared-nothing all the way down, which was deliberate
rather than lucky:

- No cross-table transaction, so tables need no coordination.
- One Delta table per source table, so no two workers write the same data.
- **One checkpoint Delta table per source table**, so no two workers write the same
  checkpoint either. This is the property that makes the rest usable: a single shared
  checkpoint table would be one Delta table every worker had to commit to, and Delta
  resolves that contention by failing writers. Removing the shared writer removed the need
  to coordinate at all.
- Idempotent merges, so Spark's own task retries are safe with no special handling.

### 5. The remaining ceiling

What does not scale this way is a *single* very large table, since one table is one unit of
work. Splitting its watermark range across workers is possible, but they would then all
merge into the same Delta table and the contention returns; the sound version stages
Parquet per range and makes one commit, which is a different design.

That is deliberately not built, because for the case it addresses, seeding a very large
table, a bulk file export is simply better: it stops paying per-row protocol costs
entirely, and is several times faster than any live query however parallel. The library
supports that route directly through a checkpoint handover rather than trying to beat it;
see Chapter II, Section 5.

## V. Resource Model

Peak memory is bounded and does not scale with table size.

```
peak memory ~ fetch_batch_size x row width
             + the Arrow batch being built
             + the Parquet writer's own buffers
```

[Figure 5-1] Determinants of peak memory

A table of fifty million rows and a table of fifty thousand cost the same footprint at the
same batch size; only wall-clock time differs, and for an incremental run even that is a
function of how much changed rather than of how much exists.

The one exception is the first run of a table, which has no checkpoint and therefore
fetches every row. That run is a full load, with a full load's runtime, and is worth
planning for explicitly when onboarding a large table.

## VI. Failure Model

### 1. The consistency boundary

The atomic unit is **one table's merge**. Delta has no cross-table transaction, and this
library does not attempt to simulate one.

Consequences, stated rather than discovered:

- A run that fails partway leaves the tables it already synced advanced and committed, and
  the rest untouched at their previous checkpoints.
- A reader querying two tables during a run can see one at this run's state and the other
  at the previous run's.
- Each table is queried independently, at a different moment, so even a wholly successful
  run does not represent a single instant at the source.

```
table 1  merged, checkpoint advanced     <- visible, current
table 2  merged, checkpoint advanced     <- visible, current
table 3  failed                          <- unchanged, previous checkpoint intact
table 4  never attempted                 <- unchanged
```

[Figure 6-1] State after a failure mid-run

If a consumer ever requires a consistent cross-table snapshot, the mechanism is a source
side snapshot isolation transaction spanning the whole run, which is not currently
designed; see Chapter X, Section 3.

### 2. Classification of faults

| Class | Examples | Outcome |
|---|---|---|
| Configuration | Missing watermark column, duplicate table, unsafe identifier | Rejected before any connection opens |
| Connectivity | Unreachable host, refused login, login timeout | The run fails; nothing was written |
| Query | A table that does not exist, a stalled query hitting its timeout | The run fails at that table; earlier tables keep their progress |
| Type contradiction | A fetched value disagreeing with its column's declared type | The run fails at that table |
| Type uncertainty | A source type with no Arrow mapping | Degrades to text; reported, never fails |
| Storage | A Delta commit conflict or object-store failure | The run fails at that table |

`<Table 6-1>` Classification of faults

The distinction between the last two rows is the fidelity policy: uncertainty about how
best to represent a type degrades, whereas a value actively contradicting its own declared
type is a structural disagreement with the source and fails loudly. On a live connection
the latter most often means a schema change landed between the catalog lookup and the
fetch.

### 3. Recovery

Run it again. Every table's checkpoint is either advanced, in which case that table has
nothing outstanding, or unchanged, in which case the next run re-fetches from where it
left off. Re-merging rows already merged is idempotent.

There is no cleanup step, no partial state to reconcile by hand, and no scenario in which
running the job twice is worse than running it once.

## VII. Type Mapping

### 1. Why the catalog and not the wire

`tiberius` reports a type for every column of a result set, and it would be natural to
drive the Arrow schema from it. That does not work, for a reason worth recording because
it is not obvious.

The TDS wire type collapses exactly the distinctions a schema needs. A **nullable** `INT`
arrives as `Intn`, an n-byte integer, not as `Int4`; its actual width lives in TDS
metadata the library does not expose. `DECIMAL` and `NUMERIC` arrive as `Decimaln` and
`Numericn` without the column's declared precision and scale, and although a decoded value
carries its own scale, and can report the number of digits it happens to use, neither
describes the column. Since most columns in a production schema are nullable, and since
the schema must be fixed before the first row arrives, the wire type cannot be the source
of truth.

`INFORMATION_SCHEMA.COLUMNS` answers exactly this, is standard T-SQL, and costs one round
trip per table. A useful side effect is that this library's type module became a close
sibling of pgdelta's: both map a catalog's own type name to the same internal model.

### 2. Mapping table

| SQL Server | Arrow | Delta |
|---|---|---|
| `TINYINT` | `Int16` | `short` |
| `SMALLINT` | `Int16` | `short` |
| `INT` | `Int32` | `integer` |
| `BIGINT` | `Int64` | `long` |
| `BIT` | `Boolean` | `boolean` |
| `REAL` | `Float64` | `double` |
| `FLOAT` | `Float64` | `double` |
| `MONEY`, `SMALLMONEY` | `Float64` | `double` |
| `DECIMAL(p,s)`, `NUMERIC(p,s)`, p<=38 | `Decimal128(p,s)` | `decimal(p,s)` |
| `DATE` | `Date32` | `date` |
| `TIME` | `Utf8` | `string` |
| `DATETIME`, `DATETIME2`, `SMALLDATETIME` | `Timestamp(Micros, UTC)` | `timestamp` |
| `DATETIMEOFFSET` | `Timestamp(Micros, UTC)` | `timestamp` |
| `BINARY`, `VARBINARY`, `IMAGE` | `Binary` | `binary` |
| `TIMESTAMP`, `ROWVERSION` | `Binary` | `binary` |
| `CHAR`, `VARCHAR`, `TEXT`, `NCHAR`, `NVARCHAR`, `NTEXT` | `Utf8` | `string` |
| `UNIQUEIDENTIFIER`, `XML` | `Utf8` | `string` |
| Anything else | `Utf8`, and reported | `string` |

`<Table 7-1>` SQL Server to Arrow type mapping

### 3. Edge cases

Each of these is a decision, not an accident.

- **`TINYINT` widens to `Int16`.** SQL Server's `TINYINT` is unsigned, spanning 0 to 255,
  which does not fit a signed 8-bit type. The wider type holds every value exactly.
- **`TIME` becomes text.** Delta Lake has no time-of-day type. Arrow's `Time64` builds
  perfectly happily and is then rejected at commit, which was found by committing one
  rather than by reading a specification. The literal is preserved, losslessly.
- **`TIMESTAMP` is not a timestamp.** In T-SQL, `TIMESTAMP` (also spelled `ROWVERSION`) is
  an eight-byte row version with no temporal meaning whatever. Mapping it by name would
  produce a column of plausible-looking and entirely fictitious dates.
- **`MONEY` becomes a float, not a decimal.** `tiberius` decodes both money types by
  dividing the wire's scaled integer by ten thousand in floating point, so exactness is
  already gone before this library sees the value. Declaring an Arrow decimal would claim
  a fidelity the data no longer has. Prefer `DECIMAL` over `MONEY` in a source schema
  where exactness matters.
- **A decimal that would lose digits is rejected, not rounded.** If a value arrives at a
  finer scale than its column declares, that is a real disagreement about what the column
  means, and quietly dropping digits off money is the kind of silent corruption the error
  policy exists to prevent.
- **A decimal too wide for its declared precision is rejected.** The Arrow builder does not
  check this on every path, and an over-wide value committed to Delta reads back wrong
  rather than failing, so it is checked before it reaches the builder.
- **A NULL is accepted from any variant.** `tiberius` picks the decoded variant for a NULL
  from the declared width in the TDS metadata, falling back to a 64-bit one when that
  width is unfamiliar. Since a NULL carries no information, accepting it into any builder
  is lossless; a non-null value of the wrong width is a different matter and is rejected.

### 4. Timestamps

Every timestamp column is written as Arrow `Timestamp(Microsecond, UTC)`, which delta-rs
maps to Delta's `timestamp`. An Arrow timestamp with **no** timezone would instead map to
`timestamp_ntz`, which requires reader version 3 and writer version 7 and would break the
compatibility floor in Chapter IX.

`DATETIME`, `DATETIME2` and `SMALLDATETIME` carry no timezone of their own and are
therefore **assumed to already be UTC**. This assumption is documented at the Python
surface because it cannot be verified and can be wrong.

`DATETIMEOFFSET` does carry a real offset, and is converted rather than assumed. Doing
that correctly required going around one of `tiberius`'s own conversions: TDS stores the
datetime part of a `DATETIMEOFFSET` already in UTC, carrying the offset alongside only so
the original local rendering can be reconstructed, and `tiberius`'s `DateTime<Utc>`
conversion subtracts that offset from it anyway, moving the instant by the offset a second
time. A value written as 13:45:30+02:00, whose true UTC instant is 11:45:30, came back as
09:45:30. Its `DateTime<FixedOffset>` conversion attaches the offset instead of
subtracting it and leaves the instant correct, so that is the one this library uses. A unit
test pins the behaviour down so that an upstream fix to one and not the other fails loudly
instead of silently shifting every timestamp.

## VIII. Security Model

The source is trusted; the credentials and the blast radius are not. That inverts
pgdelta's emphasis, where the input was hostile and credentials did not exist.

1. `#![forbid(unsafe_code)]` at the crate root.
2. **The connection string is a secret.** It carries the password. It must come from an
   environment variable or a secret store, never a literal in source or a notebook cell.
3. **It is scrubbed from every error path.** A connection failure can echo the string it
   was given verbatim. Every error this library produces passes through a redaction step
   that removes both the whole connection string and, separately, the password value
   parsed out of it, so that an error quoting only the password is caught too. This runs
   before the error reaches a Python traceback or a log.
4. **Read-only is a provisioning requirement, not a code guarantee.** This library issues
   only `SELECT`, but TDS has no client-side concept of a read-only session, so that is a
   code-review property rather than an enforced one. The account used to connect must be
   granted `SELECT` only. This is the deploying organization's responsibility and cannot
   be delegated to the client.
5. **Fetch batches are bounded**, so one very large table cannot exhaust memory or hold a
   server-side cursor open indefinitely.
6. **Queries have a deadline**, so an unattended sync cannot hang forever against a live
   production system.
7. **Row data is never logged**, and no error variant carries a value. Errors carry column
   names and expected types only.
8. **Integer and decimal conversions are checked throughout**; nothing wraps silently.
9. **Identifiers are validated before interpolation.** Table and column names cannot be
   bound as query parameters and necessarily become part of the SQL text. They come from
   the caller's own configuration rather than from data, so this is defence in depth
   rather than a response to untrusted input, but a name containing a quote, a semicolon,
   a backslash or a null byte is rejected rather than sanitised. Values are always bound
   as parameters, never interpolated.
10. **Output paths are validated.** A qualified table name maps to a subdirectory, and a
    path component that is empty, `.` or `..` is rejected.

## IX. Deployment Constraints

- Write to an **external location or a `/Volumes/...` path**, never a Unity Catalog
  *managed* table. External writers can corrupt UC-managed tables.
- The output stays at **reader version 1 and writer version 2**: no deletion vectors, no
  column mapping, no `timestamp_ntz`. Any Databricks Runtime can read the result.
- Network reachability from the compute running this to the SQL Server instance is a
  prerequisite, and in a cloud deployment usually the hardest part: it typically means
  private networking between the Databricks workspace and the database, not a public
  endpoint.
- Schedule `VACUUM`. Merges rewrite files, and the superseded ones remain until vacuumed.

## X. Assessment

### 1. Advantages

- **Cost proportional to change.** The defining property. A large table with few daily
  changes transfers few rows.
- **No intermediate system.** No export job, no landing zone, no file to arrive late or
  not at all.
- **Nothing to install.** `tiberius` is pure Rust speaking TDS over a socket. No ODBC
  driver manager, no driver binary, no C toolchain, on any platform.
- **Bounded memory** independent of table size.
- **Safe to re-run**, always, with no cleanup step.
- **High type fidelity.** Values are decoded from the wire into real types rather than
  being rendered as text and parsed back.

### 2. Disadvantages

- **Deletions are not detected.** The most significant limitation, and the one most likely
  to surprise.
- **Correctness depends on a caller's assertion** that the watermark column is monotonic,
  which cannot be verified.
- **A watermark tie loses rows permanently.** See Chapter II, Section 4.
- **No cross-table consistency.**
- **It touches production.** A misconfigured sync can put real load on a live system.
- **Per-table configuration is manual**, which is a real cost across a schema of hundreds
  of tables.
- **Two `rustls` versions** are linked into one binary, because `tiberius` and `deltalake`
  resolve different majors. Wasteful, not incorrect: the two TLS stacks never interoperate.

### 3. Conditions under which this design is inappropriate

- **Deletions matter and there is no soft-delete column.** Use CDC, or a periodic full
  reload, or accept that deleted rows persist.
- **No table has a usable watermark.** Full reload is the honest answer.
- **A consistent cross-table snapshot is required.** This design cannot provide one.
- **The source cannot be reached, or must not be queried directly.** That is pgdelta's
  problem shape, not this one's.
- **Sub-minute latency is required.** This is a batch design. Streaming replication is a
  different architecture.

## XI. Dependencies

| Crate | Why |
|---|---|
| `pyo3` | Python bindings |
| `tiberius` | Native async TDS client. No driver install; typed values off the wire |
| `tokio-util` | Bridges Tokio's IO traits to the ones `tiberius` expects |
| `deltalake` (delta-rs), with `datafusion` | Delta write path and `MERGE`. The one deliberate exception to minimal dependencies |
| `chrono` | Converting decoded temporal values into the integers Arrow stores |
| `tokio`, `futures` | The async runtime and stream combinators |

`<Table 11-1>` Direct dependencies

Arrow is used through `deltalake`'s own re-exports rather than as a direct dependency, so
its version cannot skew from the one delta-rs pins.

Deliberately not used: `thiserror`, in favour of a hand-rolled error enum, matching
pgdelta; and `odbc-api`, which this library used until the source engine was confirmed.

## References

1. Microsoft. *MS-TDS: Tabular Data Stream Protocol*.
   <https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/>
2. Microsoft. *INFORMATION_SCHEMA.COLUMNS (Transact-SQL)*.
   <https://learn.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/columns-transact-sql>
3. Microsoft. *Date and time data types (Transact-SQL)*.
   <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types>
4. The Delta Lake project. *Delta Transaction Log Protocol*.
   <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
5. delta-rs documentation. <https://docs.rs/deltalake/0.32.4/deltalake/>
6. tiberius documentation. <https://docs.rs/tiberius/0.12.3/tiberius/>
7. Apache Arrow. *Arrow Columnar Format*.
   <https://arrow.apache.org/docs/format/Columnar.html>
8. PyO3 user guide. <https://pyo3.rs/>

## Appendix A. Glossary

| Term | Meaning |
|---|---|
| TDS | Tabular Data Stream, the wire protocol SQL Server speaks |
| Watermark column | The column filtered on to find rows new or changed since the last run |
| Checkpoint | The recorded watermark value a table has been synced up to |
| Merge / upsert | A Delta operation that updates rows matching a key and inserts those that do not |
| abi3 | A stable Python ABI; one compiled wheel loads on 3.10 and later |
| Preflight | Checking configuration against the live source without writing anything |
| Text fallback | Writing a column as text because its source type has no native mapping |
| Reader/writer version | Delta protocol versions a table requires of anything reading or writing it |

`<Table A-1>` Glossary of terms
