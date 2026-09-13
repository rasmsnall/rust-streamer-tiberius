# tiberiusdelta

Rust core, Python bindings. Streams a **live SQL Server database** incrementally into
Delta Lake tables for use from Databricks: each run pulls only rows new or changed since
the last successful run, not a full reload.

This is a sibling project to `rust-streamer-pgdb` (pgdelta), not a variant of it. Three
of pgdelta's foundational assumptions are flipped here, and the design follows from that:

| | pgdelta | tiberiusdelta |
|---|---|---|
| Input | A delivered dump file | A live network connection |
| Credentials | Moot (no subprocess, no connection string) | A first-class concern again |
| Load shape | Full reload, all-or-nothing per run | Incremental: only new/changed rows |

Status: **implemented end to end on `tiberius`.** An earlier ODBC-based version of this
crate ran end to end first; it has now been replaced by a native `tiberius`
implementation, since the source engine is confirmed as SQL Server specifically (see "Why
tiberius, not ODBC"). Every gate passes against a real SQL Server instance: 64 unit
tests, 4 live end-to-end tests, doctests, `fmt`, `clippy -D warnings`, and
`RUSTDOCFLAGS=-D warnings cargo doc`. The wheel builds, and `tools/smoke.py` round-trips
a live table through it. The three long-form documents are written.

## Goals / constraints (from the user, verbatim intent)

- Rust core, Python bindings, target consumer is Databricks: same as pgdelta.
- The source engine was originally unconfirmed, so this crate was built engine-agnostic
  first (see "Why tiberius, not ODBC"). **It is now confirmed: SQL Server (T-SQL).**
- Direct, trusted, live network access to the source is assumed available (the user's
  own instruction: "just think we have direct access"). Not yet verified against the
  real production instance.
- Incremental sync, not full reload: decided explicitly with the user over the
  alternative (full reload every run, pgdelta-style), accepting the larger feature and
  the sharper failure mode (a wrong watermark silently loses or duplicates rows) in
  exchange for not re-scanning the whole source every run.
- Switch the connectivity layer from ODBC to `tiberius` (native, decided with the user)
  now that the engine is confirmed, trading a working ODBC implementation for one that
  needs no driver install and returns natively typed values instead of text.
- Same engineering discipline as pgdelta: security considered first, minimal dependency
  surface *except where a real correctness need justifies an exception* (see Dependency
  budget), code not over-commented, documentation written with each item.

## Why tiberius, not ODBC

This crate was first built against ODBC, a driver-manager protocol most enterprise
databases (SQL Server, MySQL, PostgreSQL, Oracle, and many others) support, so one code
path would work regardless of which engine the source turned out to be. That cost the
actual per-engine ODBC driver being a **deployment-time install on the machine that runs
this**, not something Cargo can vendor, the same class of external dependency
`fast-gzip`/`zstd` are for pgdelta (there, a C toolchain; here, a driver binary).

**The engine is now confirmed as SQL Server**, which settles the question the section
above existed to hedge against. Decided with the user: switch to `tiberius`, a
pure-Rust, async implementation of the TDS protocol built specifically for SQL Server.
What that buys:

- **No driver install.** `tiberius` opens a plain TCP socket itself; nothing beyond the
  compiled binary needs to exist on the machine that runs this, on any platform.
- **Natively typed values, not text.** `tiberius` decodes each column to its real TDS
  type directly off the wire (see "Driver notes" below), removing the whole
  text-round-trip class of quirks the ODBC implementation had to work around: no more
  `DATETIME2` misreported as a string, no more a decimal's leading zero disappearing.
- **One async runtime.** The SQL Server read runs on the same Tokio runtime as the Delta
  write, instead of ODBC's blocking calls dispatched from inside async code the way
  `crate::pipeline` previously had to arrange.

Cost: `connect.rs`, `types.rs`, `builders.rs` and the fetch half of `pipeline.rs` were
rewritten from their working, tested ODBC-based versions, and `values.rs` was deleted
outright rather than ported, since there is no text left to parse. The Delta-side modules
(`merge.rs`, `checkpoint.rs`, `catalog.rs`, `error.rs`) were always driver-agnostic and
came through the switch unchanged, which is most of the hard, tested logic. All of it is
re-verified against a live instance; see Open items for what remains.

## Pipeline

```
tiberius connection (TCP + TDS, no driver manager)
  -> per table: INFORMATION_SCHEMA.COLUMNS lookup -> Arrow schema (see "Driver notes"
     for why the catalog, and not the TDS wire type, is what the schema is built from)
  -> per table: SELECT ... WHERE <watermark_column> > <last_synced_value>
  -> row batches -> deltalake::arrow builders, fed tiberius's already-decoded typed
     values: no DDL text to parse and no text values to reparse, unlike pgdelta and
     unlike the earlier ODBC-based version of this crate
  -> DeltaTable::merge, keyed on the table's primary key (upsert: new rows insert,
     matching rows update)
  -> checkpoint recorded in `_streamer_checkpoints` (table, watermark_column,
     last_value, synced_at), the same role pgdelta's `_pgdelta_loads` plays
```

Read at the start of a run, advanced only after that table's merge has committed; see
`src/merge.rs`'s module docs for why that ordering is what keeps a crash mid-run safe to
retry. Checkpoint/merge atomicity is per table, not across the whole run; see Open items.

## What incremental sync does not do

Stated plainly, pgdelta-style, rather than discovered during an incident:

- **Cannot detect deletions.** A row removed from the source simply stops appearing in
  its `WHERE watermark_column > last_value` result, with no signal at all. Real delete
  detection needs a soft-delete convention in the source schema, or engine-level change
  tracking (SQL Server Change Tracking/CDC), which is a different, larger feature this
  version does not attempt.
- **Needs a watermark column and a primary key per table.** A monotonic or timestamp
  column to filter on, and a key to merge against. A table with neither cannot be synced
  incrementally; it must fall back to a full reload for that table alone, or be excluded.
  This is per-table configuration, not a global assumption, since different tables in an
  unfamiliar production schema will have different (or no) natural watermarks.
- **Not yet verified against the real source.** Everything here is designed against a
  local test SQL Server instance standed up for development; it has not been run
  against the real production instance, because that instance's schema, watermark
  columns and primary keys are still unknown.
- **A row written with exactly the same watermark value as the current checkpoint is
  permanently invisible to every future sync.** `src/pipeline.rs` filters strictly
  `WHERE watermark_column > last_value`, so a tie is excluded, not included. Within one
  run this is harmless: the query's `ORDER BY watermark_column ASC` and exhausting the
  whole cursor before advancing the checkpoint means every row tied at the maximum
  value is fetched and merged together, and the checkpoint only advances past all of
  them at once. The real risk is *across* runs: two rows written far apart in wall-clock
  time can still collide on the same stored value if the watermark's precision is
  coarser than the actual write rate (a `DATETIME2` truncated to seconds under
  concurrent writes, for instance), and the second one, if it ever lands exactly on an
  already-checkpointed value, is silently skipped forever. Found empirically (a leftover
  row from an earlier, interrupted test run happened to share a timestamp with a freshly
  inserted one, and the tie visibly broke the next sync), not reasoned about. Mitigation
  is a caller responsibility for now: pick a watermark column with enough precision, or a
  strictly monotonic one (an autoincrement id), that genuine ties cannot occur; this
  crate does not yet detect or defend against them itself.

## Threat model

Different from pgdelta's, and worth stating explicitly rather than silently inheriting
pgdelta's "the input is untrusted, credentials are moot" framing:

- The source is assumed **trusted** (the organization's own system, direct access), not
  an untrusted third party. Structural paranoia about hostile table names and field
  bytes matters less here than it does for pgdelta.
- **Credentials are back to being a first-class concern**, the opposite of pgdelta. A
  connection string/password must never reach argv, a log line, or an error message.
  Passed via environment or a secret store, never hardcoded, never echoed on failure.
- **This hits a live production system.** A bug here has a blast radius pgdelta's
  file-based input never had: a runaway query can slow down or lock a table real users
  of the source system depend on. This crate only ever issues `SELECT` (see
  `src/connect.rs`), but `tiberius`, like any TDS client, enforces nothing itself; the
  actual guarantee has to come from provisioning a `SELECT`-only database account. Fetch
  batches must stay bounded so one huge table cannot spike memory or hold a server-side
  cursor open indefinitely.
- **Cross-table consistency is weaker than a single dump file's.** Each table is queried
  independently; without a snapshot/consistent-read transaction spanning the whole run
  (not yet designed), different tables can reflect different moments in time within the
  same run, the same way pgdelta's own Phase 2 commit burst is not atomic across tables,
  but for a different underlying reason.

## Dependency budget

| Crate | Why |
|---|---|
| `pyo3` | Python bindings |
| `tiberius` | Native async TDS (SQL Server wire protocol) client. `default-features = false, features = ["tds73", "rustls", "tokio", "chrono"]`: `rustls` keeps the TLS stack pure-Rust, `chrono` gives typed date/time decoding, and `tds73` is what makes `DATE`/`TIME`/`DATETIME2`/`DATETIMEOFFSET` decode at all (it is in the default feature set this disables wholesale, so it has to be named back). `winauth` is deliberately left off: this authenticates with a SQL Server login, not Windows integrated auth. Replaces `odbc-api` now that the engine is confirmed as SQL Server specifically |
| `chrono` | Converting tiberius's decoded temporal values into the epoch-relative integers Arrow stores. Single semver line (0.4) shared with what both tiberius and deltalake already resolve, so there is no version to skew |
| `tokio-util` | `compat_write()`, bridging `tokio`'s `AsyncWrite` to the `futures`-style IO `tiberius` expects, since `tiberius` is runtime-agnostic and does not open its own socket |
| `deltalake` (delta-rs), **with the `datafusion` feature** | Delta write path, and `DeltaTable::merge` for transactional upsert by key. This is the one deliberate exception to "minimal dependency surface": DataFusion is a full query engine, much heavier than anything pgdelta carries, but reimplementing Delta's file-rewrite-on-merge logic by hand is a materially riskier undertaking than accepting it. Revisit if delta-rs ever ships a lighter merge path |
| `tokio`, `futures` | Delta's async write path, as in pgdelta, plus the raw TCP socket `tiberius` connects its TDS session over (`tokio`'s `net` feature) |

Pinned 2026-09 (probed via cargo; crates.io index reachable): `pyo3 0.29.2`,
`tiberius 0.12.3`, `deltalake 0.32.4`.

`tiberius`'s `rustls` feature resolves `rustls 0.21.12` (via `tokio-rustls 0.24.1`),
while `deltalake` independently resolves `rustls 0.23.44`. Verified by direct probing,
not assumed: two non-interoperating `rustls` majors end up compiled into one binary.
Accepted as a documented cost, not a defect: unlike the earlier `arrow-odbc`-vs-
`deltalake` Arrow version mismatch, no data type ever has to cross the boundary between
the two TLS stacks, so nothing is actually broken, only duplicated. Revisit if `tiberius`
or `deltalake` ever converge on the same `rustls` major.

`odbc-api` was the right choice while the engine was unconfirmed; it is removed now that
it is not. `arrow-odbc` was deliberately never used, for the reason recorded in this
crate's history: it resolved `arrow` 59.3.0 while `deltalake` pins `arrow` 58.x, and
using both would have built two incompatible copies of the Arrow ecosystem in one
binary. That reasoning is now moot, but is kept here in case ODBC connectivity is ever
reconsidered.

## Security requirements

Numbered to parallel pgdelta's own list, since several of the ones pgdelta could drop
(no subprocess, no connection string) apply here in force instead.

1. `#![forbid(unsafe_code)]`.
2. Connection string/password via environment or an injected secret, **never** hardcoded
   and never constructed by string-interpolating untrusted input. Parsed with
   `tiberius::Config::from_ado_string`, which keeps the existing "one opaque connection
   string" shape rather than forcing a structured config object on every caller.
3. **Scrub the connection string and password from every error path.** A connection
   failure can echo the string it was given; that must never reach a Python traceback or
   a log line.
4. **Read-only is a provisioning requirement, not a code guarantee.** `tiberius`, like
   `odbc-api` before it, has no client-side concept of a read-only session; this crate
   only ever issues `SELECT` (see `src/connect.rs`), but that is a code-review property,
   not an enforced one. **The account used to connect must be provisioned with
   `SELECT`-only grants on the relevant tables**; that is where read-only actually has to
   be guaranteed, and it is the deploying organization's responsibility, not something
   this crate can force from the client side.
5. Bounded fetch batch size, so one very large table cannot exhaust memory or hold a
   server-side cursor open indefinitely.
6. Never log row data, matching pgdelta.
7. Checked integer/numeric conversions throughout; no silent wrap, matching pgdelta.
8. A query must have a timeout. `tiberius`'s client API has no built-in query-timeout
   parameter the way `odbc-api`'s `execute(.., query_timeout_sec)` did; enforced instead
   by wrapping the query future in `tokio::time::timeout`. An unattended incremental sync
   that hangs forever against a live production system is a worse failure than one that
   fails loudly.

## Documentation standard

Same as pgdelta: module-level `//!` docs stating role, inputs, outputs and
thread/runtime; mandatory rustdoc (what/params/returns/errors/panics/blocking, an
example) on every public item; inline `//` comments rare, non-obvious *why* only; no em
dashes anywhere; `docs/` Markdown is the source of truth, generating `.docx` via a copy
of pgdelta's `tools/md2docx.py` once there is enough written to be worth generating.

## Layout

```
src/lib.rs        crate docs + pyo3 module shell
src/error.rs      hand-rolled error enum (mirrors pgdelta's)
src/connect.rs    tiberius connection setup: TCP socket, TDS login, credential handling
src/catalog.rs    per-table config: watermark column, primary key, table name
src/types.rs      tiberius::ColumnType -> internal type model
src/builders.rs   Arrow column + batch builders, fed typed values directly (no text)
src/checkpoint.rs `_streamer_checkpoints` read/write
src/merge.rs      DeltaTable::merge orchestration per table
src/pipeline.rs   orchestration: per-table sync loop
src/python.rs        pyo3 surface (sync_tables, preflight)
python/tiberiusdelta/__init__.py
python/tiberiusdelta/__init__.pyi
python/tiberiusdelta/py.typed
pyproject.toml       maturin config (abi3-py310, mixed layout)
tests/mssql_live.rs  end-to-end tests against a live SQL Server container
.devtest/*.sql       fixtures that reset that container to a known baseline
tools/seed.py        applies those fixtures where sqlcmd is not available (CI)
tools/smoke.py       round-trips a live table through the built wheel
tools/md2docx.py     generates docs/*.docx from docs/*.md
docs/architecture.md
docs/api.md
docs/operations.md
```

Build the wheel with maturin (`pip install maturin && maturin build --release
--features extension-module,azure`). CI additionally runs the live suite and the smoke
test against a SQL Server service container, so the type mapping is a real CI gate rather
than a local-only check.

`src/values.rs` (ODBC-era text parsing) is gone: `tiberius::Row::cells()` yields values
already decoded into `ColumnData` variants, so there is no text to parse. What replaced
it is arithmetic, not parsing: rescaling a decimal, counting days or microseconds from an
epoch, each checked rather than cast.

## Driver notes (tiberius, verified against its source and docs.rs)

- **`tiberius::Config::from_ado_string(&str)` exists** and parses ADO.NET-style
  connection strings (`Server=...;Database=...;User Id=...;Password=...;`), so
  `ConnectConfig`'s "one opaque connection string" shape, and its credential-redaction
  logic, carry over from the ODBC-based version largely unchanged.
- **Connecting is manual, not one call.** `tiberius` is runtime-agnostic: the caller
  opens a `tokio::net::TcpStream` itself, wraps it with
  `tokio_util::compat::TokioAsyncWriteCompatExt::compat_write()`, and only then calls
  `tiberius::Client::connect(config, stream)`, which performs the TDS login and, if
  configured, the TLS handshake. This makes `src/connect.rs` genuinely `async`, unlike
  the ODBC version's synchronous calls that `crate::pipeline` had to wrap.
- **`tiberius::ColumnType` is a rich, native TDS type enum** (`Int1`/`Int2`/`Int4`/`Int8`,
  `Bit`, `Decimaln`/`Numericn`, `Datetime2`, `Daten`, `Timen`, `BigVarChar`/`NVarchar`,
  `BigVarBin`, and more), reported faithfully off the wire. This is why the ODBC-based
  version's driver quirks cannot recur here: a `DATETIME2` column, for instance, cannot
  be misreported as text, because TDS itself distinguishes the type and `tiberius` never
  goes through a driver's own text-conversion layer to report it.
- **`tiberius::numeric::Numeric { value: i128, scale: u8 }`** is the decoded form of a
  `DECIMAL`/`NUMERIC` value: already an unscaled integer plus a scale, the exact
  representation `Decimal128Builder::append_value` wants. No parsing is left for this
  type, only a value copy; this is also why the ODBC version's `.00`-vs-`0.00` text
  quirk cannot recur.
- **The wire type cannot drive the Arrow schema, so the catalog does.** This is the one
  finding that changed the design rather than confirming it. `ColumnType` is reported per
  *result-set* column, and the wire collapses exactly the distinctions a schema needs: a
  **nullable** `INT` arrives as `Intn` (an n-byte integer), not `Int4`, with its real
  width carried in TDS metadata `tiberius` does not expose, and `Decimaln`/`Numericn`
  arrive without the column's declared precision and scale. `Numeric::precision()` does
  exist, but it counts the digits *in one decoded value*, not the column's declaration,
  so it cannot size a schema either. Since most production columns are nullable, and
  since the Arrow schema has to be fixed *before* any row arrives (a sync that fetches
  zero changed rows still opens its Delta table), `crate::types` resolves from
  `INFORMATION_SCHEMA.COLUMNS` instead: standard T-SQL, one round trip per table, exact
  precision and scale. A useful side effect is that this crate's type module is now a
  close sibling of pgdelta's, both mapping a *catalog's* type name to the same internal
  model.
- **Delta Lake has no time-of-day type.** Arrow's `Time64` builds perfectly happily and
  is then rejected at commit ("Invalid data type for Delta Lake: Time64"), so a `TIME`
  column is written as its own literal text. Found by committing one against the live
  instance, not by reading a specification; the earlier ODBC version had the same latent
  mapping and no `TIME` column in its fixture to catch it.
- **tiberius's `DateTime<Utc>` conversion for `DATETIMEOFFSET` is wrong; its
  `DateTime<FixedOffset>` one is right.** TDS stores a `DATETIMEOFFSET`'s datetime2 part
  *already in UTC*, carrying the offset alongside only so the original local rendering can
  be reconstructed. tiberius's `DateTime<Utc>` impl subtracts that offset from the
  datetime2 part anyway, moving the instant by the offset a second time: a value written
  as `2026-03-04T13:45:30+02:00` (11:45:30 UTC) comes back as 09:45:30 UTC. Caught by the
  type-zoo live test and confirmed against SQL Server's own `SWITCHOFFSET(value, 0)` for
  the same row, rather than by reading the TDS specification and hoping. Its
  `DateTime<FixedOffset>` impl attaches the offset instead of subtracting it, leaving the
  instant correct, so `src/builders.rs` reads every `DATETIMEOFFSET` through that and a
  unit test pins the behaviour down, so that a future tiberius release fixing one and
  changing the other fails loudly instead of silently shifting timestamps.
- **`MONEY`/`SMALLMONEY` arrive as `f64`, not as decimals.** `tiberius` decodes both by
  dividing the wire's scaled integer by 10^4 in floating point, so exactness is gone
  before this crate sees the value. They therefore map to Arrow `Float64`: declaring a
  decimal would claim a fidelity the data no longer has. Prefer `DECIMAL` over `MONEY` in
  a source schema where exactness matters.
- **A NULL can arrive in a neighbouring variant.** `tiberius` picks the `ColumnData`
  variant for a NULL from the declared width in the TDS metadata and falls back to a
  64-bit one when that width is not one it recognises (its own `FromSql` impls carry
  matching fallbacks, which is the tell). `crate::builders` therefore accepts a NULL from
  any variant, which is lossless, while still rejecting a *non-null* value of the wrong
  width, which would not be.

## Open items

Still open, each needing a decision or a fact before it can be finalized:

- **Per-table watermark column and primary key configuration.** Needs the actual
  production schema (still not available). Config shape is designed and built
  (`catalog::TableSync`/`catalog::SyncCatalog`); only the real table list, watermark
  columns and primary keys remain to be filled in.
- **Checkpoint/merge atomicity across tables in one run.** Resolved in the sense of
  having a documented, tested answer, though revisit if it stops being good enough:
  each table's merge and checkpoint advance happen one table at a time, checkpoint only
  after its merge has committed, so a crash mid-run leaves every already-processed table
  correctly advanced and the rest untouched; a re-run safely re-fetches and re-merges
  (idempotently) whatever the crashed table's own last checkpoint still says is
  outstanding. See `src/merge.rs`'s module docs and its passing idempotency test.
- **Read isolation level.** SQL Server's snapshot isolation vs. this crate's current
  per-table independent queries; not yet designed.
- **Confirm the real production schema, connection details and credentials.** The
  engine is known; the actual database name, table set, and how this crate would
  actually reach it (network path, account) are still not.
- **A `TIME` column lands in Delta as text**, because Delta has no time-of-day type at
  all (see Driver notes). Faithful and lossless, but a downstream consumer wanting to do
  arithmetic on it has to parse it back. Revisit only if a real consumer asks: the
  alternative (microseconds since midnight as a `long`) is machine-friendlier and less
  readable, and nothing has asked yet.
- **The per-table `INFORMATION_SCHEMA.COLUMNS` lookup is one extra round trip per table
  per run.** Negligible against a table's own data fetch, but if a run ever covers
  hundreds of small tables the same way pgdelta's dump does, it becomes worth batching
  into a single catalog query for all of them at once.
- **The query timeout bounds time between rows, not the whole query.** A query that keeps
  delivering rows slowly forever is not caught. That is the right default for a streaming
  fetch (a legitimately large table must not be killed for being large), but a whole-run
  deadline may be wanted as well once this runs unattended on a schedule.

## Environment notes

- Rust 1.94.0 (pinned via `rust-toolchain.toml`), matching pgdelta.
- Docker available locally for standing up a throwaway test SQL Server database. The
  container (`tiberiusdelta-test-mssql`, SQL Server 2022, host port 14330) and the
  `.devtest/*.sql` fixtures that reset it are the same ones the ODBC-based version was
  validated against, renamed alongside the project.
- `.devtest/type_zoo.sql` seeds one nullable column of every type `src/types.rs` maps,
  with one row of values and one row of NULLs. It exists because the type mapping is the
  part of this crate most easily wrong in a way unit tests cannot catch: what a given
  T-SQL type actually decodes to is a property of TDS and tiberius, not of this code. It
  earned its place immediately, catching two real defects on its first run: `TIME`
  committed as Arrow `Time64`, which Delta rejects outright, and every `DATETIMEOFFSET`
  landing two hours off its true instant (see Driver notes for both). Its live test reads
  the committed Delta table back and compares values, not just the schema, because a
  decimal rescaled wrongly is still a perfectly valid decimal.
