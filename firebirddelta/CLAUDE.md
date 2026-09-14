# firebirddelta

Rust core, Python bindings. Streams a **live Firebird database** incrementally into
Delta Lake tables for use from Databricks: each run pulls only rows new or changed since
the last successful run, not a full reload.

This is a sibling project to `rust-streamer-pgdb` (pgdelta) and to the sibling
`tiberiusdelta` crate in this same repository, not a variant of either. It shares
tiberiusdelta's foundational assumptions relative to pgdelta (a live network connection,
credentials as a first-class concern, incremental not full-reload sync), but targets a
different engine, which changes the connectivity layer, the type system, and, unlike
tiberiusdelta, forces a synchronous-to-async bridge the sibling crate does not need. See
"Why `rsfbclient`, not a native driver install" for the choice this crate made and what
it costs, and "Driver notes" for what that choice turned out to mean in practice.

Status: **implemented end to end against `rsfbclient`'s pure-Rust backend; not yet run
against a live Firebird instance.** The Rust crate compiles cleanly, and every gate that
does not need a live server passes: 60 unit tests, 1 doctest, `fmt`, `clippy -D
warnings`, `RUSTDOCFLAGS=-D warnings cargo doc`. The four live end-to-end tests in
`tests/firebird_live.rs`, the `.devtest/*.sql` fixtures, and `tools/seed.py`/
`tools/smoke.py` are written to the same standard as tiberiusdelta's own, but this
session had no Docker daemon and no Firebird instance available to run them against (see
Environment notes and Open items); they are a design for the live gate, not a passed one.

## Goals / constraints (from the user, verbatim intent)

- Rust core, Python bindings, target consumer is Databricks: same as pgdelta and
  tiberiusdelta.
- The user's request was, verbatim in intent: "could we make a copy of this but for
  Firebird database" — of the `tiberiusdelta` crate in this same repository. "A copy"
  is read here the way tiberiusdelta itself reads pgdelta: the same *shape* (Rust core,
  Python bindings, incremental sync into Delta, the same documentation and security
  discipline), not a literal file-for-file duplicate, since Firebird's driver ecosystem,
  wire protocol, catalog, and type system all differ enough from SQL Server's that a
  faithful port means re-deriving the driver-specific modules from Firebird's own
  properties, the same way tiberiusdelta itself re-derived them from ODBC's when it
  switched to `tiberius`.
- Direct, trusted, live network access to the source is assumed available, the same
  assumption tiberiusdelta makes. Not verified against a real production instance
  (tiberiusdelta at least reached a local test container; this crate has not yet reached
  even that — see Open items).
- Incremental sync, not full reload: inherited from tiberiusdelta's own decision, for the
  same reasons (see tiberiusdelta's `CLAUDE.md`).
- Connectivity is `rsfbclient`, specifically its `pure_rust` wire-protocol backend, for
  the same "no driver install" motivation that led tiberiusdelta to `tiberius`. Unlike
  `tiberius`, `rsfbclient` is **synchronous**, which is the one structural way this
  crate's design cannot simply mirror tiberiusdelta's; see "Why this module is a hybrid
  of blocking and async" in `src/connect.rs` and `src/pipeline.rs`.
- Same engineering discipline as tiberiusdelta and pgdelta: security considered first,
  minimal dependency surface except where a real correctness need justifies an
  exception, code not over-commented, documentation written with each item, and real
  driver defects found by reading source and (where a live instance is available)
  running one, not guessed at.

## Why `rsfbclient`, not a native driver install

Firebird's own official client library (`fbclient`), like SQL Server's ODBC driver
before tiberiusdelta switched away from it, is a deployment-time install: a `.so`/`.dll`
that has to exist on whatever machine runs this, which Cargo cannot vendor. `rsfbclient`
offers three backends behind Cargo features: `linking` and `dynamic_loading` (both wrap
the native `fbclient`, reintroducing exactly that install), and `pure_rust`
(`rsfbclient-rust`), a from-scratch reimplementation of Firebird's wire protocol that
opens a plain TCP socket itself. This crate uses `pure_rust` for the same reason
tiberiusdelta uses `tiberius` over ODBC: nothing beyond the compiled binary has to exist
on the machine that runs this, on any platform.

What that costs, verified directly against `rsfbclient`'s own source and its own
published API docs, not assumed:

- **The API is synchronous.** `tiberius` is async-native; `rsfbclient` blocks the calling
  thread on every network call, whichever backend is chosen. This is the one place this
  crate's design cannot mirror tiberiusdelta's: `crate::connect` and `crate::pipeline`
  dispatch every `rsfbclient` call onto Tokio's blocking-thread pool via
  `tokio::task::spawn_blocking`, moving the open connection in and back out of each call.
  See "Driver notes" and both modules' own documentation for the shape this takes and
  what it does not solve (a timeout that cannot cancel a blocking call already in
  flight).
- **The value type is coarse.** `rsfbclient::SqlType` has seven variants (`Text`,
  `Integer`, `Floating`, `Timestamp`, `Binary`, `Boolean`, `Null`); `SMALLINT`/
  `INTEGER`/`BIGINT` all decode to the same `Integer(i64)`, and `DATE`/`TIME`/
  `TIMESTAMP` all decode to the same `Timestamp(NaiveDateTime)`. `crate::types` resolves
  from Firebird's own catalog, not from a fetched value, for exactly this reason; see
  "Driver notes".
- **`NUMERIC`/`DECIMAL` go through a wire-level `DOUBLE` before this crate ever sees the
  value**, which is a real precision loss, not a choice this crate makes; see "Driver
  notes" and `src/types.rs::resolve`'s own documentation.
- **A handful of Firebird 4+ types cannot be described by the row reader at all**
  (`INT128`, `DECFLOAT(16)`, `DECFLOAT(34)`, `TIME WITH TIME ZONE`,
  `TIMESTAMP WITH TIME ZONE`), stated in `rsfbclient`'s own top-level documentation, not
  discovered empirically. This crate works around it with a server-side `CAST`; see
  "Driver notes".
- **The published `0.28.0` release does not compile on stable Rust at all.** Found by
  attempting the build, not by reading a changelog: `rsfbclient-rust 0.28.0`'s
  `wire.rs` uses an `if let ... && let ...` match-arm *guard*, which needs nightly's
  still-unstable `if_let_guard` feature (`rustc --explain E0658`), distinct from the
  let-chains already stabilised for plain `if`/`while` expressions. `0.27.0`'s
  `rsfbclient-core` and `rsfbclient` (row/query/builder API) are byte-identical to
  `0.28.0`'s, diffed directly, so `Cargo.toml` pins `=0.27.0` as a pure version fix, not
  a capability trade-off. Revisit once a released version fixes that guard on stable.

Weighed against `linking`/`dynamic_loading`: those backends go through the real
`fbclient`, which likely handles `NUMERIC`/`DECIMAL` and the Firebird 4+ types more
faithfully (an ISC API call can report a column's true declared scale, unlike the wire
coercion `pure_rust` applies), at the cost of the driver install this crate exists to
avoid. If a real deployment needs exact decimals or native `INT128`/`DECFLOAT` more than
it needs a driver-free binary, switching `Cargo.toml`'s `rsfbclient` feature from
`pure_rust` to `linking` is a smaller change than it looks: `crate::connect` is the only
module that names a concrete client type, and `crate::types`/`crate::builders` already
resolve from the catalog rather than the wire type, so the switch is mostly contained
there. Not attempted in this session; recorded here as the real lever to pull if this
Driver notes tradeoff ever needs revisiting.

## Pipeline

```
rsfbclient pure_rust connection (TCP + Firebird wire protocol, no driver manager),
  opened on a Tokio blocking-pool thread and threaded through the whole run
  -> per table: RDB$RELATION_FIELDS/RDB$FIELDS catalog lookup -> Arrow schema (see
     "Driver notes" for why the catalog, and not rsfbclient's own SqlType, is what the
     schema is built from, and why some columns are selected through a server-side CAST
     or excluded outright)
  -> per table: SELECT <selected/cast columns> FROM table WHERE watermark > last_value
     ORDER BY watermark ASC, run to completion on one blocking-pool thread, streaming
     completed batches out through a bounded tokio::sync::mpsc channel as it goes (see
     crate::pipeline's module docs for why one rsfbclient cursor cannot be paused and
     resumed across separate blocking calls the way tiberiusdelta pauses tiberius's own
     async stream)
  -> row batches -> deltalake::arrow builders, fed rsfbclient's decoded SqlType values,
     narrowed to each column's declared Arrow width with checked conversions
  -> DeltaTable::merge, keyed on the table's primary key (upsert: new rows insert,
     matching rows update)
  -> checkpoint recorded in that table's *own* Delta table under
     `_firebirddelta_checkpoints/<table>` (table_name, watermark_column, last_value,
     synced_at), the same role tiberiusdelta's `_streamer_checkpoints` and pgdelta's
     `_pgdelta_loads` play. One path component, not several: Firebird has a single flat
     table namespace per database file, no schema concept at all, so there is no
     database/schema qualification to split apart the way tiberiusdelta's `relative_path`
     has to.
```

Checkpoint read at the start of a run, advanced only after that table's merge has
committed; see `src/merge.rs`'s module docs for why that ordering is what keeps a crash
mid-run safe to retry. Checkpoint/merge atomicity is per table, not across the whole run;
see Open items.

## What incremental sync does not do

Stated plainly, pgdelta-style and tiberiusdelta-style, rather than discovered during an
incident:

- **Cannot detect deletions.** Same as tiberiusdelta: a row removed from the source
  simply stops appearing in its `WHERE watermark_column > last_value` result, with no
  signal at all.
- **Needs a watermark column and a primary key per table.** Same as tiberiusdelta.
- **Not yet verified against any live instance at all**, real or local. Everything here
  is designed against Firebird's own documented catalog and `rsfbclient`'s own
  documented and source-read behaviour, and the crate compiles and its 60 unit tests
  pass, but no `SELECT` has actually reached a Firebird server through this code. This is
  one level short of tiberiusdelta's own starting point, which had at least reached a
  local test container before this document's first line was written. See Open items.
- **A row written with exactly the same watermark value as the current checkpoint is
  permanently invisible to every future sync.** Identical mechanism and identical
  mitigation to tiberiusdelta's own documented case: `src/pipeline.rs` filters strictly
  `WHERE watermark_column > last_value`, so pick a watermark column with enough
  precision, or a strictly monotonic one, that genuine ties cannot occur.
- **`NUMERIC`/`DECIMAL` columns lose precision before this crate ever sees them.** Not a
  watermark-tie-style edge case but a standing property of the chosen driver: see "Why
  `rsfbclient`" above and `src/types.rs::resolve`'s own documentation. A watermark column
  should therefore never be a `NUMERIC`/`DECIMAL` column for the same underlying reason
  it should never be an imprecise floating column in any database: the comparison this
  crate trusts the source to make (`ORDER BY watermark ASC`, `WHERE watermark > ?`) is
  only as reliable as the value the source itself compares, and Firebird's own query
  engine still compares the exact stored `NUMERIC`/`DECIMAL` value, so this is actually
  safe on the source side; it is specifically the *checkpoint's own rendering* of that
  value (via `crate::builders::render_text`, which renders the already-lossy `f64` this
  crate received) that can disagree with a value stored more precisely than a `f64`
  represents, in the same class of risk as a coarse-precision timestamp watermark.
- **Firebird `ARRAY` columns are not specifically detected.** `crate::types::resolve`
  keys off `RDB$FIELD_TYPE`/`RDB$FIELD_SUB_TYPE` alone; Firebird's `ARRAY` feature is
  signalled by `RDB$RELATION_FIELDS.RDB$DIMENSIONS IS NOT NULL` on top of an ordinary
  base type, which this crate does not query at all. Behaviour against a table with an
  `ARRAY` column is genuinely unknown: it was not encountered in `.devtest/type_zoo.sql`
  and no live instance was available to find out empirically. A production schema using
  arrays needs this resolved (most plausibly: detect and route to
  `SqlType::Unsupported`, the same as an exotic `BLOB` sub-type) before being configured
  here.

## Threat model

Same as tiberiusdelta's, restated because it carries over exactly: the source is
**trusted** (direct access to the organization's own system), credentials are a
first-class concern (never in argv, a log line, or an error message; scrubbed by
`crate::connect::redact` before an error is ever constructed), this hits a live
production system so fetch batches must stay bounded (`SyncConfig::fetch_batch_size`),
and cross-table consistency is weaker than a single dump file's (each table queried
independently, no snapshot spanning the whole run).

One addition specific to this crate's synchronous client: a query timeout here
(`SyncConfig::query_timeout_sec`) bounds how long the *async caller* waits, not how long
the underlying blocking fetch actually runs. `rsfbclient` gives this crate no
cancellation point inside a synchronous call, so a timed-out fetch is abandoned, not
stopped: the blocking OS thread it runs on keeps running until it finishes or the
process exits. The same caveat applies to `ConnectConfig::login_timeout_sec`. This is
weaker than tiberiusdelta's per-row, genuinely-cancellable timeout (`tiberius` streams
over `tokio::net::TcpStream`, which a dropped future actually closes), and is a direct,
documented cost of the synchronous client rather than an oversight; see "Why
`rsfbclient`" above and Open items.

## Dependency budget

| Crate | Why |
|---|---|
| `pyo3` | Python bindings |
| `rsfbclient`, `rsfbclient-rust` | Pure-Rust Firebird wire-protocol client. `default-features = false, features = ["pure_rust"]` excludes the `linking`/`dynamic_loading` backends, which link against the official native `fbclient` and would reintroduce the deployment-time driver install this crate exists to avoid. Pinned to `=0.27.0`, not the newest `0.28.0`: probed directly, `0.28.0`'s `rsfbclient-rust` fails to compile on stable Rust at all (an unstable `if let ... && let ...` match guard); `0.27.0`'s API is byte-identical, so this is a pure version pin. `rsfbclient-rust` is a direct dependency only because `rsfbclient` itself never re-exports the concrete `RustFbClient` type its own `pure_rust` builder returns from its crate root; naming `crate::connect::FbConnection`'s type parameter needs it directly. Replaces `tiberius` for this engine. |
| `chrono` | Converting `rsfbclient`'s decoded `NaiveDateTime` values (every temporal column, whatever its real Firebird type, decodes through the same variant; see Driver notes) into the epoch-relative integers Arrow stores. |
| `deltalake` (delta-rs), **with the `datafusion` feature** | Delta write path, and `DeltaTable::merge` for transactional upsert by key. The same deliberate exception to "minimal dependency surface" tiberiusdelta and pgdelta both accept: see `docs/architecture.md`. |
| `buoyant_kernel_derive` (pinned `=1.1.0`) | Same fix tiberiusdelta's own `Cargo.toml` documents: `1.2.0` removed a macro delta-rs's kernel dependency needs, without a semver-major bump, breaking the `datafusion`-enabled build. |
| `tokio` (`rt`, `rt-multi-thread`, `sync`, `time`) | Delta's async write path, the blocking-thread pool `crate::connect` and `crate::pipeline` dispatch every `rsfbclient` call onto, the `mpsc` channel fetched batches stream through, and the timeouts wrapped around both the login and each batch. Notably **not** `net`: unlike tiberiusdelta, this crate's own code never opens a socket itself — `rsfbclient` opens its own `std::net::TcpStream` internally, synchronously, which is the whole reason the blocking-pool bridge exists in the first place. |
| `futures` | `TryStreamExt`, used by `crate::checkpoint` and `crate::merge` exactly as in tiberiusdelta; both modules ported over unchanged, since they are driver-agnostic. |

Pinned 2026-09 (probed via cargo; crates.io index reachable): `pyo3 0.29.2`,
`rsfbclient 0.27.0` (see above for why not `0.28.0`), `deltalake 0.32.4`.

Unlike tiberiusdelta, this crate's own code never links two different TLS stacks: it
never establishes TLS itself (`rsfbclient`'s `pure_rust` backend has no TLS support at
all, a further, separate limitation of the chosen backend worth naming even though it
does not currently block anything this crate does — Firebird's own wire encryption,
where used, would need the `linking`/`dynamic_loading` backend or a network-level
tunnel), and `deltalake`'s own `rustls` resolution is this crate's only TLS stack.

## Security requirements

Numbered to parallel tiberiusdelta's own list, since it carries over almost entirely
except where the synchronous client changes what "enforced" can mean.

1. `#![forbid(unsafe_code)]`. This is also *why* `crate::pipeline` cannot hold one
   `rsfbclient` cursor open across separate `spawn_blocking` calls (which would need a
   self-referential struct or `unsafe` to express) and instead confines a whole table's
   fetch to one blocking call, streaming batches out through a channel; see
   `src/pipeline.rs`'s module docs.
2. Connection string/password via environment or an injected secret, **never**
   hardcoded and never constructed by string-interpolating untrusted input. Parsed with
   `rsfbclient::builder_pure_rust().from_string(...)`, a `firebird://user:pass@host:port/db`
   URL, which keeps the "one opaque connection string" shape tiberiusdelta's ADO.NET
   string also has.
3. **Scrub the connection string and password from every error path.** Same discipline,
   same two-pass approach (`crate::connect::redact`), adapted to a URL's userinfo syntax
   instead of ADO.NET `key=value` pairs, and additionally checked against the
   percent-decoded form of the password, since `rsfbclient` decodes it before any error
   of its own could echo it.
4. **Read-only is a provisioning requirement, not a code guarantee.** Identical to
   tiberiusdelta: `rsfbclient`, like `tiberius`, has no client-side concept of a
   read-only session; this crate only ever issues `SELECT` (see `src/pipeline.rs`), but
   that is a code-review property, not an enforced one.
5. Bounded fetch batch size (`SyncConfig::fetch_batch_size`), enforced the same way:
   `crate::pipeline` never holds more than one batch's worth of rows in memory before
   merging it and clearing the buffer, and the streaming channel between the blocking
   fetch and the async merge is itself bounded (capacity 2), so the blocking producer
   cannot run arbitrarily far ahead of the merge consumer either.
6. Never log row data, matching tiberiusdelta and pgdelta.
7. Checked integer/numeric conversions throughout; no silent wrap. Concretely more work
   here than in tiberiusdelta, because `rsfbclient` reports every whole-number column
   (`SMALLINT`/`INTEGER`/`BIGINT`) as the same `i64`: narrowing it to its column's
   catalog-declared Arrow width (`Int16`/`Int32`/`Int64`) is a checked `try_from` in
   `crate::builders::ColumnBuilder::append`, not a cast, so a value that should not fit
   its declared width is reported as `Error::UnparsableValue` rather than silently
   wrapped.
8. A query must have a timeout. Enforced the same structural way as tiberiusdelta
   (wrapping the operation in `tokio::time::timeout`), but weaker in effect: see
   "Threat model" above for why a timeout here cannot actually cancel an in-flight
   `rsfbclient` call, only stop this crate from waiting on it.

## Documentation standard

Same as tiberiusdelta and pgdelta: module-level `//!` docs stating role, inputs, outputs
and thread/runtime (here, explicitly stating *which* half of a hybrid module is blocking
and which is async, since that distinction did not exist in tiberiusdelta); mandatory
rustdoc on every public item; inline `//` comments rare, non-obvious *why* only; no em
dashes anywhere; `docs/` Markdown is the source of truth. Unlike tiberiusdelta, no
`.docx` generation has been set up for this crate yet (`tools/md2docx.py` was not
copied over); revisit once there is a real reason to produce one.

## Layout

```
src/lib.rs        crate docs + pyo3 module shell
src/error.rs      hand-rolled error enum (mirrors tiberiusdelta's and pgdelta's; adds
                  Error::ExcludedColumnType, which neither sibling needs)
src/catalog.rs    per-table config: watermark column, primary key, table name
                  (unchanged from tiberiusdelta's, since it is driver-agnostic; only the
                  doc comments and tests were reworded for Firebird's flat namespace)
src/connect.rs    rsfbclient connection setup: the sync/async bridge, credential
                  handling, redaction
src/types.rs      RDB$FIELD_TYPE/RDB$FIELD_SUB_TYPE -> internal type model, including
                  the server-side CAST and exclusion decisions Driver notes describes
src/builders.rs   Arrow column + batch builders, fed rsfbclient's coarse SqlType values
src/checkpoint.rs `_firebirddelta_checkpoints` read/write (unchanged from tiberiusdelta's
                  except for the constant's name and its own tests' fixture data, since
                  it is driver-agnostic)
src/merge.rs      DeltaTable::merge orchestration per table (unchanged from
                  tiberiusdelta's, for the same reason)
src/pipeline.rs   orchestration: per-table sync loop, and the blocking/async bridge for
                  the fetch side specifically (the biggest structural departure from
                  tiberiusdelta's own pipeline.rs)
src/python.rs        pyo3 surface (sync_tables, preflight, source_watermark,
                     set_checkpoint); a near-verbatim port of tiberiusdelta's, since it
                     only ever talks to crate::pipeline's already-driver-agnostic types
python/firebirddelta/__init__.py
python/firebirddelta/__init__.pyi
python/firebirddelta/distributed.py
python/firebirddelta/py.typed
pyproject.toml       maturin config (abi3-py310, mixed layout)
tests/firebird_live.rs  end-to-end tests against a live Firebird instance; see Status
.devtest/*.sql          fixtures that reset that instance to a known baseline
tools/seed.py           applies those fixtures via the official `firebird-driver` package
tools/smoke.py          round-trips a live table through the built wheel
docs/architecture.md
docs/api.md
docs/operations.md
```

Build the wheel with maturin (`pip install maturin && maturin build --release
--features extension-module,azure`). Unlike tiberiusdelta, CI has not been set up to run
a Firebird service container yet; see Open items.

## Driver notes (`rsfbclient`, verified against its source, its own published docs, and
Firebird's own reference documentation for `RDB$FIELDS`)

- **`rsfbclient::builder_pure_rust().from_string(s)?.connect()` is synchronous and
  blocking**, unlike `tiberius::Client::connect`. There is no equivalent of tiberiusdelta's
  "the caller opens the socket, tiberius does the rest asynchronously" story: `rsfbclient`
  owns socket creation itself, on the calling thread, and every subsequent call
  (`query`, `query_iter`, `execute`) blocks the same way. `crate::connect::open` bridges
  this with `tokio::task::spawn_blocking` plus `tokio::time::timeout`; the timeout's
  caveat (does not cancel the blocking call) is documented on
  `ConnectConfig::login_timeout_sec` itself, not left implicit.
- **`rsfbclient::SqlType` is a coarse, seven-variant enum, reported per *value*, not per
  *column type*.** Confirmed by reading `rsfbclient-core`'s `row.rs`: `Text(String)`,
  `Integer(i64)`, `Floating(f64)`, `Timestamp(NaiveDateTime)`, `Binary(Vec<u8>)`,
  `Boolean(bool)`, `Null`. This is the direct cause of every other finding below.
- **`SMALLINT`/`INTEGER`/`BIGINT` all decode to the same `Integer(i64)`.** A column's
  real declared width has to come from the catalog (`RDB$FIELDS.RDB$FIELD_TYPE`), the
  same conclusion tiberiusdelta reached for a different reason (its own driver's
  `Intn`/`Int4` distinction problem); the mechanism differs but the fix is the same
  shape: `crate::types` resolves the schema from `RDB$RELATION_FIELDS`/`RDB$FIELDS`, one
  round trip per table, and `crate::builders` narrows the always-`i64` value to that
  declared width with a checked conversion.
- **`DATE`/`TIME`/`TIMESTAMP` all decode to the same `Timestamp(NaiveDateTime)`, with a
  synthetic date or time attached where the real column has neither.** Read directly off
  `rsfbclient-core`'s `date_time.rs`: a `DATE` column's decoded value carries a
  synthetic midnight time, and a `TIME` column's decoded value carries a synthetic date.
  `crate::builders::ColumnBuilder` picks `.date()`, `.time()`, or the whole value out of
  that one decoded variant according to what `crate::types` already resolved the column
  to be, never according to the value itself, which has no way to say.
- **`NUMERIC`/`DECIMAL` arrive already rounded through a wire-level `DOUBLE`, and this is
  the crate's own top-level published behaviour, not a bug this session found by
  accident.** `rsfbclient`'s own crate documentation states it plainly: "`NUMERIC`/
  `DECIMAL` go through `f64`: scaled values whose integer form exceeds 2^53 lose
  precision silently". Reading `rsfbclient-rust`'s `xsqlda.rs` confirms the mechanism:
  when building the BLR for a fetch, any `SMALLINT`/`INTEGER`/`BIGINT` column with a
  nonzero declared scale (i.e. an exact `NUMERIC`/`DECIMAL`) is coerced to be fetched as
  Firebird's own wire-level `DOUBLE` type, with its scale forced to zero. By the time a
  value reaches `rsfbclient::SqlType`, it is `Floating(f64)`, indistinguishable from a
  genuine `FLOAT`/`DOUBLE PRECISION` column, and the exactness is already gone. This is
  more severe than tiberiusdelta's own `MONEY`-as-`f64` finding: `MONEY` is a
  SQL-Server-specific type nobody is required to use, while `NUMERIC`/`DECIMAL` is
  Firebird's *only* standard exact-numeric type, so this crate cannot avoid the finding
  by recommending a different, more-exact column type the way tiberiusdelta could for
  `MONEY`. `crate::types::resolve` therefore maps every `NUMERIC`/`DECIMAL` straight to
  `SqlType::Double` (Arrow `Float64`), never a decimal type, and does *not* offer the
  `CAST`-to-text escape hatch it offers Firebird 4's exotic types, since silently turning
  every occurrence of Firebird's single most common exact-numeric type into a string
  column would be a bigger, more surprising behaviour change than the one documented
  degradation. A caller for whom this matters should `CAST` the column to `VARCHAR` in
  the source schema (a view, typically) before configuring it here; see "Why
  `rsfbclient`" above for the alternative of switching backends entirely.
- **A handful of Firebird 4+ types cannot be described by the row reader at all, and this
  too is `rsfbclient`'s own published, not empirically rediscovered, behaviour**: its
  own crate documentation states "Firebird 4+ types are not supported by the row
  reader: selecting an `INT128`, `DECFLOAT(16/34)`, `TIMESTAMP WITH TIME ZONE` or `TIME
  WITH TIME ZONE` column ... fails at describe time with 'Unsupported column type'."
  Because Firebird has no schema-qualified per-column `SELECT` the way a bare column
  list side-steps a broken column elsewhere (the *statement* fails to describe, not the
  one column), a table containing any of these types cannot be queried with a bare
  `SELECT *`, or even a bare `SELECT` naming that column, at all. `crate::types::resolve`
  works around this the way `rsfbclient`'s own documentation suggests: these five types
  resolve to `SqlType::CastToText`, and `crate::pipeline` builds the generated `SELECT`
  with `CAST(column AS VARCHAR(64)) AS column` in place of the bare column name for
  exactly these columns, moving the conversion server-side before `rsfbclient` ever has
  to describe the column's real type. Ironically, this makes `INT128` and `DECFLOAT`
  columns **more faithfully represented** than a plain `NUMERIC`/`DECIMAL` column: the
  `CAST`-to-text path preserves every digit exactly, while `NUMERIC`/`DECIMAL` has no
  such escape (see above) and is stuck going through the lossy `f64` path. A genuinely
  odd but real consequence of two independent driver limitations interacting.
- **`RDB$FIELD_NAME` (and every other identifier column in Firebird's system tables) is
  a fixed-width `CHAR`, so a short name comes back space-padded.** Found by reading
  Firebird's own `RDB$FIELDS` reference documentation, not empirically (no live instance
  was available to observe it directly): `CHAR` columns are blank-padded to their
  declared width by definition, and `rsfbclient` decodes `CHAR` the same as `VARCHAR`
  (both `SqlType::Text`), with no trimming of its own. `crate::pipeline::
  describe_table_blocking` trims every name read back from the catalog before using it;
  omitting this would silently corrupt every catalog lookup with trailing whitespace
  that string equality checks (matching a configured watermark column, for instance)
  would then always fail to find.
- **A `BLOB` column's sub-type, not just its `RDB$FIELD_TYPE`, decides whether
  `rsfbclient` can read it at all.** `RDB$FIELD_TYPE` 261 covers every `BLOB`
  regardless of sub-type; `rsfbclient`'s row reader supports sub-type 0 (binary,
  `SqlType::Binary`) and sub-type 1 (text, `SqlType::Text`) natively, per its own
  documented type table, but has no generic textual form for anything else (an array
  BLOB, or a user-defined sub-type). `crate::types::resolve` maps those columns to
  `SqlType::Unsupported`, and `crate::pipeline` excludes them from the generated
  `SELECT` and the Arrow schema entirely, reporting them through
  `TableSyncStats::excluded_columns` and, if the excluded column happens to be a
  configured watermark or primary key column, failing that table's sync outright with
  `Error::ExcludedColumnType` rather than silently omitting it, since a sync missing its
  own watermark or key column is not one that can run correctly at all.
- **A `NULL` has one shape, unlike `tiberius`'s.** `rsfbclient::SqlType::Null` is its own
  dedicated variant; there is no "a NULL can arrive in a neighbouring variant because the
  wire only carries the column's declared width" case the way tiberiusdelta had to defend
  against for `tiberius::ColumnData`. `crate::builders::ColumnBuilder::append` therefore
  only needs one `matches!(data, FbValue::Null)` check up front, not per-variant
  tolerance; documented as a positive contrast, not merely an absence of a problem, since
  it was worth confirming rather than assuming.
- **Firebird has no schema concept**, unlike SQL Server's `database.schema.table`. A
  table name is one flat identifier per database file (`RDB$RELATION_NAME`), so
  `crate::pipeline` has no `parse_qualified`/three-part-name logic to port from
  tiberiusdelta at all: `relative_path(table)` is the validated table name itself. An
  unquoted `CREATE TABLE` name is folded to upper case by Firebird itself, so a
  configured `TableSync::table` is typically expected all-caps to match; this crate does
  not normalise casing itself (matching every other sibling's "no guessing" policy) and
  matches column names case-insensitively for convenience, the same leniency
  tiberiusdelta applies.

## Scaling

**Not measured.** Unlike tiberiusdelta, which has real numbers from a built wheel
against a live SQL Server container (see tiberiusdelta's `CLAUDE.md`), this crate has
not been run against any live Firebird instance in this session (see Status and Open
items), so there is nothing here to report honestly. The distributed design
(`firebirddelta.distributed`, one Delta table and one checkpoint Delta table per source
table, idempotent merges) is ported over unchanged from tiberiusdelta's, since it rests
entirely on the shared-nothing checkpoint layout `src/checkpoint.rs` already provides,
which is driver-agnostic; the *reasoning* for why it should scale the same way is
inherited, but the *numbers* are not, and should not be assumed to transfer given the
different (and, for the fetch side, synchronous) connectivity layer underneath.

## Open items

Still open, each needing a decision or a fact before it can be finalized:

- **No live Firebird instance has ever run against this code.** The largest gap
  relative to tiberiusdelta, which reached a local test container before its own
  `CLAUDE.md` first described itself as "implemented end to end." `tests/firebird_live.rs`,
  `.devtest/*.sql`, and `tools/seed.py`/`tools/smoke.py` are written to the same
  standard as tiberiusdelta's equivalents, including the type-zoo table this session
  could not run, but every one of them needs a first real run before being trusted,
  particularly the exact literal syntax of `.devtest/type_zoo.sql`'s `TIME WITH TIME
  ZONE`/`TIMESTAMP WITH TIME ZONE` literals, which was written from Firebird's language
  reference rather than confirmed against a parser.
- **Per-table watermark column and primary key configuration.** Same open item as
  tiberiusdelta's, for the same reason: needs the actual production schema.
- **Firebird `ARRAY` columns are not detected or handled.** See "What incremental sync
  does not do" above.
- **The query and login timeouts cannot cancel an in-flight `rsfbclient` call.** See
  "Threat model." If this ever needs a real fix rather than a documented caveat, the
  paths are: switch to a client with a genuine cancellation point (a hand-rolled
  async wire client, which is a large undertaking of its own), or accept a bounded
  number of leaked blocking-pool threads as the operational cost and monitor for it.
  Not resolved here; recorded as a real, load-bearing limitation rather than papered
  over.
- **Read isolation level.** Same open item as tiberiusdelta's: Firebird's own snapshot
  isolation vs. this crate's per-table independent queries, not yet designed.
  `rsfbclient`'s default transaction (read committed, record version) is what every
  query in this crate currently runs under, unexamined for whether a different
  isolation level would better suit a long-running incremental fetch.
- **Confirm the real production schema, connection details and credentials.** Same open
  item as tiberiusdelta's.
- **A `TIME` column, and every Firebird 4+ `CastToText` column, lands in Delta as text**,
  for the reasons given in Driver notes (Delta has no time type at all, and the row
  reader cannot describe the FB4+ types natively). Revisit only if a real consumer asks.
- **The per-table catalog lookup is one extra round trip per table per run.** Same open
  item as tiberiusdelta's, for the same reason, at the same scale.
- **The `linking`/`dynamic_loading` backend switch, if `NUMERIC`/`DECIMAL` exactness or
  native `INT128`/`DECFLOAT` ever becomes a real requirement.** See "Why `rsfbclient`"
  above for what would have to change and what it would cost (the driver install this
  crate currently avoids).
- **No CI Firebird service container has been set up.** Unlike tiberiusdelta's CI, which
  runs a SQL Server service container for its own live suite and smoke test, this
  crate's live gate is not wired into any CI at all yet.

## Environment notes

- Rust 1.94.0 (pinned via `rust-toolchain.toml`), matching tiberiusdelta and pgdelta.
- **This session had no Docker daemon available** (`docker` the client binary was
  present, but `dockerd` was not running, and no Firebird instance was reachable any
  other way), unlike tiberiusdelta's own development environment, which has a running
  local SQL Server container. The Firebird equivalent this crate's fixtures and tests
  are written against, once such an environment exists, is a throwaway container running
  **Firebird 4.0 or later** (`.devtest/type_zoo.sql` needs Firebird 4 for `INT128`,
  `DECFLOAT`, and the `WITH TIME ZONE` types; earlier Firebird versions would need a
  narrower type-zoo fixture), for example:

  ```bash
  docker run -d --name firebirddelta-test-firebird -p 3050:3050 \
    -e FIREBIRD_DATABASE=firebirddelta_test.fdb \
    -e ISC_PASSWORD=masterkey \
    -e FIREBIRD_USER=SYSDBA \
    jacobalberty/firebird:v4.0
  ```

  paired with `python tools/seed.py` (needs `pip install firebird-driver`, the official
  Python client, which itself needs the native `fbclient` library present on whatever
  machine runs the *tooling* — a cost this crate's own shipped Rust/Python surface does
  not carry, since that uses `rsfbclient`'s driver-free `pure_rust` backend instead) and
  then `cargo test --test firebird_live`.
- `.devtest/type_zoo.sql` is written to the same purpose as tiberiusdelta's own: one
  nullable column of every type `src/types.rs` maps (including the Firebird 4+
  `CastToText` types and both `BLOB` sub-types this crate supports), one row of values
  and one row of NULLs, specifically because what a given Firebird type actually decodes
  to is a property of the wire protocol and `rsfbclient`, not of this code, and only a
  real server can prove the mapping the way tiberiusdelta's own type-zoo test proved
  two real defects on its first live run. This crate's version has not yet had that
  chance; see Open items.
