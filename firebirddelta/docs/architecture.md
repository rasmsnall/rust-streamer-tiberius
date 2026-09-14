# Architecture

This document is the reference for how `firebirddelta` is built. `CLAUDE.md` is the
design log (why each decision was made, what it costs, what was found by reading source
or running a live instance); this document assumes those decisions and describes the
resulting system. Where the two disagree, `CLAUDE.md` is more current.

## Overview

```
rsfbclient (pure_rust)           tokio blocking pool            tokio async runtime
        |                               |                              |
   TCP + Firebird              spawn_blocking(describe_table)   merge::open_or_create
   wire protocol   ------->    spawn_blocking(fetch_and_send)   merge::upsert
        |                               |                              |
   RDB$RELATION_FIELDS          builds RecordBatch            DeltaTable::merge (commit)
   RDB$FIELDS catalog           per fetch_batch_size rows              |
        |                               |                              v
        +----------- tokio::sync::mpsc channel (capacity 2) ---->  checkpoint::advance
```

One table's sync, in order:

1. `checkpoint::read` — where did this table leave off, if ever.
2. `pipeline::describe_table` — `RDB$RELATION_FIELDS` joined to `RDB$FIELDS`, resolved
   through `types::resolve` into an Arrow schema, a set of excluded columns, and a
   generated column list (bare names, or `CAST(... AS VARCHAR(64))` for the Firebird 4+
   types `rsfbclient` cannot describe natively).
3. `merge::open_or_create` — the table's own Delta table, created on a first sync.
4. `pipeline::fetch_and_send`, on a blocking-pool thread — opens one `rsfbclient` cursor
   for the whole table, and streams completed `RecordBatch`es (via `builders::record_batch`)
   out through a bounded channel as it accumulates `fetch_batch_size` rows at a time.
5. The async side receives each batch, calls `merge::upsert` (one Delta commit per
   batch), and tracks the greatest watermark value seen.
6. `checkpoint::advance` — only after every batch has committed.

## Why the fetch side is a single blocking call, not one per batch

`rsfbclient`'s `query_iter` returns an iterator that borrows the `Connection` mutably for
as long as the cursor is open. Tokio's `spawn_blocking` takes an owned, `'static`
closure, so passing a connection into one blocking call, getting a partial result back,
and passing it into a *second* blocking call to continue would require the iterator
itself to survive the trip — which it cannot, since it borrows a value that would have to
move. The alternative (re-running the query per chunk with `SELECT FIRST n SKIP m`) was
considered and rejected: it re-scans the ordered, filtered result set from the start on
every chunk, and a source mutated between chunks could shift what a given `SKIP` offset
actually returns. One blocking call per table, streaming out finished batches as it
goes, keeps memory bounded without either problem. See `crate::pipeline`'s own module
docs for the full reasoning, including why this needs no `unsafe` code.

## Concurrency and failure model

Identical to tiberiusdelta's and pgdelta's at the table level: each table's merge is one
atomic Delta commit, and a checkpoint only advances after its own merge has committed. A
crash between them re-fetches and re-merges the same rows on the next run, which is safe
because `MERGE` is an upsert. There is no cross-table transaction; different tables in
the same run can reflect different moments in time.

What differs from tiberiusdelta: a query timeout here (`SyncConfig::query_timeout_sec`)
bounds how long the async side *waits*, not how long the blocking fetch actually *runs*.
On a timeout, the blocking task is detached (not joined, not aborted) and left to finish
on its own; the connection it was holding is simply not reused for anything further in
that run. See `CLAUDE.md`'s Threat model and Open items.

## Type mapping

Resolved once per table from `RDB$RELATION_FIELDS`/`RDB$FIELDS`, never from a fetched
value (`rsfbclient::SqlType` is too coarse to distinguish most of these; see
`CLAUDE.md`'s Driver notes for the full reasoning behind every row below).

| Firebird type | Arrow type | Notes |
|---|---|---|
| `SMALLINT` | `Int16` | |
| `INTEGER` | `Int32` | |
| `BIGINT` | `Int64` | |
| `NUMERIC`/`DECIMAL` (any width) | `Float64` | **Not exact.** Decoded through a wire-level `DOUBLE` before this crate sees it; values whose unscaled form exceeds 2^53 can be off. |
| `FLOAT` | `Float64` | Widened from 32-bit. |
| `DOUBLE PRECISION` | `Float64` | |
| `BOOLEAN` | `Boolean` | Firebird 3+. |
| `DATE` | `Date32` | |
| `TIME` | `Utf8` (text) | Delta has no time-of-day type; the value itself is not lossy. |
| `TIMESTAMP` | `Timestamp(us, UTC)` | Assumed UTC; Firebird's plain `TIMESTAMP` has no zone of its own. |
| `CHAR`/`VARCHAR` | `Utf8` | |
| `BLOB SUB_TYPE TEXT` (1) | `Utf8` | |
| `BLOB SUB_TYPE BINARY` (0) | `Binary` | |
| `INT128` | `Utf8` (text, via server-side `CAST`) | `rsfbclient`'s row reader cannot describe this type at all; exact, since text loses nothing. |
| `DECFLOAT(16)`/`DECFLOAT(34)` | `Utf8` (text, via server-side `CAST`) | Same reason as `INT128`; also exact. |
| `TIME WITH TIME ZONE` | `Utf8` (text, via server-side `CAST`) | Same reason. |
| `TIMESTAMP WITH TIME ZONE` | `Utf8` (text, via server-side `CAST`) | Same reason. |
| `BLOB` with any other sub-type | *(excluded)* | No generic textual `CAST` exists; the column is dropped from the query and the schema entirely, reported via `TableSyncStats.excluded_columns`. |
| `ARRAY` (any base type) | **unhandled** | Not detected by `types::resolve` at all; behaviour is unverified. See `CLAUDE.md`'s Open items. |
| Anything else | `Utf8` (text) | The unrecognised-type floor; reported via `TableSyncStats.text_fallback_columns`. |

## Security model

See `CLAUDE.md`'s Security requirements and Threat model for the full numbered list.
The two properties worth restating here because they are easy to assume incorrectly:

- **A `SELECT`-only grant is a provisioning requirement, not something this code
  enforces.** Neither `rsfbclient` nor the Firebird wire protocol has a client-side
  read-only mode.
- **A query timeout does not stop a running query.** It stops this library from waiting
  on it any longer; the query itself, and the blocking OS thread running it, continue
  until Firebird's own connection eventually drops or the process exits.

## What is unchanged from tiberiusdelta

`src/catalog.rs`, `src/checkpoint.rs`, `src/merge.rs`, and `src/error.rs` (apart from one
added variant, `Error::ExcludedColumnType`) are driver-agnostic and carried over with
only cosmetic renaming. `src/python.rs` is a near-verbatim port for the same reason: it
only ever talks to `crate::pipeline`'s already-driver-agnostic public types.
