# firebirddelta

Stream a live **Firebird** database into **Delta Lake** tables, **incrementally**. Rust
core, Python bindings, intended for use from Databricks.

Each run pulls only the rows new or changed since that table's own last successful run,
and merges them into a Delta table keyed on its primary key.

```
Firebird -> SELECT ... WHERE watermark > checkpoint -> Arrow -> Delta MERGE
```

Sibling project to [`rust-streamer-pgdb`](https://github.com/rasmsnall/rust-streamer-pgdb)
(pgdelta) and to the `tiberiusdelta` crate in this same repository, which does the same
job for SQL Server. See [`CLAUDE.md`](CLAUDE.md) for the full design, including what
changed (and what could not simply be copied) in adapting tiberiusdelta's design to a
different engine with a synchronous driver.

**Status: not yet run against a live Firebird instance.** The crate compiles, its unit
tests and doctests pass, and `clippy`/`fmt`/`cargo doc` are all clean, but no Docker
daemon was available in the session that wrote it. Read `CLAUDE.md`'s Status and Open
items before relying on this for anything beyond review.

## Install

```bash
pip install firebirddelta-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

One `abi3` wheel loads on CPython 3.10 and later. **Nothing else to install**: no
`fbclient` driver, no driver binary, no C toolchain. Connectivity is
[`rsfbclient`](https://docs.rs/rsfbclient)'s pure-Rust wire-protocol backend, which
speaks Firebird's own protocol over a plain socket. Unlike `tiberius` in the sibling
crate, `rsfbclient` is synchronous; see `CLAUDE.md` for how this crate bridges that.

## Use

```python
import os
import firebirddelta

TABLES = [
    {"table": "CUSTOMERS", "watermark_column": "UPDATED_AT", "primary_key": "ID"},
    {"table": "INVOICES", "watermark_column": "MODIFIED_AT", "primary_key": "INVOICE_ID"},
]

report = firebirddelta.sync_tables(
    os.environ["FIREBIRD_CONNECTION_STRING"],
    "/Volumes/main/raw/firebird/",
    TABLES,
)

for t in report.tables:
    print(t.table, t.rows_fetched, t.rows_inserted, t.rows_updated)
```

Check the configuration against the live source first, without writing anything:

```python
for t in firebirddelta.preflight(connection_string, TABLES):
    print(t.table, "ready" if t.ready else "NOT READY")
```

## How it works

For each configured table, in this order and never the reverse:

```
read checkpoint -> SELECT rows above it -> MERGE into Delta (commits) -> advance checkpoint
```

Advancing the checkpoint last is what makes a crash safe; see `CLAUDE.md`'s Pipeline
section. Each table gets **its own** checkpoint Delta table, at
`_firebirddelta_checkpoints/<table>` — one path component, not several, since Firebird
has no schema concept at all, unlike SQL Server's `database.schema.table`.

## What it will not do

Stated up front, because each of these is silent rather than loud; see `CLAUDE.md`'s
"What incremental sync does not do" for the full list. Two are specific to this crate,
not inherited from its siblings:

- **`NUMERIC`/`DECIMAL` columns are written as a 64-bit float, not an exact decimal.**
  This crate's chosen Firebird client decodes every scaled-integer column through a
  wire-level `DOUBLE` before this library ever sees the value; the precision loss
  happens upstream of anything this library could fix. See `CLAUDE.md`'s Driver notes.
- **Firebird `ARRAY` columns are not detected or handled.** Untested; see `CLAUDE.md`'s
  Open items.

## Requirements

- A **`SELECT`-only** database account. The library issues only `SELECT`, but the
  Firebird wire protocol has no client-side read-only mode, so the grant is where that
  guarantee actually lives.
- A TCP route from the compute to the instance.
- An **index on each watermark column**, or every run is a full scan and a sort.
- Output to an external location or a `/Volumes/...` path, never a Unity Catalog
  *managed* table.
- A scheduled `VACUUM`. Merges rewrite files; the superseded ones stay until vacuumed.

## Documentation

| Document | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Design, concurrency, failure model, type mapping, security model |
| [`docs/api.md`](docs/api.md) | Python surface, parameters, statistics, worked examples |
| [`docs/operations.md`](docs/operations.md) | Deploying, provisioning, monitoring, recovery, runbook |

## Development

```bash
cargo test --lib --doc                 # unit tests and doctests, no database needed
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The end-to-end tests need a Firebird instance (untested in this session; see
`CLAUDE.md`'s Environment notes for the intended container and version):

```bash
docker run -d --name firebirddelta-test-firebird -p 3050:3050 \
  -e FIREBIRD_DATABASE=firebirddelta_test.fdb \
  -e FIREBIRD_ROOT_PASSWORD='Test_Passw0rd!2026' \
  firebirdsql/firebird:5.0.4

export FIREBIRD_PASSWORD='Test_Passw0rd!2026'
pip install firebird-driver
python tools/seed.py
cargo test --test firebird_live
```

`.github/workflows/firebirddelta-ci.yml` runs the same steps in CI, plus builds and
smoke-tests the wheel.

## Status

Compiles and passes every gate that does not need a live server. **Not yet confirmed to
work against any Firebird instance**, live or local, by this session's own hand — no
Docker daemon was available to run it here. `.github/workflows/firebirddelta-ci.yml`
exists to close that gap the first time it runs; see `CLAUDE.md`'s Status and Open
items for what "not yet confirmed" covers precisely.

## License

MIT. See [LICENSE](LICENSE).
