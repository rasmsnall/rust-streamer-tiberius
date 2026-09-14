# API reference

The Python surface is the intended entry point; the underlying Rust crate (`pipeline`,
`catalog`, `types`, `builders`, `connect`, `checkpoint`, `merge`, `error`) is documented
via rustdoc (`cargo doc --open`) and is not repeated here. Full parameter documentation
also lives in `python/firebirddelta/__init__.pyi`, which a type checker or IDE reads
directly; this document is the narrative version with worked examples.

## `sync_tables`

```python
firebirddelta.sync_tables(
    connection_string: str,
    output_uri: str,
    tables: list[TableConfig],
    *,
    checkpoint_uri: str | None = None,
    fetch_batch_size: int = 10_000,
    login_timeout_sec: int | None = 30,
    query_timeout_sec: int | None = 300,
    progress: Callable[[ProgressEvent], Any] | None = None,
) -> SyncReport
```

Syncs every configured table over one connection, sequentially. Each `TableConfig` is:

```python
{"table": "CUSTOMERS", "watermark_column": "UPDATED_AT", "primary_key": "ID"}
# or, for a composite key:
{"table": "ORDER_ITEMS", "watermark_column": "UPDATED_AT", "primary_key": ["ORDER_ID", "LINE_NO"]}
```

`table` is matched against `RDB$RELATION_NAME` exactly as given: an unquoted Firebird
`CREATE TABLE` name is folded to upper case by Firebird itself, so this is normally
all-caps. Firebird has no schema concept, so this is always a single identifier, unlike
tiberiusdelta's `schema.table` or `database.schema.table` forms.

`connection_string` is a `firebird://user:password@host:port/database` URL, for example:

```python
os.environ["FIREBIRD_CONNECTION_STRING"]
# "firebird://svc_reader:s3cret@firebird.internal:3050/PROD"
```

It is redacted from every error message this library raises, including its password
value in both raw and percent-decoded form (see `CLAUDE.md`'s Security requirements).

### Return value

A `SyncReport` with `.tables` (one `TableSyncStats` per table, in configured order) and
`.total_rows_fetched`. Each `TableSyncStats` carries `rows_fetched`, `rows_inserted`,
`rows_updated`, `text_fallback_columns` (synced, degraded to text), and
`excluded_columns` (not synced at all — see below).

### `excluded_columns`, and why it can fail a sync

Firebird 4's `INT128`, `DECFLOAT(16)`, `DECFLOAT(34)`, `TIME WITH TIME ZONE`, and
`TIMESTAMP WITH TIME ZONE` types, and a `BLOB` with an unusual sub-type, cannot be
selected by this library's chosen Firebird client at all in their native form. Most such
columns are simply excluded and reported; **if the excluded column is the configured
watermark column or a primary key column**, the sync for that table raises `ValueError`
(`Error::ExcludedColumnType`) instead, since a sync cannot run without being able to
select the column it filters or merges on. Run `preflight` first against an unfamiliar
schema to see this before it happens mid-run.

## `preflight`

```python
firebirddelta.preflight(
    connection_string: str,
    tables: list[TableConfig],
    *,
    output_uri: str = "",
    checkpoint_uri: str | None = None,
    login_timeout_sec: int | None = 30,
    query_timeout_sec: int | None = 300,
) -> list[TablePreflight]
```

Reads each table's catalog and reports what a real sync would do, without writing
anything. Worked example, checking a schema before pointing a real sync at it:

```python
found = firebirddelta.preflight(connection_string, TABLES)
for t in found:
    if not t.ready:
        print(f"{t.table}: NOT READY (missing PK columns: {t.missing_primary_key_columns})")
        continue
    for c in t.columns:
        if c.excluded:
            print(f"{t.table}.{c.name}: EXCLUDED ({c.source_type})")
        elif not c.recognised:
            print(f"{t.table}.{c.name}: text fallback ({c.source_type})")
```

## `source_watermark` / `set_checkpoint`

The two halves of a bulk backfill, unchanged in shape from tiberiusdelta's own (see
tiberiusdelta's `docs/api.md` for the full procedure, which transfers directly):

```python
watermark = firebirddelta.source_watermark(connection_string, TABLE)  # before exporting
# ... export by whatever bulk means is fastest, load the Delta table directly ...
firebirddelta.set_checkpoint(output_uri, TABLE, watermark)
```

`source_watermark` opens no output and writes nothing; `set_checkpoint` opens no
connection to Firebird at all.

## `firebirddelta.distributed`

```python
from firebirddelta.distributed import sync_tables_distributed
report = sync_tables_distributed(connection_string, output_uri, TABLES, spark=spark)
```

Spreads the table list across Spark executors. Unchanged in design from tiberiusdelta's
own `distributed` module (same partitioning heuristic, same idempotent-retry safety);
see `CLAUDE.md`'s Scaling section for why the *numbers* have not been re-measured for
this crate even though the *design* transfers unchanged.

## Exceptions

| Raised as | When |
|---|---|
| `ConnectionError` | The source could not be reached, or the login was refused. |
| `ValueError` | Bad configuration: missing key, missing watermark/primary key, duplicate table, unsafe name, a watermark/primary key column that does not exist or cannot be selected, a fetched value contradicting its column's type. |
| `RuntimeError` | A Delta write, a checkpoint write, or an internal invariant failed. |
| `firebirddelta.ConcurrentWriteError` (subclasses `RuntimeError`) | Another writer committed to the same Delta table at the same time. Carries a `.table` attribute. Safe to retry. |
| `KeyboardInterrupt` | Ctrl-C between tables. |
