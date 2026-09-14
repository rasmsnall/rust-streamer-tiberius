//! Checkpoints: where each source table's last-synced watermark value is recorded, and
//! read back at the start of the next run.
//!
//! Plays the same role pgdelta's `_pgdelta_loads` manifest, and tiberiusdelta's
//! `_streamer_checkpoints`, play for their own history, but is read as well as written,
//! since incremental sync needs to know where it left off.
//!
//! # One Delta table per source table
//!
//! Checkpoints live at `<checkpoint_uri>/<table>`, mirroring the layout of the data
//! tables themselves, each holding the single current row for its own source table.
//!
//! This is a deliberate choice over the obvious alternative, one shared table with a row
//! per source table, and the reason is concurrency. A shared table is a single Delta
//! table that every table's sync must commit to, so two syncs running at once contend on
//! it, and Delta resolves that contention by failing one of them
//! ([`crate::Error::ConcurrentWrite`]). That is survivable for one process syncing tables
//! one at a time, and fatal to the goal of scaling out: distributing tables across
//! Databricks workers means many processes advancing checkpoints at once, and they would
//! all serialise through, and fight over, that one table. Giving each source table its
//! own checkpoint removes the shared mutable resource entirely, so no two workers ever
//! write the same Delta table and scaling out needs no coordination at all.
//!
//! The cost is that `SELECT * FROM _firebirddelta_checkpoints` no longer shows everything
//! at once. Each checkpoint still carries its own `table_name`, so a union view over the
//! prefix restores that; see `docs/operations.md`.
//!
//! Reading one back parses its visible Parquet files directly (see [`read`]), rather than
//! going through DataFusion's query engine: `datafusion` is already a dependency for
//! `crate::merge`'s upsert, but wiring a `TableProvider` for a table with one row would be
//! more moving parts for no benefit.
//!
//! Async throughout; must run on a Tokio runtime, the same as `crate::merge`.

use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use deltalake::logstore::object_store::ObjectStoreExt;
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use futures::TryStreamExt;

use crate::error::{Error, Result};
use crate::merge;

/// Timezone stamped on `synced_at`. Matches `crate::builders::UTC`.
const UTC: &str = "UTC";

// Column names of the checkpoint table, named so a caller reading the raw Delta table
// directly (for an audit query, say) knows what to expect without reading this source.

/// Source table name. Redundant with the checkpoint's own path, and kept so a union view
/// over the prefix is self-describing.
pub const TABLE_NAME_COLUMN: &str = "table_name";
/// The column that was filtered on to produce this checkpoint's `last_value`.
pub const WATERMARK_COLUMN_COLUMN: &str = "watermark_column";
/// The watermark value, as text, up to which the table has been synced.
pub const LAST_VALUE_COLUMN: &str = "last_value";
/// When this checkpoint was recorded, microseconds since the epoch, UTC.
pub const SYNCED_AT_COLUMN: &str = "synced_at";

fn schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new(TABLE_NAME_COLUMN, DataType::Utf8, false),
        Field::new(WATERMARK_COLUMN_COLUMN, DataType::Utf8, false),
        Field::new(LAST_VALUE_COLUMN, DataType::Utf8, false),
        Field::new(
            SYNCED_AT_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            false,
        ),
    ]))
}

fn delta_err(e: impl std::fmt::Display) -> Error {
    Error::Checkpoint {
        message: e.to_string(),
    }
}

/// Where one source table's checkpoint lives beneath `checkpoint_uri`.
///
/// `relative` is the same path component the table's own data is written under, so the
/// two layouts mirror each other and a checkpoint is findable from its table by
/// inspection.
#[must_use]
pub fn uri_for(checkpoint_uri: &str, relative: &str) -> String {
    format!("{}/{}", checkpoint_uri.trim_end_matches('/'), relative)
}

/// Reads one table's last-synced watermark value.
///
/// `uri` is that table's own checkpoint, as [`uri_for`] builds it. Returns `None` if it
/// does not exist yet: a first sync, before the table has ever been synced, is not an
/// error.
///
/// # Errors
///
/// [`Error::Checkpoint`] if the checkpoint exists but its files could not be read, or its
/// schema disagrees with what this module expects (which would mean the table at this URI
/// is not one this crate wrote).
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn read(uri: &str) -> Result<Option<String>> {
    Ok(read_rows(uri).await?.into_values().next())
}

/// Reads every row of the checkpoint at `uri`, keyed by source table name.
///
/// Retained as the shared implementation of [`read`]: a per-table checkpoint holds
/// exactly one row, but reading it as a map keeps this honest about what the file
/// actually contains rather than assuming.
async fn read_rows(checkpoint_uri: &str) -> Result<HashMap<String, String>> {
    let url = deltalake::table::builder::ensure_table_uri(checkpoint_uri).map_err(delta_err)?;
    let mut table = match deltalake::DeltaTableBuilder::from_url(url) {
        Ok(builder) => match builder.build() {
            Ok(t) => t,
            Err(e) => return Err(delta_err(e)),
        },
        Err(e) => return Err(delta_err(e)),
    };
    if table.load().await.is_err() {
        // No `_delta_log` yet: nothing has ever been checkpointed.
        return Ok(HashMap::new());
    }

    let log_store = table.log_store();
    let object_store = table.object_store();
    let Ok(snapshot) = table.snapshot() else {
        return Ok(HashMap::new());
    };
    let files: Vec<_> = snapshot
        .snapshot()
        .file_views(&log_store, None)
        .map_ok(|f| f.path().to_string())
        .try_collect()
        .await
        .map_err(delta_err)?;

    let mut out = HashMap::with_capacity(files.len());
    for path in files {
        let bytes = object_store
            .get(&path.clone().into())
            .await
            .map_err(delta_err)?
            .bytes()
            .await
            .map_err(delta_err)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(delta_err)?
            .build()
            .map_err(delta_err)?;
        for batch in reader {
            let batch = batch.map_err(delta_err)?;
            extend_from_batch(&mut out, &batch)?;
        }
    }
    Ok(out)
}

/// Reads `table_name` and `last_value` out of one batch, overwriting any earlier entry
/// for the same table: Parquet row groups have no defined order across files, so the
/// caller cannot assume the last-written row is read last. Since a table's checkpoint is
/// upserted (one row per table, never appended), every batch this module ever reads back
/// holds each table's single current row, so which occurrence "wins" only matters if the
/// same table's row is ever split across files, which correct use of `crate::merge`
/// never produces.
fn extend_from_batch(out: &mut HashMap<String, String>, batch: &RecordBatch) -> Result<()> {
    let names = column_as_utf8(batch, TABLE_NAME_COLUMN)?;
    let values = column_as_utf8(batch, LAST_VALUE_COLUMN)?;
    // Both columns are declared non-nullable in `schema()`, so every index in range is a
    // real value; nothing here needs to tolerate a null.
    for i in 0..batch.num_rows() {
        out.insert(names.value(i).to_string(), values.value(i).to_string());
    }
    Ok(())
}

/// Reads column `name` as a plain `StringArray` (Arrow `Utf8`), normalising it there if
/// necessary.
///
/// Verified this session: a batch written through `crate::merge::upsert` (which routes
/// data through DataFusion) does not reliably keep the `Utf8` physical type this
/// module's own `schema()` declares for a string column; DataFusion's own execution can
/// produce `Utf8View` instead, a different in-memory representation of the same logical
/// string type. Casting explicitly, rather than downcasting straight to `StringArray`
/// and failing on anything else, is what makes this module robust to that: a schema
/// mismatch between what was declared and what was actually returned must never crash
/// or silently misread data, only fail loudly for a genuinely wrong type (see the
/// `Err` path in [`Error::Checkpoint`] below).
fn column_as_utf8(batch: &RecordBatch, name: &str) -> Result<StringArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Checkpoint {
            message: format!("checkpoint table is missing its {name} column"),
        })?;
    let column = batch.column(idx);
    let cast = deltalake::arrow::compute::cast(column, &DataType::Utf8).map_err(|_| {
        Error::Checkpoint {
            message: format!("checkpoint table's {name} column is not a textual type"),
        }
    })?;
    cast.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| Error::Checkpoint {
            message: format!("checkpoint table's {name} column is not the expected type"),
        })
}

/// Records that `table` has been synced up to `last_value` of `watermark_column`.
///
/// `uri` is that table's own checkpoint, as [`uri_for`] builds it; no other source table
/// shares it, which is what lets independent workers advance checkpoints concurrently.
/// Upserts (see `crate::merge::upsert`) keyed on the table name, so a later call for the
/// same table replaces its row rather than adding another. Must only be called after the
/// corresponding data merge has already committed successfully; see the module docs and
/// `crate::merge` for why that order is the one that keeps a crash mid-run safe to
/// retry.
///
/// # Errors
///
/// [`Error::Delta`] for a commit conflict or storage failure.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn advance(
    uri: &str,
    table: &str,
    watermark_column: &str,
    last_value: &str,
    synced_at_micros: i64,
) -> Result<()> {
    let delta_table = merge::open_or_create(uri, &schema()).await?;
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec![table])),
            Arc::new(StringArray::from(vec![watermark_column])),
            Arc::new(StringArray::from(vec![last_value])),
            Arc::new(TimestampMicrosecondArray::from(vec![synced_at_micros]).with_timezone(UTC)),
        ],
    )
    .map_err(|e| Error::Arrow {
        message: e.to_string(),
    })?;
    merge::upsert(delta_table, batch, &[TABLE_NAME_COLUMN.to_string()])
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "firebirddelta-checkpoint-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn uri(dir: &std::path::Path) -> String {
        format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn reading_a_checkpoint_that_does_not_exist_yet_is_none_not_an_error() {
        let dir = tmpdir("missing");
        rt().block_on(async {
            let found = read(&uri_for(&uri(&dir), "CUSTOMERS")).await.unwrap();
            assert!(found.is_none(), "a first sync is not an error");
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn advance_then_read_round_trips() {
        let dir = tmpdir("roundtrip");
        rt().block_on(async {
            let at = uri_for(&uri(&dir), "CUSTOMERS");
            advance(&at, "CUSTOMERS", "UPDATED_AT", "2026-01-03 12:00:00", 0)
                .await
                .unwrap();
            assert_eq!(
                read(&at).await.unwrap().as_deref(),
                Some("2026-01-03 12:00:00")
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The property incremental sync depends on: a second `advance` for the same table
    /// must replace its checkpoint, not add a second row that would make "the"
    /// last-synced value ambiguous.
    #[test]
    fn advancing_the_same_table_again_replaces_its_checkpoint() {
        let dir = tmpdir("replace");
        rt().block_on(async {
            let at = uri_for(&uri(&dir), "CUSTOMERS");
            advance(&at, "CUSTOMERS", "UPDATED_AT", "v1", 0)
                .await
                .unwrap();
            advance(&at, "CUSTOMERS", "UPDATED_AT", "v2", 1)
                .await
                .unwrap();
            assert_eq!(read(&at).await.unwrap().as_deref(), Some("v2"));
            assert_eq!(
                read_rows(&at).await.unwrap().len(),
                1,
                "must not accumulate a row per call"
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The property this layout exists for: two source tables share no Delta table, so
    /// two workers advancing their own checkpoints can never contend. If these ever
    /// resolved to one location, scaling out would serialise through it and conflict.
    #[test]
    fn each_table_gets_its_own_checkpoint_location() {
        let prefix = "file:///out/_firebirddelta_checkpoints";
        let customers = uri_for(prefix, "CUSTOMERS");
        let orders = uri_for(prefix, "ORDERS");
        assert_ne!(customers, orders);
        assert_eq!(
            customers,
            "file:///out/_firebirddelta_checkpoints/CUSTOMERS"
        );

        // A trailing separator on the prefix must not produce a doubled one, which would
        // be a different path on an object store and silently orphan the checkpoint.
        assert_eq!(
            uri_for("file:///out/_c/", "CUSTOMERS"),
            customers.replace("_firebirddelta_checkpoints", "_c")
        );
    }

    #[test]
    fn multiple_tables_are_tracked_independently() {
        let dir = tmpdir("multi");
        rt().block_on(async {
            let prefix = uri(&dir);
            let customers = uri_for(&prefix, "CUSTOMERS");
            let orders = uri_for(&prefix, "ORDERS");
            advance(&customers, "CUSTOMERS", "UPDATED_AT", "v1", 0)
                .await
                .unwrap();
            advance(&orders, "ORDERS", "ID", "100", 0).await.unwrap();
            assert_eq!(read(&customers).await.unwrap().as_deref(), Some("v1"));
            assert_eq!(read(&orders).await.unwrap().as_deref(), Some("100"));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
