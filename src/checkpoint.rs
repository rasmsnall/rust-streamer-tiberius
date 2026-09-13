//! The `_streamer_checkpoints` Delta table: where each source table's last-synced
//! watermark value is recorded, and read back at the start of the next run.
//!
//! Plays the same role pgdelta's `_pgdelta_loads` manifest plays for its own history,
//! but is read as well as written, since incremental sync needs to know where it left
//! off. One row per source table, upserted (never appended) via `crate::merge::upsert`
//! keyed on `table_name`, so there is always exactly one current row per table rather
//! than a growing audit log.
//!
//! Reading it back is done by listing the table's own visible Parquet files and parsing
//! them directly (see [`read_all`]), rather than through DataFusion's query engine:
//! `datafusion` is already a dependency for `crate::merge`'s upsert, but wiring a
//! `TableProvider` for a table this small would be more moving parts for no benefit.
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

/// Qualified source table name; the merge key, one row per table.
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

/// Reads every table's last-synced watermark value from the checkpoint table at
/// `checkpoint_uri`.
///
/// Returns an empty map if the checkpoint table does not exist yet: a first run, before
/// any table has ever been synced, is not an error.
///
/// # Errors
///
/// [`Error::Checkpoint`] if the table exists but its files could not be read, or its
/// schema disagrees with what this module expects (which would mean the table at this
/// URI is not one this crate wrote).
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn read_all(checkpoint_uri: &str) -> Result<HashMap<String, String>> {
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
    checkpoint_uri: &str,
    table: &str,
    watermark_column: &str,
    last_value: &str,
    synced_at_micros: i64,
) -> Result<()> {
    let delta_table = merge::open_or_create(checkpoint_uri, &schema()).await?;
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
            "tiberiusdelta-checkpoint-{tag}-{}",
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
    fn reading_a_checkpoint_table_that_does_not_exist_yet_is_empty_not_an_error() {
        let dir = tmpdir("missing");
        rt().block_on(async {
            let map = read_all(&uri(&dir)).await.unwrap();
            assert!(map.is_empty());
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn advance_then_read_round_trips() {
        let dir = tmpdir("roundtrip");
        rt().block_on(async {
            let checkpoint_uri = uri(&dir);
            advance(
                &checkpoint_uri,
                "dbo.customers",
                "updated_at",
                "2026-01-03 12:00:00",
                0,
            )
            .await
            .unwrap();
            let map = read_all(&checkpoint_uri).await.unwrap();
            assert_eq!(
                map.get("dbo.customers").map(String::as_str),
                Some("2026-01-03 12:00:00")
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The property incremental sync depends on: a second `advance` call for the same
    /// table must replace its checkpoint, not add a second row that would make "the"
    /// last-synced value ambiguous.
    #[test]
    fn advancing_the_same_table_again_replaces_its_checkpoint() {
        let dir = tmpdir("replace");
        rt().block_on(async {
            let checkpoint_uri = uri(&dir);
            advance(&checkpoint_uri, "dbo.customers", "updated_at", "v1", 0)
                .await
                .unwrap();
            advance(&checkpoint_uri, "dbo.customers", "updated_at", "v2", 1)
                .await
                .unwrap();
            let map = read_all(&checkpoint_uri).await.unwrap();
            assert_eq!(map.get("dbo.customers").map(String::as_str), Some("v2"));
            assert_eq!(map.len(), 1, "must not accumulate a row per call");
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multiple_tables_are_tracked_independently() {
        let dir = tmpdir("multi");
        rt().block_on(async {
            let checkpoint_uri = uri(&dir);
            advance(&checkpoint_uri, "dbo.customers", "updated_at", "v1", 0)
                .await
                .unwrap();
            advance(&checkpoint_uri, "dbo.orders", "id", "100", 0)
                .await
                .unwrap();
            let map = read_all(&checkpoint_uri).await.unwrap();
            assert_eq!(map.get("dbo.customers").map(String::as_str), Some("v1"));
            assert_eq!(map.get("dbo.orders").map(String::as_str), Some("100"));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
