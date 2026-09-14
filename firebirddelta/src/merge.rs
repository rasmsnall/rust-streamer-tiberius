//! Opening a Delta table and applying an upsert (`MERGE`) keyed on a primary key.
//!
//! The only asynchronous module in the crate besides `crate::checkpoint` and the Delta
//! half of `crate::pipeline`, matching pgdelta's own `sink.rs` and tiberiusdelta's own
//! `merge.rs`: delta-rs exposes an async-only API, so a Tokio runtime exists, but it is
//! confined to this module, `crate::checkpoint` (which reuses [`upsert`] for its own tiny
//! table), and the Delta-facing half of `crate::pipeline`. The Firebird-facing half of
//! `crate::pipeline` is synchronous, dispatched onto a blocking task; see that module's
//! docs for why.
//!
//! # Why `MERGE`, not stage-then-commit
//!
//! pgdelta's two-phase stage-then-commit exists because a *full* reload wants
//! all-or-nothing semantics across the *entire* dump. This crate has a different shape,
//! shared with tiberiusdelta: each table is synced independently, from an independent
//! live query, and `CLAUDE.md`'s own threat model already accepts that cross-table
//! consistency is weaker here than a single dump file's. So the atomic unit here is *one
//! table's merge*, not the whole run: `DeltaTable::merge` is itself one atomic Delta
//! commit, and this module does not attempt to batch several tables' merges into one
//! transaction, because sequencing "merge this table's data, then advance its
//! checkpoint" per table, one table at a time, is both simpler and safer than it looks.
//! If the process crashes between a table's merge committing and its checkpoint being
//! written (see `crate::checkpoint`), the checkpoint stays at the old value and the next
//! run re-fetches and re-applies the same rows; because `MERGE` is an upsert,
//! re-applying an already-merged row is idempotent, not a duplicate. The reverse order
//! (checkpoint before merge) would not be safe: a crash between them would permanently
//! skip rows that were never actually merged. This module deliberately never advances a
//! checkpoint itself; the caller (`crate::pipeline`) must call `crate::checkpoint::advance`
//! only after `upsert` here has returned successfully.

use std::sync::Arc;

use deltalake::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use deltalake::datafusion::logical_expr::{Expr, col};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::StructType;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::transaction::TransactionError;
use deltalake::operations::merge::MergeMetrics;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::ensure_table_uri;
use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError, arrow::array::RecordBatch};

use crate::error::{Error, Result};

fn delta_err(e: impl std::fmt::Display) -> Error {
    Error::Delta {
        message: e.to_string(),
    }
}

/// Classifies a Delta failure, separating a lost commit race from everything else.
///
/// Matched on the error's structure rather than its message text, so a reworded
/// diagnostic upstream cannot silently turn a recognised conflict back into an opaque
/// one. Every conflict variant counts, not only `ConcurrentAppend`: from this crate's
/// point of view they all mean the same thing, which is that another writer got there
/// first and this run should simply be repeated.
///
/// `MaxCommitAttempts` is included for the same reason: delta-rs raises it after retrying
/// a conflict as many times as it is willing to, so it is a conflict that did not resolve
/// rather than a distinct kind of failure.
fn commit_err(e: DeltaTableError, table_uri: &str) -> Error {
    let conflict = matches!(
        &e,
        DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_) | TransactionError::MaxCommitAttempts(_),
        }
    );
    if conflict {
        Error::ConcurrentWrite {
            table: table_uri.to_string(),
        }
    } else {
        delta_err(e)
    }
}

/// Opens the Delta table at `uri`, creating it with `schema` on a first run.
///
/// # Errors
///
/// [`Error::Delta`] for a storage or protocol failure, or if an existing table's
/// current schema cannot be read.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn open_or_create(uri: &str, schema: &ArrowSchemaRef) -> Result<DeltaTable> {
    let url = ensure_table_uri(uri).map_err(delta_err)?;
    let mut table = DeltaTableBuilder::from_url(url)
        .map_err(delta_err)?
        .build()
        .map_err(delta_err)?;

    if table.load().await.is_ok() {
        return Ok(table);
    }

    let kernel: StructType = schema.as_ref().try_into_kernel().map_err(delta_err)?;
    table = table
        .create()
        .with_columns(kernel.fields().cloned())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .map_err(delta_err)?;
    Ok(table)
}

/// Upserts `batch` into `table`, matching existing rows on `primary_key` and updating
/// every other column; a row whose key does not match any existing row is inserted.
///
/// One Delta commit, made by `DeltaTable::merge` itself: this function does not add any
/// atomicity of its own, and does not touch a checkpoint. See the module docs for why
/// that is the caller's responsibility, done only after this returns successfully.
///
/// # Errors
///
/// [`Error::Internal`] if `primary_key` is empty (this would match every row against
/// every row, which is never the intended merge), [`Error::ConcurrentWrite`] if another
/// writer committed to the same table first, and [`Error::Delta`] for any other storage
/// or protocol failure.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn upsert(
    table: DeltaTable,
    batch: RecordBatch,
    primary_key: &[String],
) -> Result<(DeltaTable, MergeMetrics)> {
    let predicate = primary_key
        .iter()
        .map(|k| col(format!("target.{k}")).eq(col(format!("source.{k}"))))
        .reduce(Expr::and)
        .ok_or(Error::Internal {
            detail: "upsert called with an empty primary key",
        })?;

    let columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let table_uri = table.table_url().to_string();
    let ctx = SessionContext::new();
    let source = ctx.read_batch(batch).map_err(delta_err)?;
    let session_state = Arc::new(ctx.state());

    let update_columns = columns.clone();
    let insert_columns = columns;

    table
        .merge(source, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(session_state)
        .when_matched_update(|update| {
            update_columns
                .iter()
                .fold(update, |u, c| u.update(c, col(format!("source.{c}"))))
        })
        .map_err(delta_err)?
        .when_not_matched_insert(|insert| {
            insert_columns
                .iter()
                .fold(insert, |i, c| i.set(c, col(format!("source.{c}"))))
        })
        .map_err(delta_err)?
        .await
        .map_err(|e| commit_err(e, &table_uri))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    fn schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("firebirddelta-merge-{tag}-{}", std::process::id()));
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
    fn a_first_run_creates_and_inserts() {
        let dir = tmpdir("first-run");
        rt().block_on(async {
            let table = open_or_create(&uri(&dir), &schema()).await.unwrap();
            let pk = vec!["id".to_string()];
            let (table, metrics) = upsert(table, batch(&[1, 2], &["alice", "bob"]), &pk)
                .await
                .unwrap();
            assert_eq!(metrics.num_target_rows_inserted, 2);
            assert_eq!(metrics.num_target_rows_updated, 0);
            assert_eq!(table.version(), Some(1));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The property incremental sync depends on: re-applying the same row must update
    /// it in place, not create a duplicate, since a crash between a merge and its
    /// checkpoint advancing means the next run re-fetches and re-merges it.
    #[test]
    fn reapplying_the_same_row_updates_rather_than_duplicates() {
        let dir = tmpdir("idempotent");
        rt().block_on(async {
            let pk = vec!["id".to_string()];
            let table = open_or_create(&uri(&dir), &schema()).await.unwrap();
            let (table, _) = upsert(table, batch(&[1], &["alice"]), &pk).await.unwrap();

            // Same key, changed value: must update in place.
            let (table, metrics) = upsert(table, batch(&[1], &["alice-v2"]), &pk)
                .await
                .unwrap();
            assert_eq!(metrics.num_target_rows_updated, 1);
            assert_eq!(metrics.num_target_rows_inserted, 0);

            // A new key alongside an existing one: the existing row is untouched, the
            // new one inserted.
            let (table, metrics) = upsert(table, batch(&[1, 2], &["alice-v2", "bob"]), &pk)
                .await
                .unwrap();
            assert_eq!(metrics.num_target_rows_inserted, 1);
            let _ = table;
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_empty_primary_key_is_rejected() {
        let dir = tmpdir("empty-pk");
        rt().block_on(async {
            let table = open_or_create(&uri(&dir), &schema()).await.unwrap();
            let err = upsert(table, batch(&[1], &["alice"]), &[])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Internal { .. }));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
