//! Orchestration: the per-table incremental sync loop.
//!
//! Ties `crate::connect` (open a connection), `crate::types` and `crate::builders`
//! (resolve columns, decode fetched rows into Arrow), `crate::merge` (upsert into Delta),
//! and `crate::checkpoint` (record how far this table has been synced) together into one
//! call per table. See `crate::merge`'s module docs for why a checkpoint is only ever
//! advanced after its table's merge has already committed.
//!
//! Async throughout; must run on a Tokio runtime. Unlike this crate's earlier ODBC-based
//! version, the fetch side is async too: `tiberius` speaks TDS over the same runtime the
//! Delta write path already needed, so there is no blocking call to dispatch from inside
//! async code.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tiberius::Row;

use crate::catalog::{SyncCatalog, TableSync};
use crate::connect::{self, ConnectConfig, SqlClient};
use crate::error::{Error, Result};
use crate::types::{self, ResolvedType};
use crate::{builders, checkpoint, merge};

/// Settings shared by every table one call to [`sync_table`] covers.
#[derive(Clone)]
pub struct SyncConfig {
    /// How to reach the source database.
    pub connect: ConnectConfig,
    /// Prefix each table's Delta table is written beneath, for example
    /// `/Volumes/main/raw/mssql/`. Mirrors pgdelta's `LoadConfig::output_uri`.
    pub output_uri: String,
    /// Where the checkpoint table lives, for example
    /// `<output_uri>/_streamer_checkpoints`. Kept as its own field, rather than always
    /// derived from `output_uri`, so a caller can point several sync configs at
    /// independently checkpointed prefixes if that is ever wanted.
    pub checkpoint_uri: String,
    /// Rows accumulated per merge into Delta. Bounds memory: this is Security
    /// requirement 5 in `CLAUDE.md`, not a performance knob to maximise blindly.
    pub fetch_batch_size: usize,
    /// Seconds the source may go without producing the next row before the sync gives
    /// up. `None` waits indefinitely.
    ///
    /// A per-row deadline rather than a whole-query one, because `tiberius` streams:
    /// what needs bounding is a query that stops making progress (Security requirement
    /// 8), not a large table that is legitimately still delivering rows.
    pub query_timeout_sec: Option<u64>,
}

/// What one table's sync produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSyncStats {
    /// Qualified table name.
    pub table: String,
    /// Rows fetched from the source in this run (new or changed since the last
    /// checkpoint, or every row on a first sync).
    pub rows_fetched: u64,
    /// Rows the merge inserted (keys not previously present).
    pub rows_inserted: usize,
    /// Rows the merge updated (keys already present).
    pub rows_updated: usize,
    /// Columns whose source type this crate does not map, and which were therefore
    /// written as text. Reported rather than logged, matching pgdelta's own
    /// `text_fallback_columns`: a wide production schema will have some, and that is a
    /// fact for the caller to see, not a failure.
    pub text_fallback_columns: Vec<String>,
}

/// Rejects a table or column name unsuitable for interpolation into a `SELECT`.
///
/// Table and column names cannot be bound as query parameters (only values can); they
/// necessarily become part of the SQL text itself. These names come from this crate's own
/// caller-supplied [`TableSync`] configuration, not from the source database's data, so
/// this is defence in depth rather than a response to an untrusted input the way
/// pgdelta's `sink::relative_path` is. Still checked, not assumed: a config value
/// copy-pasted from somewhere unexpected is exactly the case this guards.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] if `name` contains a quote, semicolon, or backslash, or is
/// empty.
fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() || name.contains(['\'', '"', ';', '\\', '\0']) {
        return Err(Error::UnsafeTableName {
            name: name.to_string(),
        });
    }
    Ok(())
}

/// Maps a qualified source table name to a relative Delta output path.
///
/// `dbo.customers` becomes `dbo/customers`; a bare name with no schema stays as-is. Does
/// not attempt pgdelta's full path-traversal defence (there is no untrusted third party
/// here to defend against, see `CLAUDE.md`'s threat model), but still rejects `.`/`..`
/// path segments defensively, the cheap and clearly correct part of that defence.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] if a component is empty or is exactly `.` or `..`.
fn relative_path(table: &str) -> Result<String> {
    validate_identifier(table)?;
    let parts: Vec<&str> = table.split('.').collect();
    for part in &parts {
        if part.is_empty() || *part == "." || *part == ".." {
            return Err(Error::UnsafeTableName {
                name: table.to_string(),
            });
        }
    }
    Ok(parts.join("/"))
}

/// Splits `dbo.customers` into its schema and table halves; a bare name has no schema.
fn split_qualified(table: &str) -> (Option<&str>, &str) {
    match table.split_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, table),
    }
}

/// Reads a table's column names and resolved types from `INFORMATION_SCHEMA.COLUMNS`.
///
/// Columns come back in `ORDINAL_POSITION` order, which is the order `SELECT *` produces
/// them, so the result lines up positionally with each fetched row's own cells. See
/// `crate::types`'s module docs for why the catalog, and not the TDS wire type, is what
/// the Arrow schema is built from.
///
/// `NUMERIC_PRECISION` and `NUMERIC_SCALE` are cast to `int` in the query rather than
/// read at their catalog-declared widths (`tinyint` and `int` respectively), so one
/// decoded type covers both regardless of what a given SQL Server version declares them
/// as.
///
/// # Errors
///
/// [`Error::Query`] if the lookup fails, and [`Error::Internal`] if it returns no rows,
/// which means the table does not exist or the connected account cannot see it.
async fn describe_table(
    client: &mut SqlClient,
    config: &SyncConfig,
    table: &str,
) -> Result<Vec<(String, ResolvedType)>> {
    const PROJECTION: &str = "SELECT COLUMN_NAME, DATA_TYPE, \
         CAST(NUMERIC_PRECISION AS int) AS NUMERIC_PRECISION, \
         CAST(NUMERIC_SCALE AS int) AS NUMERIC_SCALE \
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @P1";

    let (schema, name) = split_qualified(table);
    let rows = match schema {
        Some(schema) => {
            let sql = format!("{PROJECTION} AND TABLE_SCHEMA = @P2 ORDER BY ORDINAL_POSITION");
            client.query(sql, &[&name, &schema]).await
        }
        None => {
            let sql =
                format!("{PROJECTION} AND TABLE_SCHEMA = SCHEMA_NAME() ORDER BY ORDINAL_POSITION");
            client.query(sql, &[&name]).await
        }
    }
    .map_err(|e| connect::query_error(&e, &config.connect.connection_string))?
    .into_first_result()
    .await
    .map_err(|e| connect::query_error(&e, &config.connect.connection_string))?;

    if rows.is_empty() {
        return Err(Error::Internal {
            detail: "the source table has no columns, or is not visible to this account",
        });
    }

    let mut columns = Vec::with_capacity(rows.len());
    for row in &rows {
        let name: &str = row
            .try_get("COLUMN_NAME")
            .ok()
            .flatten()
            .unwrap_or_default();
        let data_type: &str = row.try_get("DATA_TYPE").ok().flatten().unwrap_or_default();
        let precision: Option<i32> = row.try_get("NUMERIC_PRECISION").ok().flatten();
        let scale: Option<i32> = row.try_get("NUMERIC_SCALE").ok().flatten();
        columns.push((
            name.to_string(),
            types::resolve(data_type, precision.map(i64::from), scale.map(i64::from)),
        ));
    }
    Ok(columns)
}

/// Awaits `future`, giving up after `timeout_sec` if it is set.
async fn with_timeout<T>(
    timeout_sec: Option<u64>,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match timeout_sec {
        Some(secs) => tokio::time::timeout(Duration::from_secs(secs), future)
            .await
            .map_err(|_| Error::Query {
                message: format!("the source produced no further rows within {secs}s"),
            })?,
        None => future.await,
    }
}

/// Syncs one table: fetches rows new or changed since its last checkpoint (or every row,
/// on a first sync), upserts them into its Delta table, and advances its checkpoint only
/// once that upsert has committed.
///
/// The query is always `ORDER BY <watermark_column> ASC`, so the watermark value of the
/// last row fetched is always the greatest one seen, regardless of whether the column is
/// numeric, textual, or temporal: this crate compares nothing itself and trusts the
/// source database's own ordering, rather than re-implementing type-aware comparison for
/// a column whose type it may not even map (see `crate::types`).
///
/// # Errors
///
/// [`Error::IncrementalConfigMissing`] if `table_sync` is not fully configured;
/// [`Error::UnsafeTableName`] if its table name or watermark column is unsafe to
/// interpolate into SQL; [`Error::Connect`] or [`Error::Query`] for a connection or query
/// failure; [`Error::UnparsableValue`] if a fetched value contradicts its column's
/// resolved type; and [`Error::Delta`]/[`Error::Checkpoint`] for a storage or commit
/// failure.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn sync_table(config: &SyncConfig, table_sync: &TableSync) -> Result<TableSyncStats> {
    let mut client = connect::open(&config.connect).await?;
    sync_one(&mut client, config, table_sync).await
}

/// The body of [`sync_table`], against an already-open client.
///
/// Split out so [`run`] can sync a whole catalog over one connection instead of logging
/// in once per table: a run covering many tables otherwise pays a TDS login, and possibly
/// a TLS handshake, for each one.
async fn sync_one(
    client: &mut SqlClient,
    config: &SyncConfig,
    table_sync: &TableSync,
) -> Result<TableSyncStats> {
    table_sync.validate()?;
    validate_identifier(&table_sync.table)?;
    validate_identifier(&table_sync.watermark_column)?;
    for key in &table_sync.primary_key {
        validate_identifier(key)?;
    }

    let checkpoints = checkpoint::read_all(&config.checkpoint_uri).await?;
    let last_value = checkpoints.get(&table_sync.table).cloned();

    let columns = describe_table(client, config, &table_sync.table).await?;

    let watermark_index = columns
        .iter()
        .position(|(name, _)| name.eq_ignore_ascii_case(&table_sync.watermark_column))
        .ok_or(Error::Internal {
            detail: "the watermark column is not a column of this table",
        })?;
    let text_fallback_columns: Vec<String> = columns
        .iter()
        .filter(|(_, rt)| !rt.recognised)
        .map(|(name, _)| name.clone())
        .collect();

    let table_uri = format!(
        "{}/{}",
        config.output_uri.trim_end_matches('/'),
        relative_path(&table_sync.table)?
    );
    let arrow_schema = Arc::new(builders::arrow_schema(&columns));
    let mut delta_table = merge::open_or_create(&table_uri, &arrow_schema).await?;

    let query = format!(
        "SELECT * FROM {} {} ORDER BY {} ASC",
        table_sync.table,
        if last_value.is_some() {
            format!("WHERE {} > @P1", table_sync.watermark_column)
        } else {
            String::new()
        },
        table_sync.watermark_column,
    );

    let stream = match &last_value {
        Some(v) => client.query(query, &[&v.as_str()]).await,
        None => client.query(query, &[]).await,
    }
    .map_err(|e| connect::query_error(&e, &config.connect.connection_string))?;
    let mut rows = stream.into_row_stream();

    let mut stats = TableSyncStats {
        table: table_sync.table.clone(),
        rows_fetched: 0,
        rows_inserted: 0,
        rows_updated: 0,
        text_fallback_columns,
    };
    let mut newest_watermark: Option<String> = None;
    let mut batch: Vec<Row> = Vec::with_capacity(config.fetch_batch_size);

    loop {
        let next = with_timeout(config.query_timeout_sec, async {
            match rows.next().await {
                Some(row) => row
                    .map(Some)
                    .map_err(|e| connect::query_error(&e, &config.connect.connection_string)),
                None => Ok(None),
            }
        })
        .await?;

        let end_of_stream = next.is_none();
        if let Some(row) = next {
            batch.push(row);
            if batch.len() < config.fetch_batch_size {
                continue;
            }
        }
        if batch.is_empty() {
            break;
        }

        // The query orders by the watermark ascending, so the last row of the last batch
        // carries the greatest value seen; tracking it per batch keeps that true without
        // holding every fetched row alive to the end of the sync.
        if let Some(last) = batch.last()
            && let Some((_, data)) = last.cells().nth(watermark_index)
            && let Some(rendered) = builders::render_text(data)
        {
            newest_watermark = Some(rendered);
        }

        let record_batch = builders::record_batch(&columns, &batch)?;
        stats.rows_fetched += record_batch.num_rows() as u64;
        batch.clear();

        let (updated_table, metrics) =
            merge::upsert(delta_table, record_batch, &table_sync.primary_key).await?;
        delta_table = updated_table;
        stats.rows_inserted += metrics.num_target_rows_inserted;
        stats.rows_updated += metrics.num_target_rows_updated;

        if end_of_stream {
            break;
        }
    }

    if let Some(new_value) = newest_watermark {
        let synced_at_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        checkpoint::advance(
            &config.checkpoint_uri,
            &table_sync.table,
            &table_sync.watermark_column,
            &new_value,
            synced_at_micros,
        )
        .await?;
    }

    Ok(stats)
}

/// Progress reported after each table finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    /// The table that just finished.
    pub table: String,
    /// Tables finished so far, including this one.
    pub tables_done: usize,
    /// Tables this run covers in total.
    pub total_tables: usize,
    /// Rows fetched across every table so far.
    pub rows_fetched: u64,
}

/// What one whole run produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// Per-table results, in the order the catalog listed them.
    pub tables: Vec<TableSyncStats>,
    /// Rows fetched from the source across every table.
    pub total_rows_fetched: u64,
}

/// Syncs every table in `catalog`, one at a time, over a single connection.
///
/// Tables are synced sequentially and independently: each one's merge and checkpoint
/// advance complete before the next begins, so an interruption or failure partway
/// through leaves every already-synced table correctly checkpointed and the rest simply
/// untouched, ready to be picked up by the next run. There is deliberately no
/// cross-table transaction; see `CLAUDE.md`'s threat model for why that consistency
/// boundary is drawn at one table.
///
/// `on_progress` is called after each table finishes and returning `false` from it stops
/// the run with [`Error::Interrupted`], which is how a caller implements cancellation
/// (the Python binding uses it for Ctrl-C).
///
/// # Errors
///
/// Any error [`sync_table`] can return, plus [`Error::Interrupted`] if `on_progress`
/// asked the run to stop. The first failing table ends the run; tables already synced
/// keep their advanced checkpoints.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks the calling thread, driving its own private Tokio runtime, so it **must not**
/// be called from inside an existing runtime. Mirrors pgdelta's `pipeline::run`.
pub fn run(
    config: &SyncConfig,
    catalog: &SyncCatalog,
    mut on_progress: impl FnMut(Progress) -> bool,
) -> Result<SyncReport> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Io {
            message: e.to_string(),
        })?;

    runtime.block_on(async {
        let mut client = connect::open(&config.connect).await?;
        let total_tables = catalog.tables().len();
        let mut report = SyncReport {
            tables: Vec::with_capacity(total_tables),
            total_rows_fetched: 0,
        };

        for table_sync in catalog.tables() {
            let stats = sync_one(&mut client, config, table_sync).await?;
            report.total_rows_fetched += stats.rows_fetched;
            report.tables.push(stats);

            let keep_going = on_progress(Progress {
                table: table_sync.table.clone(),
                tables_done: report.tables.len(),
                total_tables,
                rows_fetched: report.total_rows_fetched,
            });
            if !keep_going {
                return Err(Error::Interrupted);
            }
        }
        Ok(report)
    })
}

/// One column, as a pre-flight check reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPreflight {
    /// Column name, as the source catalog spells it.
    pub name: String,
    /// The source type name, as the source catalog spells it.
    pub source_type: String,
    /// The Arrow type this column would be written as, rendered for display.
    pub arrow_type: String,
    /// False when the source type has no native mapping and the column would be written
    /// as text. Not a failure; see `crate::types`'s fidelity policy.
    pub recognised: bool,
}

/// What a pre-flight check found for one configured table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePreflight {
    /// Qualified table name, as configured.
    pub table: String,
    /// Every column the sync would read, in the order `SELECT *` produces them.
    pub columns: Vec<ColumnPreflight>,
    /// False if the configured watermark column is not a column of this table, which
    /// would fail the sync.
    pub watermark_present: bool,
    /// Configured primary key columns that are not columns of this table. Non-empty
    /// means the merge would fail.
    pub missing_primary_key_columns: Vec<String>,
    /// The value this table has been synced up to, if it has ever been synced.
    pub last_synced_value: Option<String>,
}

impl TablePreflight {
    /// True if this table is ready to sync: the watermark column exists and every
    /// primary key column exists.
    ///
    /// An unrecognised column type does **not** make a table unready: it degrades to text
    /// by design rather than failing (see `crate::types`), so it is reported through
    /// [`ColumnPreflight::recognised`] for a human to judge, not treated as a blocker.
    pub fn is_ready(&self) -> bool {
        self.watermark_present && self.missing_primary_key_columns.is_empty()
    }
}

/// Checks every table in `catalog` against the live source without writing anything.
///
/// Connects, reads each table's columns from the catalog, and reports what the sync
/// would do: which Arrow type each column would become, which columns would fall back to
/// text, whether the configured watermark and primary key columns actually exist, and
/// where each table's checkpoint currently stands. The counterpart to pgdelta's
/// `validate_dump`, and the cheap thing to run first when pointing this at an unfamiliar
/// schema for the first time.
///
/// Writes nothing: no Delta table is created and no checkpoint is advanced.
///
/// # Errors
///
/// [`Error::Connect`] or [`Error::Query`] if the source cannot be reached or a catalog
/// lookup fails, [`Error::UnsafeTableName`] for a table name unsafe to interpolate, and
/// [`Error::Checkpoint`] if the checkpoint table exists but cannot be read.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks the calling thread, driving its own private Tokio runtime, so it must not be
/// called from inside an existing runtime.
pub fn preflight(config: &SyncConfig, catalog: &SyncCatalog) -> Result<Vec<TablePreflight>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Io {
            message: e.to_string(),
        })?;

    runtime.block_on(async {
        let checkpoints = checkpoint::read_all(&config.checkpoint_uri).await?;
        let mut client = connect::open(&config.connect).await?;
        let mut out = Vec::with_capacity(catalog.tables().len());

        for table_sync in catalog.tables() {
            validate_identifier(&table_sync.table)?;
            let columns = describe_table(&mut client, config, &table_sync.table).await?;
            let has = |wanted: &str| {
                columns
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case(wanted))
            };

            out.push(TablePreflight {
                table: table_sync.table.clone(),
                watermark_present: has(&table_sync.watermark_column),
                missing_primary_key_columns: table_sync
                    .primary_key
                    .iter()
                    .filter(|key| !has(key))
                    .cloned()
                    .collect(),
                last_synced_value: checkpoints.get(&table_sync.table).cloned(),
                columns: columns
                    .iter()
                    .map(|(name, rt)| ColumnPreflight {
                        name: name.clone(),
                        source_type: rt.source.clone(),
                        arrow_type: builders::arrow_type(rt).to_string(),
                        recognised: rt.recognised,
                    })
                    .collect(),
            });
        }
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_preflight_is_ready_only_when_its_configured_columns_exist() {
        let mut p = TablePreflight {
            table: "dbo.customers".to_string(),
            columns: Vec::new(),
            watermark_present: true,
            missing_primary_key_columns: Vec::new(),
            last_synced_value: None,
        };
        assert!(p.is_ready());

        p.watermark_present = false;
        assert!(!p.is_ready());

        p.watermark_present = true;
        p.missing_primary_key_columns = vec!["id".to_string()];
        assert!(!p.is_ready());
    }

    /// An unmapped column type degrades to text by design, so it must not make a table
    /// look unready; it is reported, not treated as a blocker.
    #[test]
    fn an_unrecognised_column_type_does_not_make_a_table_unready() {
        let p = TablePreflight {
            table: "dbo.shapes".to_string(),
            columns: vec![ColumnPreflight {
                name: "area".to_string(),
                source_type: "geography".to_string(),
                arrow_type: "Utf8".to_string(),
                recognised: false,
            }],
            watermark_present: true,
            missing_primary_key_columns: Vec::new(),
            last_synced_value: None,
        };
        assert!(p.is_ready());
    }

    #[test]
    fn identifiers_with_sql_metacharacters_are_rejected() {
        for bad in ["users; DROP TABLE x", "users'", "users\"", "us\\ers", ""] {
            assert!(
                validate_identifier(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(validate_identifier("dbo.customers").is_ok());
    }

    #[test]
    fn relative_path_maps_schema_qualified_names() {
        assert_eq!(relative_path("dbo.customers").unwrap(), "dbo/customers");
        assert_eq!(relative_path("customers").unwrap(), "customers");
    }

    #[test]
    fn relative_path_rejects_traversal_segments() {
        assert!(relative_path("dbo..customers").is_err());
        assert!(relative_path("..").is_err());
        assert!(relative_path(".").is_err());
    }

    #[test]
    fn qualified_names_split_into_schema_and_table() {
        assert_eq!(split_qualified("dbo.customers"), (Some("dbo"), "customers"));
        assert_eq!(split_qualified("customers"), (None, "customers"));
    }
}
