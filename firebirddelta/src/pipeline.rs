//! Orchestration: the per-table incremental sync loop.
//!
//! Ties `crate::connect` (open a connection), `crate::types` and `crate::builders`
//! (resolve columns, decode fetched rows into Arrow), `crate::merge` (upsert into Delta),
//! and `crate::checkpoint` (record how far this table has been synced) together into one
//! call per table. See `crate::merge`'s module docs for why a checkpoint is only ever
//! advanced after its table's merge has already committed.
//!
//! # Why this module is a hybrid of blocking and async, unlike tiberiusdelta's
//!
//! `rsfbclient` is synchronous (see `crate::connect`'s module docs), but `crate::merge`
//! and `crate::checkpoint` are async-only (delta-rs). This module bridges the two per
//! table: `describe_table` and the row-fetching half of `sync_one` run on Tokio's
//! blocking-thread pool via `tokio::task::spawn_blocking`, moving the open
//! [`crate::connect::FbConnection`] in and back out of each blocking call so the same
//! login is reused across a whole table (and, in [`run`], a whole catalog), while the
//! Delta merge and checkpoint calls in between stay on the async side. Fetched rows
//! cross that boundary through a bounded `tokio::sync::mpsc` channel, one already-built
//! `RecordBatch` at a time, which is what keeps memory bounded (`CLAUDE.md`'s Security
//! requirement 5) without needing to hold a `rsfbclient` cursor open *across* separate
//! blocking calls, which its borrow-based iterator API does not allow without unsafe
//! code (forbidden here, see `CLAUDE.md`'s Security requirement 1): one blocking call
//! owns the whole cursor for as long as it is open, and streams completed batches out
//! through the channel as it goes.

use std::sync::Arc;
use std::time::Duration;

use rsfbclient::{Queryable, Row as FbRow};

use crate::catalog::{SyncCatalog, TableSync};
use crate::connect::{self, ConnectConfig, FbConnection};
use crate::error::{Error, Result};
use crate::types::{self, ResolvedType};
use crate::{builders, checkpoint, merge};

/// Settings shared by every table one call to [`sync_table`] covers.
#[derive(Clone)]
pub struct SyncConfig {
    /// How to reach the source database.
    pub connect: ConnectConfig,
    /// Prefix each table's Delta table is written beneath, for example
    /// `/Volumes/main/raw/firebird/`. Mirrors tiberiusdelta's `SyncConfig::output_uri`.
    pub output_uri: String,
    /// Where the checkpoint table lives, for example
    /// `<output_uri>/_firebirddelta_checkpoints`. Kept as its own field, rather than
    /// always derived from `output_uri`, so a caller can point several sync configs at
    /// independently checkpointed prefixes if that is ever wanted.
    pub checkpoint_uri: String,
    /// Rows accumulated per merge into Delta. Bounds memory: this is Security
    /// requirement 5 in `CLAUDE.md`, not a performance knob to maximise blindly.
    pub fetch_batch_size: usize,
    /// Seconds a table's sync may go without a completed batch reaching the merge side
    /// before the sync gives up waiting for it.
    ///
    /// Coarser than tiberiusdelta's per-row deadline: `rsfbclient`'s synchronous
    /// iterator gives this crate no point to check a deadline *between* rows the way
    /// polling an async stream does, so this bounds time per **batch** of up to
    /// `fetch_batch_size` rows, not per row. It also does not cancel the underlying
    /// blocking fetch: see this module's own caveat about it, the same one
    /// `crate::connect::ConnectConfig::login_timeout_sec` documents for the login.
    /// `None` waits indefinitely.
    pub query_timeout_sec: Option<u64>,
}

impl SyncConfig {
    /// Rejects settings that cannot produce a working run.
    ///
    /// Checked before a connection is opened, so a misconfiguration costs nothing and
    /// fails where the cause is obvious. Each of these would otherwise fail later and
    /// less clearly: an empty `output_uri` writes tables to a relative path nobody meant,
    /// and a `fetch_batch_size` of zero merges once per row, turning a bounded-memory
    /// design into one Delta commit per row.
    ///
    /// # Errors
    ///
    /// [`Error::Internal`] naming the setting at fault.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn validate(&self) -> Result<()> {
        if self.output_uri.trim().is_empty() {
            return Err(Error::Internal {
                detail: "output_uri is empty; there is nowhere to write tables",
            });
        }
        if self.checkpoint_uri.trim().is_empty() {
            return Err(Error::Internal {
                detail: "checkpoint_uri is empty; there is nowhere to record progress",
            });
        }
        if self.fetch_batch_size == 0 {
            return Err(Error::Internal {
                detail: "fetch_batch_size is zero; it must be at least one row",
            });
        }
        Ok(())
    }
}

/// What one table's sync produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSyncStats {
    /// Table name.
    pub table: String,
    /// Rows fetched from the source in this run (new or changed since the last
    /// checkpoint, or every row on a first sync).
    pub rows_fetched: u64,
    /// Rows the merge inserted (keys not previously present).
    pub rows_inserted: usize,
    /// Rows the merge updated (keys already present).
    pub rows_updated: usize,
    /// Columns whose source type this crate does not map, and which were therefore
    /// written as text. Reported rather than logged, matching tiberiusdelta's own
    /// `text_fallback_columns`: a wide production schema will have some, and that is a
    /// fact for the caller to see, not a failure.
    pub text_fallback_columns: Vec<String>,
    /// Columns excluded from the sync entirely, because this crate's Firebird client
    /// cannot read them even as text (see `crate::types`'s module docs and
    /// [`Error::ExcludedColumnType`]). Distinct from `text_fallback_columns`: those
    /// columns are still synced, just degraded to a string; these are not synced at
    /// all.
    pub excluded_columns: Vec<String>,
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
    if name.is_empty()
        || name.contains(['\'', '"', ';', '\\', '\0', '[', ']'])
        || name.chars().any(char::is_whitespace)
    {
        return Err(Error::UnsafeTableName {
            name: name.to_string(),
        });
    }
    Ok(())
}

/// Maps a configured table name to its relative Delta output path.
///
/// Unlike tiberiusdelta's `relative_path`, there is no database/schema qualification to
/// split apart: Firebird has a single flat table namespace per database file (no schema
/// concept at all), so a configured [`TableSync::table`] and its output path are the
/// same string, once validated.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] if `table` is unsafe to interpolate or to use as a path
/// component.
fn relative_path(table: &str) -> Result<String> {
    validate_identifier(table)?;
    if table == "." || table == ".." {
        return Err(Error::UnsafeTableName {
            name: table.to_string(),
        });
    }
    Ok(table.to_string())
}

/// Runs `f` on Tokio's blocking-thread pool, bounding how long the caller waits for it.
///
/// Shared by [`describe_table`] and [`source_watermark`] for their one-shot catalog and
/// aggregate queries; the row-streaming half of [`sync_one`] has a different shape
/// (many chunks, not one return value) and implements the same caveat itself.
///
/// **Does not cancel `f`.** `rsfbclient`'s synchronous calls have no cancellation point
/// this crate can reach into, the same limitation `crate::connect::open`'s own timeout
/// documents for the login: on a timeout, the spawned blocking task is only abandoned,
/// not stopped, and keeps running (and keeps holding its `FbConnection`) until it
/// finishes or the process exits. `what` names the operation in the timeout message.
///
/// # Errors
///
/// [`Error::Query`] if `timeout_sec` elapses first, and [`Error::Io`] if the blocking
/// task panicked.
///
/// # Panics
///
/// Does not panic.
async fn blocking_with_timeout<T>(
    timeout_sec: Option<u64>,
    what: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    let handle = tokio::task::spawn_blocking(f);
    match timeout_sec {
        Some(secs) => match tokio::time::timeout(Duration::from_secs(secs), handle).await {
            Ok(joined) => joined.map_err(|e| Error::Io {
                message: format!("{what} task did not complete: {e}"),
            }),
            Err(_) => Err(Error::Query {
                message: format!(
                    "{what} did not complete within {secs}s (the underlying attempt may \
                     still be running; see ConnectConfig::login_timeout_sec's caveat)"
                ),
            }),
        },
        None => handle.await.map_err(|e| Error::Io {
            message: format!("{what} task did not complete: {e}"),
        }),
    }
}

/// Reads a table's column names and resolved types from
/// `RDB$RELATION_FIELDS`/`RDB$FIELDS`.
///
/// Columns come back in `RDB$FIELD_POSITION` order, the order a `SELECT *` would produce
/// them (though `crate::pipeline` never actually issues `SELECT *`; see [`sync_one`]).
/// See `crate::types`'s module docs for why this catalog, and not `rsfbclient`'s own
/// coarse `SqlType`, is what the Arrow schema is built from.
///
/// `RDB$FIELD_NAME` is a fixed-width `CHAR` column in Firebird's own catalog, so a short
/// name comes back padded with trailing spaces; every name is trimmed before use.
///
/// Runs `describe_table_blocking` on a blocking-pool thread and returns `conn` alongside
/// the result so the caller can reuse the same login for the next step.
///
/// # Errors
///
/// [`Error::Query`] if the lookup fails or returns a row this crate cannot read, and
/// [`Error::TableNotFound`] if it returns no rows at all.
async fn describe_table(
    conn: FbConnection,
    config: &SyncConfig,
    table: &str,
) -> Result<(FbConnection, Vec<(String, ResolvedType)>)> {
    let table_owned = table.to_string();
    let connection_string = config.connect.connection_string.clone();
    let (conn, result) =
        blocking_with_timeout(config.query_timeout_sec, "the catalog lookup", move || {
            let mut conn = conn;
            let result = describe_table_blocking(&mut conn, &table_owned, &connection_string);
            (conn, result)
        })
        .await?;
    Ok((conn, result?))
}

/// The synchronous body of [`describe_table`].
fn describe_table_blocking(
    conn: &mut FbConnection,
    table: &str,
    connection_string: &str,
) -> Result<Vec<(String, ResolvedType)>> {
    let sql = "SELECT rf.RDB$FIELD_NAME, f.RDB$FIELD_TYPE, f.RDB$FIELD_SUB_TYPE \
               FROM RDB$RELATION_FIELDS rf \
               JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME \
               WHERE rf.RDB$RELATION_NAME = ? \
               ORDER BY rf.RDB$FIELD_POSITION";

    let rows: Vec<(String, i32, Option<i32>)> = conn
        .query(sql, (table,))
        .map_err(|e| connect::query_error(&e, connection_string))?;

    if rows.is_empty() {
        return Err(Error::TableNotFound {
            table: table.to_string(),
        });
    }

    Ok(rows
        .into_iter()
        .map(|(name, field_type, field_sub_type)| {
            let name = name.trim_end().to_string();
            let resolved = types::resolve(&name, field_type, field_sub_type);
            (name, resolved)
        })
        .collect())
}

/// One decoded batch, on its way from the blocking fetch side to the async merge side.
struct FetchedChunk {
    batch: deltalake::arrow::array::RecordBatch,
    /// The watermark column's rendered value from this chunk's *last* row, if any row
    /// carried one. Since the query orders by the watermark ascending, the last chunk's
    /// value (once every chunk has been merged) is the greatest one seen.
    newest_watermark: Option<String>,
}

/// The channel [`fetch_and_send`] streams [`FetchedChunk`]s out through.
type FetchedChunkSender = tokio::sync::mpsc::Sender<Result<FetchedChunk>>;

/// Everything [`fetch_and_send`] needs besides the connection and the channel, bundled
/// so the function itself stays under clippy's argument-count lint without resorting to
/// an `#[allow]`.
struct FetchRequest<'a> {
    query: &'a str,
    last_value: Option<&'a str>,
    fetch_batch_size: usize,
    columns: &'a [(String, ResolvedType)],
    watermark_index: usize,
    connection_string: &'a str,
}

/// The synchronous body run on a blocking-pool thread for the whole of one table's row
/// fetch: opens the cursor, and streams completed `RecordBatch`es out through `tx` as
/// they fill up, rather than collecting the whole table into memory first.
///
/// One `rsfbclient` cursor's lifetime is confined entirely to this function, which is
/// why it exists as its own blocking call rather than one call per chunk: `rsfbclient`'s
/// `query_iter` borrows `conn` for as long as it is open, so the same cursor cannot be
/// closed and reopened across separate `spawn_blocking` calls without either losing its
/// position or (for a `WHERE`-filtered, ordered query) re-scanning from the start each
/// time. See the module docs for the fuller design rationale.
///
/// A send failure (the receiver dropped, which happens when the async side has already
/// given up, for example on its own timeout) stops the fetch early rather than treating
/// it as an error: there is no one left to hand batches to.
fn fetch_and_send(conn: &mut FbConnection, request: &FetchRequest<'_>, tx: &FetchedChunkSender) {
    let iter = match request.last_value {
        Some(v) => conn.query_iter::<_, FbRow>(request.query, (v,)),
        None => conn.query_iter::<_, FbRow>(request.query, ()),
    };
    let mut iter = match iter {
        Ok(iter) => iter,
        Err(e) => {
            let _ = tx.blocking_send(Err(connect::query_error(&e, request.connection_string)));
            return;
        }
    };

    let mut batch: Vec<FbRow> = Vec::with_capacity(request.fetch_batch_size);
    loop {
        let next = iter.next();
        let end_of_stream = next.is_none();
        match next {
            Some(Ok(row)) => {
                batch.push(row);
                if batch.len() < request.fetch_batch_size {
                    continue;
                }
            }
            Some(Err(e)) => {
                let _ = tx.blocking_send(Err(connect::query_error(&e, request.connection_string)));
                return;
            }
            None => {}
        }
        if batch.is_empty() {
            break;
        }

        let newest_watermark = batch
            .last()
            .and_then(|row| row.cols.get(request.watermark_index))
            .and_then(|col| builders::render_text(&col.value));

        let record_batch = match builders::record_batch(request.columns, &batch) {
            Ok(rb) => rb,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
        };
        batch.clear();

        if tx
            .blocking_send(Ok(FetchedChunk {
                batch: record_batch,
                newest_watermark,
            }))
            .is_err()
        {
            return;
        }
        if end_of_stream {
            break;
        }
    }
}

/// Syncs one table: fetches rows new or changed since its last checkpoint (or every row,
/// on a first sync), upserts them into its Delta table, and advances its checkpoint only
/// once that upsert has committed.
///
/// The query is always `ORDER BY <watermark_column> ASC`, so the watermark value of the
/// last row fetched is always the greatest one seen, regardless of whether the column is
/// numeric, textual, or temporal: this crate compares nothing itself and trusts the
/// source database's own ordering, the same reasoning tiberiusdelta's own `sync_one`
/// gives.
///
/// # Errors
///
/// [`Error::IncrementalConfigMissing`] if `table_sync` is not fully configured;
/// [`Error::UnsafeTableName`] if its table name or watermark column is unsafe to
/// interpolate into SQL; [`Error::ColumnNotFound`] if the watermark or a primary key
/// column does not exist at all; [`Error::ExcludedColumnType`] if one of them exists but
/// has a type this crate's Firebird client cannot read even as text (see
/// `crate::types`); [`Error::Connect`] or [`Error::Query`] for a connection or query
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
/// Async; must run on a Tokio runtime with blocking threads available. Internally
/// dispatches every `rsfbclient` call onto that blocking pool; see the module docs.
pub async fn sync_table(config: &SyncConfig, table_sync: &TableSync) -> Result<TableSyncStats> {
    let last_value = checkpoint::read(&checkpoint_uri_for(config, &table_sync.table)?).await?;
    let conn = connect::open(&config.connect).await?;
    let (_conn, stats) = sync_one(conn, config, table_sync, last_value).await?;
    Ok(stats)
}

/// Where one configured table's checkpoint lives.
fn checkpoint_uri_for(config: &SyncConfig, table: &str) -> Result<String> {
    Ok(checkpoint::uri_for(
        &config.checkpoint_uri,
        &relative_path(table)?,
    ))
}

/// Looks up a configured column among a table's described columns, distinguishing "does
/// not exist" from "exists but this crate's Firebird client cannot read it at all".
///
/// Shared by [`sync_one`]'s watermark and primary key checks: both need exactly this
/// three-way answer, and treating "excluded" the same as "not found" would blame the
/// caller's configuration for what is actually a driver limitation (see
/// `crate::types`'s module docs).
///
/// # Errors
///
/// [`Error::ColumnNotFound`] if no column named `wanted` exists at all, and
/// [`Error::ExcludedColumnType`] if one does but cannot be selected.
fn require_selectable_column<'a>(
    columns: &'a [(String, ResolvedType)],
    wanted: &str,
    table: &str,
    role: &'static str,
) -> Result<&'a (String, ResolvedType)> {
    let found = columns
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(wanted));
    match found {
        Some((name, rt)) if rt.excluded() => Err(Error::ExcludedColumnType {
            table: table.to_string(),
            column: name.clone(),
            sql_type: rt.source.clone(),
        }),
        Some(found) => Ok(found),
        None => Err(Error::ColumnNotFound {
            table: table.to_string(),
            column: wanted.to_string(),
            role,
        }),
    }
}

/// The body of [`sync_table`], against an already-open connection, returning it back to
/// the caller so [`run`] can reuse one login across a whole catalog instead of paying a
/// Firebird login for every table.
async fn sync_one(
    conn: FbConnection,
    config: &SyncConfig,
    table_sync: &TableSync,
    last_value: Option<String>,
) -> Result<(FbConnection, TableSyncStats)> {
    table_sync.validate()?;
    validate_identifier(&table_sync.table)?;
    validate_identifier(&table_sync.watermark_column)?;
    for key in &table_sync.primary_key {
        validate_identifier(key)?;
    }

    let (conn, columns) = describe_table(conn, config, &table_sync.table).await?;

    require_selectable_column(
        &columns,
        &table_sync.watermark_column,
        &table_sync.table,
        "watermark column",
    )?;
    for key in &table_sync.primary_key {
        require_selectable_column(&columns, key, &table_sync.table, "primary key column")?;
    }

    let selected: Vec<(String, ResolvedType)> = columns
        .iter()
        .filter(|(_, rt)| !rt.excluded())
        .cloned()
        .collect();
    let excluded_columns: Vec<String> = columns
        .iter()
        .filter(|(_, rt)| rt.excluded())
        .map(|(name, _)| name.clone())
        .collect();
    let text_fallback_columns: Vec<String> = selected
        .iter()
        .filter(|(_, rt)| !rt.recognised)
        .map(|(name, _)| name.clone())
        .collect();

    // Already checked selectable above; recomputed against `selected`'s own order
    // because that is the order the generated SELECT (and so each fetched row's cells)
    // will actually be in.
    let watermark_index = selected
        .iter()
        .position(|(name, _)| name.eq_ignore_ascii_case(&table_sync.watermark_column))
        .ok_or(Error::Internal {
            detail: "watermark column passed its selectability check but is missing from \
                     the selected column list",
        })?;

    let table_uri = format!(
        "{}/{}",
        config.output_uri.trim_end_matches('/'),
        relative_path(&table_sync.table)?
    );
    let arrow_schema = Arc::new(builders::arrow_schema(&selected));
    let mut delta_table = merge::open_or_create(&table_uri, &arrow_schema).await?;

    let projection = selected
        .iter()
        .map(|(name, rt)| match &rt.cast_as {
            Some(cast) => format!("{cast} AS {name}"),
            None => name.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {projection} FROM {} {} ORDER BY {} ASC",
        table_sync.table,
        if last_value.is_some() {
            format!("WHERE {} > ?", table_sync.watermark_column)
        } else {
            String::new()
        },
        table_sync.watermark_column,
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<FetchedChunk>>(2);
    let connection_string = config.connect.connection_string.clone();
    let fetch_batch_size = config.fetch_batch_size;
    let mut conn = conn;
    let handle = tokio::task::spawn_blocking(move || {
        let request = FetchRequest {
            query: &query,
            last_value: last_value.as_deref(),
            fetch_batch_size,
            columns: &selected,
            watermark_index,
            connection_string: &connection_string,
        };
        fetch_and_send(&mut conn, &request, &tx);
        drop(tx);
        conn
    });

    let mut stats = TableSyncStats {
        table: table_sync.table.clone(),
        rows_fetched: 0,
        rows_inserted: 0,
        rows_updated: 0,
        text_fallback_columns,
        excluded_columns,
    };
    let mut newest_watermark: Option<String> = None;

    loop {
        let received = match config.query_timeout_sec {
            Some(secs) => match tokio::time::timeout(Duration::from_secs(secs), rx.recv()).await {
                Ok(r) => r,
                Err(_) => {
                    // The blocking fetch may still be running with no way for this
                    // crate to interrupt it (see the module docs); detach it rather
                    // than waiting on a task that may never finish.
                    drop(handle);
                    return Err(Error::Query {
                        message: format!(
                            "the source did not complete within {secs}s (the underlying \
                             fetch may still be running)"
                        ),
                    });
                }
            },
            None => rx.recv().await,
        };

        match received {
            Some(Ok(chunk)) => {
                stats.rows_fetched += chunk.batch.num_rows() as u64;
                if chunk.newest_watermark.is_some() {
                    newest_watermark = chunk.newest_watermark;
                }
                let (updated_table, metrics) =
                    merge::upsert(delta_table, chunk.batch, &table_sync.primary_key).await?;
                delta_table = updated_table;
                stats.rows_inserted += metrics.num_target_rows_inserted;
                stats.rows_updated += metrics.num_target_rows_updated;
            }
            Some(Err(e)) => {
                // The blocking side has already sent its last message and is on its
                // way to returning; safe to join it before propagating.
                let _ = handle.await;
                return Err(e);
            }
            None => break,
        }
    }

    let conn = handle.await.map_err(|e| Error::Io {
        message: format!("the fetch task did not complete: {e}"),
    })?;

    if let Some(new_value) = newest_watermark {
        let synced_at_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        checkpoint::advance(
            &checkpoint_uri_for(config, &table_sync.table)?,
            &table_sync.table,
            &table_sync.watermark_column,
            &new_value,
            synced_at_micros,
        )
        .await?;
    }

    Ok((conn, stats))
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
/// be called from inside an existing runtime. Mirrors tiberiusdelta's and pgdelta's own
/// `pipeline::run`.
pub fn run(
    config: &SyncConfig,
    catalog: &SyncCatalog,
    mut on_progress: impl FnMut(Progress) -> bool,
) -> Result<SyncReport> {
    config.validate()?;
    // Nothing to do, and nothing worth opening a connection for.
    if catalog.tables().is_empty() {
        return Ok(SyncReport {
            tables: Vec::new(),
            total_rows_fetched: 0,
        });
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Io {
            message: e.to_string(),
        })?;

    runtime.block_on(async {
        let mut conn = connect::open(&config.connect).await?;
        let total_tables = catalog.tables().len();
        let mut report = SyncReport {
            tables: Vec::with_capacity(total_tables),
            total_rows_fetched: 0,
        };

        for table_sync in catalog.tables() {
            let last_value = checkpoint::read(&checkpoint_uri_for(config, &table_sync.table)?)
                .await
                .map_err(|e| e.in_table(&table_sync.table))?;
            // A run covers many tables, so every failure says which one it was.
            let (next_conn, stats) = sync_one(conn, config, table_sync, last_value)
                .await
                .map_err(|e| e.in_table(&table_sync.table))?;
            conn = next_conn;
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
    /// The Arrow type this column would be written as, rendered for display. `None` for
    /// a column that would be excluded from the sync entirely; see
    /// [`ColumnPreflight::excluded`].
    pub arrow_type: Option<String>,
    /// False when the source type has no native mapping and the column would be written
    /// as text, or is excluded outright. Not necessarily a failure; see
    /// `crate::types`'s fidelity policy.
    pub recognised: bool,
    /// True if this column cannot be synced at all (see [`crate::Error::ExcludedColumnType`]).
    pub excluded: bool,
}

/// What a pre-flight check found for one configured table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePreflight {
    /// Table name, as configured.
    pub table: String,
    /// Every column the catalog reports for this table, in `RDB$FIELD_POSITION` order,
    /// including excluded ones (see [`ColumnPreflight::excluded`]).
    pub columns: Vec<ColumnPreflight>,
    /// False if the configured watermark column is not a column of this table, or
    /// exists but is excluded, either of which would fail the sync.
    pub watermark_present: bool,
    /// Configured primary key columns that are missing or excluded. Non-empty means the
    /// sync would fail.
    pub missing_primary_key_columns: Vec<String>,
    /// The value this table has been synced up to, if it has ever been synced.
    pub last_synced_value: Option<String>,
}

impl TablePreflight {
    /// True if this table is ready to sync: the watermark column is present and
    /// selectable, and every primary key column is too.
    ///
    /// An unrecognised-but-selectable column type does **not** make a table unready: it
    /// degrades to text by design rather than failing (see `crate::types`), so it is
    /// reported through [`ColumnPreflight::recognised`] for a human to judge, not
    /// treated as a blocker. An *excluded* column that happens to be the watermark or a
    /// primary key column does block, since the sync cannot select it at all.
    pub fn is_ready(&self) -> bool {
        self.watermark_present && self.missing_primary_key_columns.is_empty()
    }
}

/// Checks every table in `catalog` against the live source without writing anything.
///
/// Connects, reads each table's columns from the catalog, and reports what the sync
/// would do: which Arrow type each column would become, which columns would fall back to
/// text or be excluded outright, whether the configured watermark and primary key
/// columns actually exist and are selectable, and where each table's checkpoint
/// currently stands. The counterpart to tiberiusdelta's and pgdelta's own pre-flight
/// checks, and the cheap thing to run first when pointing this at an unfamiliar schema
/// for the first time.
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

    if catalog.tables().is_empty() {
        return Ok(Vec::new());
    }

    runtime.block_on(async {
        let mut conn = connect::open(&config.connect).await?;
        let mut out = Vec::with_capacity(catalog.tables().len());

        for table_sync in catalog.tables() {
            let (next_conn, columns) = describe_table(conn, config, &table_sync.table)
                .await
                .map_err(|e| e.in_table(&table_sync.table))?;
            conn = next_conn;

            let selectable = |wanted: &str| {
                columns
                    .iter()
                    .any(|(name, rt)| name.eq_ignore_ascii_case(wanted) && !rt.excluded())
            };

            out.push(TablePreflight {
                table: table_sync.table.clone(),
                watermark_present: selectable(&table_sync.watermark_column),
                missing_primary_key_columns: table_sync
                    .primary_key
                    .iter()
                    .filter(|key| !selectable(key))
                    .cloned()
                    .collect(),
                // An empty checkpoint_uri means the caller did not say where checkpoints
                // live and does not want them reported. Deriving one from an empty
                // output_uri would probe an absolute path at the filesystem root, which
                // only looks harmless because a failed load reads back as empty.
                last_synced_value: if config.checkpoint_uri.is_empty() {
                    None
                } else {
                    checkpoint::read(&checkpoint_uri_for(config, &table_sync.table)?)
                        .await
                        .map_err(|e| e.in_table(&table_sync.table))?
                },
                columns: columns
                    .iter()
                    .map(|(name, rt)| ColumnPreflight {
                        name: name.clone(),
                        source_type: rt.source.clone(),
                        arrow_type: if rt.excluded() {
                            None
                        } else {
                            Some(builders::arrow_type(rt).to_string())
                        },
                        recognised: rt.recognised,
                        excluded: rt.excluded(),
                    })
                    .collect(),
            });
        }
        Ok(out)
    })
}

/// Reads a table's current greatest watermark value from the source, without syncing.
///
/// The first half of a bulk backfill; see tiberiusdelta's and pgdelta's own
/// `source_watermark` for the full handover procedure this mirrors exactly, including
/// the "capture before the export starts" caution.
///
/// Rendered exactly as [`sync_table`] would record it, through the same
/// `crate::builders::render_text`, so the value is comparable with one this crate wrote
/// itself.
///
/// Returns `None` if the table is empty, in which case there is no watermark to record
/// and an ordinary first sync is the right thing.
///
/// # Errors
///
/// [`Error::Connect`] or [`Error::Query`] for a connection or query failure,
/// [`Error::UnsafeTableName`] for a name unsafe to interpolate, and
/// [`Error::ColumnNotFound`]/[`Error::ExcludedColumnType`] if the configured watermark
/// column does not exist or cannot be selected.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks the calling thread, driving its own private Tokio runtime, so it must not be
/// called from inside an existing runtime.
pub fn source_watermark(config: &SyncConfig, table_sync: &TableSync) -> Result<Option<String>> {
    table_sync.validate()?;
    validate_identifier(&table_sync.watermark_column)?;
    validate_identifier(&table_sync.table)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Io {
            message: e.to_string(),
        })?;

    runtime.block_on(async {
        let conn = connect::open(&config.connect).await?;
        let (conn, columns) = describe_table(conn, config, &table_sync.table)
            .await
            .map_err(|e| e.in_table(&table_sync.table))?;
        require_selectable_column(
            &columns,
            &table_sync.watermark_column,
            &table_sync.table,
            "watermark column",
        )?;

        let sql = format!(
            "SELECT MAX({}) FROM {}",
            table_sync.watermark_column, table_sync.table
        );
        let connection_string = config.connect.connection_string.clone();
        let query_timeout_sec = config.query_timeout_sec;
        blocking_with_timeout(query_timeout_sec, "the source", move || {
            let mut conn = conn;
            let row: Result<Option<FbRow>> = conn
                .query_first(&sql, ())
                .map_err(|e| connect::query_error(&e, &connection_string));
            row.map(|row| {
                row.and_then(|row| {
                    row.cols
                        .first()
                        .and_then(|c| builders::render_text(&c.value))
                })
            })
        })
        .await?
    })
}

/// Records that `table_sync` has been synced up to `last_value`, without syncing anything.
///
/// The second half of a bulk backfill: once the data is in the table's Delta table by
/// whatever means, this hands over to incremental sync, which then fetches only what has
/// changed since `last_value` rather than re-pulling everything.
///
/// `last_value` should be what [`source_watermark`] returned **before** the export ran.
///
/// This writes a checkpoint for data it has not itself verified, which is the whole point
/// and also the risk: a value ahead of what was actually loaded silently skips the rows in
/// between, and nothing will report it. A value behind is safe, costing only a re-fetch.
/// When unsure, choose the earlier value.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] for a name unsafe to use in a path, and
/// [`Error::Checkpoint`] or [`Error::Delta`] if the checkpoint could not be written.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks the calling thread, driving its own private Tokio runtime, so it must not be
/// called from inside an existing runtime.
pub fn set_checkpoint(config: &SyncConfig, table_sync: &TableSync, last_value: &str) -> Result<()> {
    table_sync.validate()?;
    let at = checkpoint_uri_for(config, &table_sync.table)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Io {
            message: e.to_string(),
        })?;

    runtime.block_on(async {
        let synced_at_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        checkpoint::advance(
            &at,
            &table_sync.table,
            &table_sync.watermark_column,
            last_value,
            synced_at_micros,
        )
        .await
        .map_err(|e| e.in_table(&table_sync.table))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_preflight_is_ready_only_when_its_configured_columns_are_selectable() {
        let mut p = TablePreflight {
            table: "CUSTOMERS".to_string(),
            columns: Vec::new(),
            watermark_present: true,
            missing_primary_key_columns: Vec::new(),
            last_synced_value: None,
        };
        assert!(p.is_ready());

        p.watermark_present = false;
        assert!(!p.is_ready());

        p.watermark_present = true;
        p.missing_primary_key_columns = vec!["ID".to_string()];
        assert!(!p.is_ready());
    }

    /// An unmapped-but-selectable column type degrades to text by design, so it must not
    /// make a table look unready; it is reported, not treated as a blocker.
    #[test]
    fn an_unrecognised_column_type_does_not_make_a_table_unready() {
        let p = TablePreflight {
            table: "SHAPES".to_string(),
            columns: vec![ColumnPreflight {
                name: "AREA".to_string(),
                source_type: "RDB$FIELD_TYPE 999".to_string(),
                arrow_type: Some("Utf8".to_string()),
                recognised: false,
                excluded: false,
            }],
            watermark_present: true,
            missing_primary_key_columns: Vec::new(),
            last_synced_value: None,
        };
        assert!(p.is_ready());
    }

    fn config(output_uri: &str, checkpoint_uri: &str, batch: usize) -> SyncConfig {
        SyncConfig {
            connect: ConnectConfig {
                connection_string: "firebird://sysdba:masterkey@localhost/x.fdb".to_string(),
                login_timeout_sec: None,
            },
            output_uri: output_uri.to_string(),
            checkpoint_uri: checkpoint_uri.to_string(),
            fetch_batch_size: batch,
            query_timeout_sec: None,
        }
    }

    /// Each of these would otherwise fail later and less clearly, after a connection had
    /// already been opened against a production database.
    #[test]
    fn unworkable_settings_are_rejected_before_anything_connects() {
        assert!(
            config("file:///out", "file:///out/_c", 10)
                .validate()
                .is_ok()
        );
        assert!(config("", "file:///out/_c", 10).validate().is_err());
        assert!(config("   ", "file:///out/_c", 10).validate().is_err());
        assert!(config("file:///out", "", 10).validate().is_err());
        assert!(
            config("file:///out", "file:///out/_c", 0)
                .validate()
                .is_err()
        );
    }

    /// A run over no tables should not open a connection to do nothing with.
    #[test]
    fn an_empty_catalog_produces_an_empty_report_without_connecting() {
        let catalog = SyncCatalog::new(Vec::new()).unwrap();
        let report = run(
            &config("file:///out", "file:///out/_c", 10),
            &catalog,
            |_| true,
        )
        .unwrap();
        assert!(report.tables.is_empty());
        assert_eq!(report.total_rows_fetched, 0);

        let found = preflight(&config("file:///out", "file:///out/_c", 10), &catalog).unwrap();
        assert!(found.is_empty());
    }

    /// A run covers many tables, so a bare "delta error: ..." does not say which failed.
    #[test]
    fn errors_gain_the_table_they_happened_on() {
        let annotated = Error::Delta {
            message: "object store timed out".to_string(),
        }
        .in_table("INVOICES");
        assert_eq!(
            annotated.to_string(),
            "delta error: on table INVOICES: object store timed out"
        );
    }

    #[test]
    fn identifiers_with_sql_metacharacters_are_rejected() {
        for bad in ["users; DROP TABLE x", "users'", "users\"", "us\\ers", ""] {
            assert!(
                validate_identifier(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(validate_identifier("CUSTOMERS").is_ok());
    }

    #[test]
    fn relative_path_is_the_table_name_itself() {
        assert_eq!(relative_path("CUSTOMERS").unwrap(), "CUSTOMERS");
    }

    #[test]
    fn relative_path_rejects_traversal_segments() {
        assert!(relative_path("..").is_err());
        assert!(relative_path(".").is_err());
    }

    #[test]
    fn identifiers_with_whitespace_or_brackets_are_rejected() {
        assert!(validate_identifier("my table").is_err());
        assert!(validate_identifier("[customers]").is_err());
    }
}
