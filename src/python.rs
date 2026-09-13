//! Python bindings.
//!
//! A thin translation layer over [`crate::pipeline`]: it converts keyword arguments into
//! a [`SyncConfig`] and a [`SyncCatalog`], releases the GIL for the sync work, checks for
//! `KeyboardInterrupt` between tables, and returns the report as a Python object.
//!
//! Executes on the thread that called into it. The sync work runs with the GIL released;
//! the progress callback re-acquires it.

use pyo3::exceptions::{
    PyConnectionError, PyIOError, PyKeyboardInterrupt, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::catalog::{SyncCatalog, TableSync};
use crate::connect::ConnectConfig;
use crate::error::Error;
use crate::pipeline::{self, Progress, SyncConfig, TablePreflight, TableSyncStats};

/// Registers everything the extension module exposes.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(sync_tables, module)?)?;
    module.add_function(wrap_pyfunction!(preflight, module)?)?;
    module.add_class::<PySyncReport>()?;
    module.add_class::<PyTableSyncStats>()?;
    module.add_class::<PyTablePreflight>()?;
    module.add_class::<PyColumnPreflight>()?;
    module.add(
        "__all__",
        (
            "sync_tables",
            "preflight",
            "SyncReport",
            "TableSyncStats",
            "TablePreflight",
            "ColumnPreflight",
        ),
    )?;
    Ok(())
}

/// Maps a crate error onto the closest Python exception.
///
/// `ValueError` is the default because most variants describe configuration that is
/// wrong. Faults that are ours rather than the caller's raise `RuntimeError` instead, and
/// a failure to reach the database raises `ConnectionError` so a caller can retry that
/// case specifically without pattern-matching on a message.
///
/// Every message is already redacted by `crate::connect` before it reaches here, so no
/// connection string or password can escape into a traceback; see `CLAUDE.md`'s Security
/// requirement 3.
fn to_pyerr(err: Error) -> PyErr {
    let message = err.to_string();
    match err {
        Error::Interrupted => PyKeyboardInterrupt::new_err(message),
        Error::Io { .. } => PyIOError::new_err(message),
        Error::Connect { .. } => PyConnectionError::new_err(message),
        Error::Delta { .. }
        | Error::Arrow { .. }
        | Error::Checkpoint { .. }
        | Error::Internal { .. } => PyRuntimeError::new_err(message),
        _ => PyValueError::new_err(message),
    }
}

/// Per-table result, mirroring [`TableSyncStats`].
#[pyclass(name = "TableSyncStats", frozen, get_all)]
pub struct PyTableSyncStats {
    /// Qualified table name.
    table: String,
    /// Rows fetched from the source in this run.
    rows_fetched: u64,
    /// Rows the merge inserted.
    rows_inserted: usize,
    /// Rows the merge updated.
    rows_updated: usize,
    /// Columns written as text because their source type has no native mapping.
    text_fallback_columns: Vec<String>,
}

#[pymethods]
impl PyTableSyncStats {
    fn __repr__(&self) -> String {
        format!(
            "TableSyncStats(table={:?}, rows_fetched={}, rows_inserted={}, rows_updated={})",
            self.table, self.rows_fetched, self.rows_inserted, self.rows_updated
        )
    }
}

impl From<TableSyncStats> for PyTableSyncStats {
    fn from(s: TableSyncStats) -> Self {
        Self {
            table: s.table,
            rows_fetched: s.rows_fetched,
            rows_inserted: s.rows_inserted,
            rows_updated: s.rows_updated,
            text_fallback_columns: s.text_fallback_columns,
        }
    }
}

/// What one whole run produced, mirroring `pipeline::SyncReport`.
#[pyclass(name = "SyncReport", frozen, get_all)]
pub struct PySyncReport {
    /// Per-table results, in the order the tables were configured.
    tables: Vec<Py<PyTableSyncStats>>,
    /// Rows fetched from the source across every table.
    total_rows_fetched: u64,
}

#[pymethods]
impl PySyncReport {
    fn __repr__(&self) -> String {
        format!(
            "SyncReport(tables={}, total_rows_fetched={})",
            self.tables.len(),
            self.total_rows_fetched
        )
    }
}

/// One column, mirroring `pipeline::ColumnPreflight`.
#[pyclass(name = "ColumnPreflight", frozen, get_all)]
pub struct PyColumnPreflight {
    /// Column name, as the source catalog spells it.
    name: String,
    /// The source type name, as the source catalog spells it.
    source_type: String,
    /// The Arrow type this column would be written as.
    arrow_type: String,
    /// False when the column would be written as text because its type is not mapped.
    recognised: bool,
}

#[pymethods]
impl PyColumnPreflight {
    fn __repr__(&self) -> String {
        format!(
            "ColumnPreflight(name={:?}, source_type={:?}, arrow_type={:?}, recognised={})",
            self.name, self.source_type, self.arrow_type, self.recognised
        )
    }
}

/// What a pre-flight check found for one table, mirroring [`TablePreflight`].
#[pyclass(name = "TablePreflight", frozen, get_all)]
pub struct PyTablePreflight {
    /// Qualified table name, as configured.
    table: String,
    /// Every column the sync would read.
    columns: Vec<Py<PyColumnPreflight>>,
    /// False if the configured watermark column is not a column of this table.
    watermark_present: bool,
    /// Configured primary key columns that do not exist in the table.
    missing_primary_key_columns: Vec<String>,
    /// The value this table has been synced up to, or `None` if never synced.
    last_synced_value: Option<String>,
    /// True if the watermark and every primary key column exist, so a sync would run.
    ready: bool,
}

#[pymethods]
impl PyTablePreflight {
    fn __repr__(&self) -> String {
        format!(
            "TablePreflight(table={:?}, ready={}, columns={})",
            self.table,
            self.ready,
            self.columns.len()
        )
    }
}

/// Builds the catalog from the `tables` argument.
///
/// Each entry is a mapping with `table`, `watermark_column` and `primary_key` keys. A
/// `primary_key` given as a bare string is accepted as a single-column key, because
/// writing `"id"` rather than `["id"]` is the obvious mistake to make and rejecting it
/// would be pedantry rather than safety.
fn build_catalog(tables: Vec<Bound<'_, PyAny>>) -> PyResult<SyncCatalog> {
    // A missing key is a configuration error, and the documented surface promises
    // ValueError for those. Letting `get_item`'s own KeyError through would contradict
    // both __init__.pyi and docs/api.md.
    fn required<'py>(entry: &Bound<'py, PyAny>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        entry.get_item(key).map_err(|_| {
            PyValueError::new_err(format!("each table configuration needs a {key:?} key"))
        })
    }

    let mut entries = Vec::with_capacity(tables.len());
    for entry in tables {
        let table: String = required(&entry, "table")?.extract()?;
        let watermark_column: String = required(&entry, "watermark_column")?.extract()?;
        let key = required(&entry, "primary_key")?;
        let primary_key: Vec<String> = match key.extract::<String>() {
            Ok(single) => vec![single],
            Err(_) => key.extract()?,
        };
        entries.push(TableSync {
            table,
            watermark_column,
            primary_key,
        });
    }
    SyncCatalog::new(entries).map_err(to_pyerr)
}

fn build_config(
    connection_string: String,
    output_uri: String,
    checkpoint_uri: Option<String>,
    fetch_batch_size: usize,
    login_timeout_sec: Option<u64>,
    query_timeout_sec: Option<u64>,
) -> SyncConfig {
    // An empty output_uri with no explicit checkpoint_uri leaves the checkpoint URI
    // empty too, which preflight reads as "do not report checkpoints". Deriving
    // "/_streamer_checkpoints" from nothing would probe the filesystem root instead.
    let checkpoint_uri = checkpoint_uri.unwrap_or_else(|| {
        if output_uri.is_empty() {
            String::new()
        } else {
            format!("{}/_streamer_checkpoints", output_uri.trim_end_matches('/'))
        }
    });
    SyncConfig {
        connect: ConnectConfig {
            connection_string,
            login_timeout_sec,
        },
        output_uri,
        checkpoint_uri,
        fetch_batch_size,
        query_timeout_sec,
    }
}

#[pyfunction]
#[pyo3(signature = (
    connection_string,
    output_uri,
    tables,
    *,
    checkpoint_uri = None,
    fetch_batch_size = 10_000,
    login_timeout_sec = 30,
    query_timeout_sec = 300,
    progress = None,
))]
// A keyword-argument surface is many arguments by construction; collapsing them into a
// config object would make the Python call site worse, which is the only thing this
// function exists to serve.
#[allow(clippy::too_many_arguments)]
fn sync_tables(
    py: Python<'_>,
    connection_string: String,
    output_uri: String,
    tables: Vec<Bound<'_, PyAny>>,
    checkpoint_uri: Option<String>,
    fetch_batch_size: usize,
    login_timeout_sec: Option<u64>,
    query_timeout_sec: Option<u64>,
    progress: Option<Py<PyAny>>,
) -> PyResult<PySyncReport> {
    let catalog = build_catalog(tables)?;
    let config = build_config(
        connection_string,
        output_uri,
        checkpoint_uri,
        fetch_batch_size,
        login_timeout_sec,
        query_timeout_sec,
    );

    // The callback runs on the calling thread with the GIL held. It reports progress and,
    // by re-checking signals, lets Ctrl-C stop a sync that would otherwise run for hours.
    // Returning false aborts with Error::Interrupted.
    //
    // A signal, or an exception raised by the caller's callback, is restored as the
    // pending Python exception rather than discarded, and picked back up below: reporting
    // every abort as a bare KeyboardInterrupt would throw away the real diagnostic.
    let on_progress = |p: Progress| -> bool {
        Python::attach(|py| {
            if let Err(err) = py.check_signals() {
                err.restore(py);
                return false;
            }
            let Some(cb) = progress.as_ref() else {
                return true;
            };
            let payload = PyDict::new(py);
            let called = payload
                .set_item("table", p.table)
                .and_then(|()| payload.set_item("tables_done", p.tables_done))
                .and_then(|()| payload.set_item("total_tables", p.total_tables))
                .and_then(|()| payload.set_item("rows_fetched", p.rows_fetched))
                .and_then(|()| cb.call1(py, (payload,)).map(|_| ()));
            match called {
                Ok(()) => true,
                Err(err) => {
                    err.restore(py);
                    false
                }
            }
        })
    };

    let outcome = py.detach(|| pipeline::run(&config, &catalog, on_progress));

    // Whatever the callback restored wins: it is the cause, and Error::Interrupted is
    // only the mechanism by which the sync stopped.
    if let Some(err) = PyErr::take(py) {
        return Err(err);
    }
    let report = outcome.map_err(to_pyerr)?;

    let tables = report
        .tables
        .into_iter()
        .map(|t| Py::new(py, PyTableSyncStats::from(t)))
        .collect::<PyResult<Vec<_>>>()?;

    Ok(PySyncReport {
        tables,
        total_rows_fetched: report.total_rows_fetched,
    })
}

#[pyfunction]
#[pyo3(signature = (
    connection_string,
    tables,
    *,
    output_uri = String::new(),
    checkpoint_uri = None,
    login_timeout_sec = 30,
    query_timeout_sec = 300,
))]
fn preflight(
    py: Python<'_>,
    connection_string: String,
    tables: Vec<Bound<'_, PyAny>>,
    output_uri: String,
    checkpoint_uri: Option<String>,
    login_timeout_sec: Option<u64>,
    query_timeout_sec: Option<u64>,
) -> PyResult<Vec<PyTablePreflight>> {
    let catalog = build_catalog(tables)?;
    let config = build_config(
        connection_string,
        output_uri,
        checkpoint_uri,
        1,
        login_timeout_sec,
        query_timeout_sec,
    );

    let found: Vec<TablePreflight> = py
        .detach(|| pipeline::preflight(&config, &catalog))
        .map_err(to_pyerr)?;

    found
        .into_iter()
        .map(|t| {
            Ok(PyTablePreflight {
                ready: t.is_ready(),
                table: t.table,
                watermark_present: t.watermark_present,
                missing_primary_key_columns: t.missing_primary_key_columns,
                last_synced_value: t.last_synced_value,
                columns: t
                    .columns
                    .into_iter()
                    .map(|c| {
                        Py::new(
                            py,
                            PyColumnPreflight {
                                name: c.name,
                                source_type: c.source_type,
                                arrow_type: c.arrow_type,
                                recognised: c.recognised,
                            },
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()?,
            })
        })
        .collect()
}
