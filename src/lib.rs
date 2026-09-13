//! Stream a live SQL Server database incrementally into Delta Lake tables.
//!
//! See `CLAUDE.md` for the design: this is a sibling project to `rust-streamer-pgdb`,
//! not a variant of it, and three of that project's foundational assumptions are flipped
//! here (live connection, credentials matter, incremental not full-reload).
//!
//! Connectivity is `tiberius`, a native pure-Rust TDS client, so nothing beyond this
//! binary has to be installed to reach SQL Server.
//!
//! Every public item carries rustdoc; see `CLAUDE.md`'s Documentation standard.
//!
//! # Entry points
//!
//! [`pipeline::run`] syncs a whole [`catalog::SyncCatalog`] over one connection and is
//! what the compiled `tiberiusdelta` Python module wraps; [`pipeline::preflight`] checks
//! the same catalog against the live source without writing anything.
//! [`pipeline::sync_table`] is the single-table async form, for a caller that already has
//! a Tokio runtime. `run` and `preflight` are blocking, drive their own runtime, and must
//! not be called from inside one.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod builders;
pub mod catalog;
pub mod checkpoint;
pub mod connect;
pub mod error;
pub mod merge;
pub mod pipeline;
pub mod types;

pub use error::{Error, Result};

mod python;

use pyo3::prelude::*;

/// The compiled half of the `tiberiusdelta` package. `python/tiberiusdelta/__init__.py`
/// re-exports its contents, so callers import from `tiberiusdelta`, not
/// `tiberiusdelta._tiberiusdelta`.
#[pymodule]
fn _tiberiusdelta(module: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(module)
}
