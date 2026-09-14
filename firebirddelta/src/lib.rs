//! Stream a live Firebird database incrementally into Delta Lake tables.
//!
//! See `CLAUDE.md` for the design: this is a sibling project to `rust-streamer-pgdb`
//! (pgdelta) and to the sibling `tiberiusdelta` crate in this same repository, not a
//! variant of either. It shares tiberiusdelta's foundational assumptions (a live network
//! connection, credentials as a first-class concern, incremental not full-reload sync)
//! but targets Firebird rather than SQL Server, which changes the connectivity layer,
//! the type system, and (see `CLAUDE.md`'s Driver notes) several sharp edges that do not
//! exist in tiberiusdelta at all.
//!
//! Connectivity is `rsfbclient`'s pure-Rust wire-protocol client, so nothing beyond this
//! binary has to be installed to reach Firebird. Unlike `tiberius` in the sibling crate,
//! `rsfbclient` is synchronous; see `crate::connect`'s module docs for how this crate
//! bridges that into the async Delta write path.
//!
//! Every public item carries rustdoc; see `CLAUDE.md`'s Documentation standard.
//!
//! # Entry points
//!
//! [`pipeline::run`] syncs a whole [`catalog::SyncCatalog`] over one connection and is
//! what the compiled `firebirddelta` Python module wraps; [`pipeline::preflight`] checks
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

/// The compiled half of the `firebirddelta` package. `python/firebirddelta/__init__.py`
/// re-exports its contents, so callers import from `firebirddelta`, not
/// `firebirddelta._firebirddelta`.
#[pymodule]
fn _firebirddelta(module: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(module)
}
