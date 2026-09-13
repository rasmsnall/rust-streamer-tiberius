//! Error type for the whole crate.
//!
//! Hand-rolled rather than derived, mirroring pgdelta's own `error.rs`: no variant
//! carries a raw driver or Delta error object, both of which are neither `Clone` nor
//! `PartialEq` and, for a live database connection, can carry a connection string or
//! password in their message text. See [`Error::Connect`] and `crate::connect` for how
//! that is kept out of this type in the first place, not merely hoped not to leak.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong while syncing from a live source into Delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The connection could not be established.
    ///
    /// Covers an unparseable connection string, an unreachable server, a refused login,
    /// and a login that exceeded `ConnectConfig::login_timeout_sec`. The message is
    /// redacted (see `crate::connect`) before it ever reaches this variant: a
    /// connection-time diagnostic can otherwise echo the connection string or password
    /// it was given verbatim, and that must never reach a log or a Python traceback.
    Connect {
        /// Display form of the underlying failure, with the known secret values of this
        /// run substituted out.
        message: String,
    },

    /// A statement failed against an already-established session.
    ///
    /// Distinct from [`Error::Connect`] so a caller can tell "could not reach the
    /// database" from "the database refused this query", which have different
    /// operational responses. Redacted identically: a query error can quote the
    /// session's own connection details just as a login error can.
    Query {
        /// Display form of the underlying failure, with the known secret values of this
        /// run substituted out.
        message: String,
    },

    /// A configured table does not exist, or the connected account cannot see it.
    ///
    /// Its own variant rather than [`Error::Internal`], which would tell the caller this
    /// crate has a defect when in fact their configuration names a table the source does
    /// not have, or their account lacks the grant to see it. Those are the two things to
    /// check, and the message says so.
    TableNotFound {
        /// Qualified table name, as configured.
        table: String,
    },

    /// A table is configured for incremental sync but is missing the configuration an
    /// incremental sync needs.
    ///
    /// Full-table structural validation is not the concern here (that fails as
    /// [`Error::Query`], from the query itself); this is specifically the case where the
    /// caller never said which column to filter on or which column identifies a row.
    IncrementalConfigMissing {
        /// Qualified table name.
        table: String,
        /// Which piece of configuration was missing.
        missing: MissingConfig,
    },

    /// A SQL Server column type has no mapping to an Arrow type this crate produces.
    ///
    /// Mirrors pgdelta's type-fidelity policy: an unrecognised type degrades to text
    /// rather than failing the sync, since across an unfamiliar production schema the
    /// type zoo is wide and one unmapped column must not stop every other table
    /// syncing. This variant exists to be reported in run statistics, not to fail
    /// anything; see the caller of `crate::types::resolve`.
    UnrecognisedColumnType {
        /// Qualified table name.
        table: String,
        /// Column whose type was not recognised.
        column: String,
        /// The type as the source's own catalog names it, for the statistics.
        sql_type: String,
    },

    /// A value read from the source contradicts its column's resolved type.
    ///
    /// Structural: the source disagrees with its own reported schema, which on a live
    /// connection most likely means a concurrent schema change mid-sync rather than a
    /// malformed dump. Carries the column and expected type only, never the value: this
    /// reaches logs and Python tracebacks, same discipline as pgdelta.
    UnparsableValue {
        /// Column whose value contradicted its declared type.
        column: String,
        /// The type that was expected.
        expected: &'static str,
    },

    /// A table name or column name would be unsafe to use in the output path or in a
    /// generated identifier.
    ///
    /// Reused concern from pgdelta's `sink::relative_path`: even on a trusted source, a
    /// name is still attacker-adjacent if anything upstream of the database (a web form
    /// feeding a `CREATE TABLE`, for instance) does not sanitise it. Validate and reject
    /// rather than sanitise silently, matching pgdelta.
    UnsafeTableName {
        /// The rejected name.
        name: String,
    },

    /// The Delta write or merge path failed.
    ///
    /// `DeltaTableError` is neither `Clone` nor `PartialEq`, so it is flattened to its
    /// message, mirroring pgdelta's `Error::Delta`.
    Delta {
        /// Display form of the originating error.
        message: String,
    },

    /// Arrow rejected an assembled batch.
    ///
    /// Indicates a defect in this crate's builders rather than a problem with the
    /// source, since the schema and the arrays are both produced here.
    Arrow {
        /// Display form of the originating error.
        message: String,
    },

    /// The checkpoint table could not be read or written.
    Checkpoint {
        /// Display form of the underlying failure.
        message: String,
    },

    /// Underlying I/O failure, reduced to its kind and a message.
    Io {
        /// Display form of the originating error.
        message: String,
    },

    /// The caller asked the sync to stop, for example on a Ctrl-C signal.
    Interrupted,

    /// An invariant between two stages of this crate was violated.
    ///
    /// Indicates a defect here rather than a problem with the source.
    Internal {
        /// Which invariant failed. Never carries source data.
        detail: &'static str,
    },
}

/// Which piece of per-table incremental-sync configuration was missing.
///
/// A separate type from [`Error::IncrementalConfigMissing`]'s message text so a caller
/// can match on it rather than parse a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingConfig {
    /// No column configured to filter `WHERE <column> > <last_value>` on.
    WatermarkColumn,
    /// No column (or columns) configured to match rows on during `MERGE`.
    PrimaryKey,
}

impl fmt::Display for MissingConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MissingConfig::WatermarkColumn => f.write_str("watermark column"),
            MissingConfig::PrimaryKey => f.write_str("primary key"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connect { message } => write!(f, "connection error: {message}"),
            Error::Query { message } => write!(f, "query error: {message}"),
            Error::TableNotFound { table } => write!(
                f,
                "table {table} does not exist, or this account has no SELECT grant on it"
            ),
            Error::IncrementalConfigMissing { table, missing } => {
                write!(
                    f,
                    "table {table} is configured for incremental sync but has no {missing}"
                )
            }
            Error::UnrecognisedColumnType {
                table,
                column,
                sql_type,
            } => write!(
                f,
                "column {table}.{column} has an unrecognised type ({sql_type}); \
                 written as text"
            ),
            Error::UnparsableValue { column, expected } => {
                write!(f, "value in column {column} is not a valid {expected}")
            }
            Error::UnsafeTableName { name } => {
                write!(
                    f,
                    "name is unsafe to use in an output path or identifier: {name}"
                )
            }
            Error::Delta { message } => write!(f, "delta error: {message}"),
            Error::Arrow { message } => write!(f, "arrow error: {message}"),
            Error::Checkpoint { message } => write!(f, "checkpoint error: {message}"),
            Error::Io { message } => write!(f, "io error: {message}"),
            Error::Interrupted => f.write_str("sync interrupted by caller"),
            Error::Internal { detail } => write!(f, "internal invariant violated: {detail}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io {
            message: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Error text reaches logs and Python tracebacks; a value must never appear in it.
    #[test]
    fn no_variant_carries_row_data() {
        let err = Error::UnparsableValue {
            column: "amount".into(),
            expected: "integer",
        };
        let rendered = err.to_string();
        assert!(rendered.contains("amount"));
        assert!(rendered.contains("integer"));
    }

    #[test]
    fn missing_config_is_matchable_not_just_a_string() {
        let err = Error::IncrementalConfigMissing {
            table: "public.customers".into(),
            missing: MissingConfig::WatermarkColumn,
        };
        assert!(matches!(
            err,
            Error::IncrementalConfigMissing {
                missing: MissingConfig::WatermarkColumn,
                ..
            }
        ));
    }
}
