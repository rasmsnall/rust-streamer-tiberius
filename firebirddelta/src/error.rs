//! Error type for the whole crate.
//!
//! Hand-rolled rather than derived, mirroring tiberiusdelta's own `error.rs` (which itself
//! mirrors pgdelta's): no variant carries a raw driver or Delta error object, both of
//! which are neither `Clone` nor `PartialEq` and, for a live database connection, can
//! carry a connection string or password in their message text. See [`Error::Connect`]
//! and `crate::connect` for how that is kept out of this type in the first place, not
//! merely hoped not to leak.

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

    /// Another writer committed to this table's Delta table at the same time.
    ///
    /// Delta's optimistic concurrency detected the conflict and refused the commit, so
    /// nothing is corrupted: the data is exactly as the other writer left it. The usual
    /// cause is two runs of the same sync overlapping, which on Databricks means a job
    /// whose "maximum concurrent runs" is above one, or a manual run started while the
    /// scheduled one was still going.
    ///
    /// Safe to retry: the merge is an upsert and the checkpoint of a table that failed
    /// here was never advanced, so a later run re-fetches and re-applies the same rows.
    ConcurrentWrite {
        /// Qualified table name whose commit lost the race.
        table: String,
    },

    /// A column named in a table's configuration does not exist in that table.
    ///
    /// Distinct from [`Error::IncrementalConfigMissing`], which is about configuration
    /// that was never supplied; this is configuration that was supplied and does not
    /// match the source, which is what happens when a column is renamed or dropped.
    ColumnNotFound {
        /// Qualified table name.
        table: String,
        /// The configured column that the table does not have.
        column: String,
        /// What the column was configured as, for example "watermark column".
        role: &'static str,
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

    /// A Firebird column type has no mapping to an Arrow type this crate produces.
    ///
    /// Mirrors tiberiusdelta's (and pgdelta's) type-fidelity policy: an unrecognised type
    /// degrades to text rather than failing the sync, since across an unfamiliar
    /// production schema the type zoo is wide and one unmapped column must not stop
    /// every other table syncing. This variant exists to be reported in run statistics,
    /// not to fail anything; see the caller of `crate::types::resolve`.
    UnrecognisedColumnType {
        /// Qualified table name.
        table: String,
        /// Column whose type was not recognised.
        column: String,
        /// The type as the source's own catalog names it, for the statistics.
        sql_type: String,
    },

    /// A column's type cannot be read through this crate's chosen Firebird client at all,
    /// and was excluded from the query entirely rather than degraded to text.
    ///
    /// Distinct from [`Error::UnrecognisedColumnType`]: an unrecognised type still
    /// reaches this crate as *some* value and is written faithfully as text, but a small
    /// number of Firebird types make `rsfbclient`'s pure-Rust row reader refuse to
    /// describe the statement at all (see `CLAUDE.md`'s Driver notes). Those columns
    /// cannot be selected without a server-side `CAST`, and this crate applies one where
    /// it can (see `crate::types::resolve`); what remains after that is excluded, never
    /// included in a doomed `SELECT`, and reported here instead.
    ExcludedColumnType {
        /// Qualified table name.
        table: String,
        /// Column excluded from the query.
        column: String,
        /// The type as the source's own catalog names it.
        sql_type: String,
    },

    /// A value read from the source contradicts its column's resolved type.
    ///
    /// Structural: the source disagrees with its own reported schema, which on a live
    /// connection most likely means a concurrent schema change mid-sync rather than a
    /// malformed dump. Carries the column and expected type only, never the value: this
    /// reaches logs and Python tracebacks, same discipline as pgdelta and tiberiusdelta.
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
    /// rather than sanitise silently, matching pgdelta and tiberiusdelta.
    UnsafeTableName {
        /// The rejected name.
        name: String,
    },

    /// The Delta write or merge path failed.
    ///
    /// `DeltaTableError` is neither `Clone` nor `PartialEq`, so it is flattened to its
    /// message, mirroring pgdelta's and tiberiusdelta's `Error::Delta`.
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
            Error::ConcurrentWrite { table } => write!(
                f,
                "another writer committed to {table} at the same time, so this commit was                  refused; nothing was corrupted and the sync can simply be re-run. If this                  recurs, ensure only one sync runs at a time (on Databricks, set the job's                  maximum concurrent runs to 1)"
            ),
            Error::ColumnNotFound {
                table,
                column,
                role,
            } => write!(
                f,
                "{table} has no column {column}, configured as its {role}; run a pre-flight                  check to see the columns it does have"
            ),
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
            Error::ExcludedColumnType {
                table,
                column,
                sql_type,
            } => write!(
                f,
                "column {table}.{column} has type {sql_type}, which this crate's Firebird \
                 client cannot read even as text; excluded from the sync"
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

impl Error {
    /// Returns this error with `table` named in its message, if it does not already say.
    ///
    /// A run covers many tables, so "delta error: ..." is markedly less useful than
    /// "delta error on RSHIP.INVOICES: ...". Only the variants whose message is free
    /// text are annotated; the rest already carry the table in a field of their own and
    /// would only be made repetitive by this.
    ///
    /// # Panics
    ///
    /// Does not panic.
    #[must_use]
    pub fn in_table(self, table: &str) -> Self {
        let annotate = |message: String| {
            if message.contains(table) {
                message
            } else {
                format!("on table {table}: {message}")
            }
        };
        match self {
            Error::Connect { message } => Error::Connect {
                message: annotate(message),
            },
            Error::Query { message } => Error::Query {
                message: annotate(message),
            },
            Error::Delta { message } => Error::Delta {
                message: annotate(message),
            },
            Error::Arrow { message } => Error::Arrow {
                message: annotate(message),
            },
            Error::Checkpoint { message } => Error::Checkpoint {
                message: annotate(message),
            },
            Error::Io { message } => Error::Io {
                message: annotate(message),
            },
            other => other,
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
            table: "RSHIP.CUSTOMERS".into(),
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

    #[test]
    fn excluded_column_type_names_the_column_and_type() {
        let err = Error::ExcludedColumnType {
            table: "RSHIP.SENSORS".into(),
            column: "READING".into(),
            sql_type: "DECFLOAT(34)".into(),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("READING"));
        assert!(rendered.contains("DECFLOAT(34)"));
    }
}
