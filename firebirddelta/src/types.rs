//! Firebird column type to the crate's internal type model.
//!
//! Executes wherever a table's column metadata is read, which is the
//! `RDB$RELATION_FIELDS`/`RDB$FIELDS` catalog lookup done once per table before its rows
//! are fetched (see `crate::pipeline::describe_table`). Nothing here touches row data:
//! like tiberiusdelta's and pgdelta's own `types.rs`, this module resolves *what kind of
//! column this is*, and `crate::builders` binds the result to an Arrow `DataType` and
//! does the actual per-value conversion.
//!
//! # Why the catalog, and not `rsfbclient`'s own `SqlType`
//!
//! `rsfbclient::SqlType` (the value type this crate actually receives one decoded cell
//! as) is deliberately coarse: `Text(String)`, `Integer(i64)`, `Floating(f64)`,
//! `Timestamp(NaiveDateTime)`, `Binary(Vec<u8>)`, `Boolean(bool)`, `Null`. `SMALLINT`,
//! `INTEGER` and `BIGINT` all arrive as the same `Integer(i64)`; `DATE`, `TIME` and
//! `TIMESTAMP` all arrive as the same `Timestamp(NaiveDateTime)` (a `DATE` gets a
//! synthetic midnight time attached, a `TIME` a synthetic date); see "Driver notes"
//! below. Since the Arrow schema has to be fixed *before* any row arrives (a sync that
//! fetches zero changed rows still opens its Delta table), and since the wire value alone
//! cannot even tell a `DATE` from a `TIME`, the wire type cannot drive it.
//! `RDB$RELATION_FIELDS` joined to `RDB$FIELDS` is standard Firebird catalog SQL, costs
//! one round trip per table, and answers exactly this, which is also what makes this
//! module a close sibling of tiberiusdelta's and pgdelta's own type modules: all three
//! map a *catalog's* type name (or code) to the same kind of internal model.
//!
//! # Fidelity policy
//!
//! Type uncertainty degrades and never fails, mirroring tiberiusdelta and pgdelta. A type
//! this crate does not map at all resolves to text, preserving the value as a string,
//! with [`ResolvedType::recognised`] false so the fact is reportable rather than silent.
//! A small number of Firebird 4+ types go one step further: `rsfbclient`'s pure-Rust row
//! reader cannot describe them *at all* (see "Driver notes"), so they cannot even reach
//! this crate as a value to render. Where a server-side `SELECT CAST(... AS VARCHAR(n))`
//! can move the conversion to the source before the value ever reaches `rsfbclient`, this
//! module says so via [`ResolvedType::cast_as`] and [`crate::pipeline`] builds the query
//! accordingly; where no such cast exists (an oddly-subtyped `BLOB`), the column is
//! excluded from the query entirely rather than attempted and failed.

/// The subset of Firebird's type system this crate distinguishes.
///
/// Everything not listed resolves to [`SqlType::Text`]. That is a deliberate floor, the
/// same as tiberiusdelta's and pgdelta's own `Text` fallback: text is always a faithful
/// representation of whatever the source reported, once it can reach this crate as a
/// value at all (see [`ResolvedType::excluded`] for the cases where it cannot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    /// `SMALLINT` (`RDB$FIELD_TYPE` 7, sub-type 0). 16-bit, signed.
    SmallInt,
    /// `INTEGER` (`RDB$FIELD_TYPE` 8, sub-type 0). 32-bit, signed.
    Integer,
    /// `BIGINT` (`RDB$FIELD_TYPE` 16, sub-type 0). 64-bit, signed.
    BigInt,
    /// `FLOAT` (`RDB$FIELD_TYPE` 10). 32-bit float, widened to 64-bit in Arrow: see
    /// [`SqlType::Double`] for why both land on the same Arrow type regardless.
    Real,
    /// `DOUBLE PRECISION` (`RDB$FIELD_TYPE` 27), and also `NUMERIC`/`DECIMAL` (any
    /// `RDB$FIELD_TYPE` of 7/8/16 with sub-type 1 or 2): see [`resolve`] for why the
    /// exact numeric types land here rather than on an Arrow `Decimal128`.
    Double,
    /// `BOOLEAN` (`RDB$FIELD_TYPE` 23, Firebird 3+).
    Boolean,
    /// `DATE` (`RDB$FIELD_TYPE` 12).
    Date,
    /// `TIME` (`RDB$FIELD_TYPE` 13). Written as text, because Delta Lake has no
    /// time-of-day type at all: see `crate::builders::arrow_type`. The type is
    /// recognised, so this is not reported as an unmapped column; what degrades is the
    /// target format, not this crate's understanding of the source. The same policy
    /// tiberiusdelta applies to T-SQL's `TIME`.
    Time,
    /// `TIMESTAMP` (`RDB$FIELD_TYPE` 35). No timezone of its own; assumed UTC, see
    /// `crate::builders`.
    Timestamp,
    /// `CHAR`/`VARCHAR` (`RDB$FIELD_TYPE` 14/37), and `BLOB SUB_TYPE TEXT`
    /// (`RDB$FIELD_TYPE` 261, sub-type 1).
    Text,
    /// `BLOB SUB_TYPE BINARY` (`RDB$FIELD_TYPE` 261, sub-type 0).
    Binary,
    /// A Firebird 4+ type `rsfbclient`'s pure-Rust row reader cannot describe on its own
    /// (`INT128`, `DECFLOAT(16)`, `DECFLOAT(34)`, `TIME WITH TIME ZONE`,
    /// `TIMESTAMP WITH TIME ZONE`), moved server-side to a `CAST(... AS VARCHAR(64))` so
    /// it can be selected at all, and written as its literal text. See [`resolve`] and
    /// "Driver notes" in `CLAUDE.md`.
    ///
    /// Written as text for the same reason [`SqlType::Time`] is: not because the value
    /// is unrecognised, but because what it lands in cannot hold it any other way. Two
    /// of these (`INT128`, `DECFLOAT`) are, ironically, *more* faithfully represented
    /// this way than [`SqlType::Double`]'s `NUMERIC`/`DECIMAL`, which reaches this crate
    /// already lossy: casting to text here is a deliberate escape hatch, not a downgrade.
    CastToText,
    /// Recognised as a type Firebird has, but one `rsfbclient`'s pure-Rust row reader
    /// cannot read even after a `CAST` (a `BLOB` whose sub-type is neither binary (0)
    /// nor text (1) is the practical case: arrays and user-defined BLOB sub-types have no
    /// generic textual form to cast to). The column is excluded from the query
    /// altogether; see [`ResolvedType::excluded`].
    Unsupported,
}

/// The outcome of resolving one column's catalog-reported type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedType {
    /// The type this column maps to.
    pub sql: SqlType,
    /// False when the source type could not be represented at all and fell back to
    /// text. Always true for [`SqlType::CastToText`]: the type is understood, only the
    /// storage format degrades (see that variant's docs). Always false for
    /// [`SqlType::Unsupported`], which is also [`ResolvedType::excluded`].
    pub recognised: bool,
    /// The type name as the source's own catalog spells it, retained for run statistics
    /// and error messages. Never a value: this is schema metadata, not row data.
    pub source: String,
    /// A server-side `CAST` expression to substitute for the bare column name in the
    /// generated `SELECT`, when [`SqlType::CastToText`] requires one to be selectable at
    /// all. `None` for every other variant, including [`SqlType::Unsupported`], whose
    /// columns are dropped from the query rather than cast.
    pub cast_as: Option<String>,
}

impl ResolvedType {
    /// True if this column cannot be selected at all and must be dropped from the query
    /// `crate::pipeline` builds, rather than degraded to text.
    #[must_use]
    pub fn excluded(&self) -> bool {
        matches!(self.sql, SqlType::Unsupported)
    }
}

/// `RDB$FIELD_TYPE` codes, from Firebird's `RDB$TYPES` system table (`RDB$FIELD_NAME` =
/// `'RDB$FIELD_TYPE'`). Stable across Firebird versions; 24-29 are Firebird 4 additions.
mod field_type {
    pub const SMALLINT: i32 = 7;
    pub const INTEGER: i32 = 8;
    pub const FLOAT: i32 = 10;
    pub const DATE: i32 = 12;
    pub const TIME: i32 = 13;
    pub const CHAR: i32 = 14;
    pub const BIGINT: i32 = 16;
    pub const BOOLEAN: i32 = 23;
    pub const DECFLOAT16: i32 = 24;
    pub const DECFLOAT34: i32 = 25;
    pub const INT128: i32 = 26;
    pub const DOUBLE: i32 = 27;
    pub const TIME_TZ: i32 = 28;
    pub const TIMESTAMP_TZ: i32 = 29;
    pub const TIMESTAMP: i32 = 35;
    pub const VARCHAR: i32 = 37;
    pub const BLOB: i32 = 261;
}

/// `RDB$FIELD_SUB_TYPE` codes for an exact numeric stored in a `SMALLINT`/`INTEGER`/
/// `BIGINT` column (`0`/`NULL` means the column really is that integer type).
mod numeric_sub_type {
    pub const NUMERIC: i32 = 1;
    pub const DECIMAL: i32 = 2;
}

/// `RDB$FIELD_SUB_TYPE` codes for a `BLOB` column.
mod blob_sub_type {
    pub const BINARY: i32 = 0;
    pub const TEXT: i32 = 1;
}

/// Width of the `VARCHAR` a [`SqlType::CastToText`] column is cast to.
///
/// Generous for every type that lands here: `INT128`'s widest text form is 40 characters
/// (39 digits plus a sign), `DECFLOAT(34)`'s is under 45 (34 significant digits plus sign,
/// decimal point and a signed two-digit exponent), and `TIME`/`TIMESTAMP WITH TIME ZONE`'s
/// ISO-ish rendering is under 45 including a named zone. 64 leaves headroom without
/// guessing at an exact bound for each type individually.
const CAST_TO_TEXT_WIDTH: u16 = 64;

/// Resolves one column's type from its `RDB$RELATION_FIELDS`/`RDB$FIELDS` catalog row.
///
/// `column` is the column name, used only to build [`ResolvedType::cast_as`] (a `CAST`
/// expression must repeat the column it casts). `field_type` and `field_sub_type` are
/// `RDB$FIELDS.RDB$FIELD_TYPE` and `RDB$FIELDS.RDB$FIELD_SUB_TYPE` (the latter `NULL` for
/// every type it does not apply to, read here as `None`).
///
/// Never fails. An unrepresentable declaration resolves to text with
/// [`ResolvedType::recognised`] false, and one this crate's chosen Firebird client
/// cannot read at all resolves to [`SqlType::Unsupported`] (see the module docs).
///
/// `NUMERIC` and `DECIMAL` (any of `SMALLINT`/`INTEGER`/`BIGINT` with `field_sub_type` 1
/// or 2) resolve to [`SqlType::Double`], not a decimal type, because that is what
/// actually arrives: `rsfbclient`'s pure-Rust row reader coerces any scaled integer
/// column to Firebird's own wire-level `DOUBLE` before decoding it (see `CLAUDE.md`'s
/// Driver notes), so the exactness is already gone before this crate ever sees the
/// value, and declaring an Arrow decimal would claim a fidelity the data no longer has.
/// There is no escape hatch for this one the way there is for the Firebird 4+ types
/// below: casting server-side to `VARCHAR` would work, but this crate does not do it
/// automatically for `NUMERIC`/`DECIMAL`, since that is Firebird's single most common
/// exact-numeric type and silently changing every occurrence of it into a string column
/// would be a bigger, more surprising behaviour change than the one degradation this
/// module already documents. A caller for whom this precision loss matters should
/// `CAST` the column to `VARCHAR` in the source schema (a view, typically) before
/// configuring it here.
///
/// `INT128`, `DECFLOAT(16)`, `DECFLOAT(34)`, `TIME WITH TIME ZONE` and
/// `TIMESTAMP WITH TIME ZONE` resolve to [`SqlType::CastToText`]: `rsfbclient`'s pure-Rust
/// row reader refuses to describe a statement selecting any of these at all ("Unsupported
/// column type"), so `crate::pipeline` must select `CAST(column AS VARCHAR(64))` instead
/// of the bare column name for these, which is why [`ResolvedType::cast_as`] is
/// populated for them.
///
/// A `BLOB` whose `field_sub_type` is neither binary (0) nor text (1) resolves to
/// [`SqlType::Unsupported`]: there is no generic textual `CAST` for an arbitrary BLOB
/// sub-type, so this column cannot be selected at all and `crate::pipeline` drops it from
/// the query.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use firebirddelta::types::{SqlType, resolve};
///
/// assert_eq!(resolve("ID", 8, None).sql, SqlType::Integer);
/// assert_eq!(resolve("BALANCE", 8, Some(2)).sql, SqlType::Double);
/// assert!(resolve("READING", 25, None).recognised, "DECFLOAT is understood");
/// assert!(resolve("READING", 25, None).cast_as.is_some());
/// ```
pub fn resolve(column: &str, field_type: i32, field_sub_type: Option<i32>) -> ResolvedType {
    let source = source_name(field_type, field_sub_type);
    let cast = |sql: SqlType| ResolvedType {
        sql,
        recognised: true,
        source: source.clone(),
        cast_as: Some(format!("CAST({column} AS VARCHAR({CAST_TO_TEXT_WIDTH}))")),
    };
    let plain = |sql: SqlType, recognised: bool| ResolvedType {
        sql,
        recognised,
        source: source.clone(),
        cast_as: None,
    };

    match field_type {
        field_type::SMALLINT | field_type::INTEGER | field_type::BIGINT => match field_sub_type {
            Some(numeric_sub_type::NUMERIC) | Some(numeric_sub_type::DECIMAL) => {
                plain(SqlType::Double, true)
            }
            _ => plain(
                match field_type {
                    field_type::SMALLINT => SqlType::SmallInt,
                    field_type::INTEGER => SqlType::Integer,
                    _ => SqlType::BigInt,
                },
                true,
            ),
        },
        field_type::FLOAT => plain(SqlType::Real, true),
        field_type::DOUBLE => plain(SqlType::Double, true),
        field_type::BOOLEAN => plain(SqlType::Boolean, true),
        field_type::DATE => plain(SqlType::Date, true),
        field_type::TIME => plain(SqlType::Time, true),
        field_type::TIMESTAMP => plain(SqlType::Timestamp, true),
        field_type::CHAR | field_type::VARCHAR => plain(SqlType::Text, true),
        field_type::BLOB => match field_sub_type {
            Some(blob_sub_type::BINARY) | None => plain(SqlType::Binary, true),
            Some(blob_sub_type::TEXT) => plain(SqlType::Text, true),
            Some(_) => plain(SqlType::Unsupported, false),
        },
        field_type::INT128
        | field_type::DECFLOAT16
        | field_type::DECFLOAT34
        | field_type::TIME_TZ
        | field_type::TIMESTAMP_TZ => cast(SqlType::CastToText),
        _ => plain(SqlType::Text, false),
    }
}

/// Renders a catalog type code (and, where relevant, sub-type) as the name this module's
/// callers should see in statistics and error messages, since the raw catalog only gives
/// numeric codes.
fn source_name(field_type: i32, field_sub_type: Option<i32>) -> String {
    match field_type {
        field_type::SMALLINT => numeric_source_name("SMALLINT", field_sub_type),
        field_type::INTEGER => numeric_source_name("INTEGER", field_sub_type),
        field_type::BIGINT => numeric_source_name("BIGINT", field_sub_type),
        field_type::FLOAT => "FLOAT".to_string(),
        field_type::DOUBLE => "DOUBLE PRECISION".to_string(),
        field_type::BOOLEAN => "BOOLEAN".to_string(),
        field_type::DATE => "DATE".to_string(),
        field_type::TIME => "TIME".to_string(),
        field_type::TIMESTAMP => "TIMESTAMP".to_string(),
        field_type::CHAR => "CHAR".to_string(),
        field_type::VARCHAR => "VARCHAR".to_string(),
        field_type::BLOB => match field_sub_type {
            Some(blob_sub_type::BINARY) | None => "BLOB SUB_TYPE BINARY".to_string(),
            Some(blob_sub_type::TEXT) => "BLOB SUB_TYPE TEXT".to_string(),
            Some(other) => format!("BLOB SUB_TYPE {other}"),
        },
        field_type::INT128 => "INT128".to_string(),
        field_type::DECFLOAT16 => "DECFLOAT(16)".to_string(),
        field_type::DECFLOAT34 => "DECFLOAT(34)".to_string(),
        field_type::TIME_TZ => "TIME WITH TIME ZONE".to_string(),
        field_type::TIMESTAMP_TZ => "TIMESTAMP WITH TIME ZONE".to_string(),
        other => format!("RDB$FIELD_TYPE {other}"),
    }
}

fn numeric_source_name(base: &str, field_sub_type: Option<i32>) -> String {
    match field_sub_type {
        Some(numeric_sub_type::NUMERIC) => "NUMERIC".to_string(),
        Some(numeric_sub_type::DECIMAL) => "DECIMAL".to_string(),
        _ => base.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(field_type: i32) -> SqlType {
        resolve("C", field_type, None).sql
    }

    #[test]
    fn integer_family() {
        assert_eq!(sql(field_type::SMALLINT), SqlType::SmallInt);
        assert_eq!(sql(field_type::INTEGER), SqlType::Integer);
        assert_eq!(sql(field_type::BIGINT), SqlType::BigInt);
    }

    #[test]
    fn float_family() {
        assert_eq!(sql(field_type::FLOAT), SqlType::Real);
        assert_eq!(sql(field_type::DOUBLE), SqlType::Double);
    }

    /// `NUMERIC`/`DECIMAL` arrive from `rsfbclient` as `f64` regardless of declared
    /// precision and scale, so the declared Arrow type must say so rather than claim a
    /// decimal's exactness it can never deliver.
    #[test]
    fn numeric_and_decimal_resolve_to_double_not_a_decimal_type() {
        let numeric = resolve(
            "BALANCE",
            field_type::INTEGER,
            Some(numeric_sub_type::NUMERIC),
        );
        assert_eq!(numeric.sql, SqlType::Double);
        assert!(numeric.recognised);
        assert_eq!(numeric.source, "NUMERIC");

        let decimal = resolve(
            "BALANCE",
            field_type::BIGINT,
            Some(numeric_sub_type::DECIMAL),
        );
        assert_eq!(decimal.sql, SqlType::Double);
        assert_eq!(decimal.source, "DECIMAL");
    }

    #[test]
    fn plain_integer_columns_are_not_mistaken_for_numeric() {
        let r = resolve("ID", field_type::INTEGER, None);
        assert_eq!(r.sql, SqlType::Integer);
        assert_eq!(r.source, "INTEGER");
    }

    #[test]
    fn temporal_types() {
        assert_eq!(sql(field_type::DATE), SqlType::Date);
        assert_eq!(sql(field_type::TIME), SqlType::Time);
        assert_eq!(sql(field_type::TIMESTAMP), SqlType::Timestamp);
    }

    #[test]
    fn text_types_are_recognised() {
        for t in [field_type::CHAR, field_type::VARCHAR] {
            let r = resolve("C", t, None);
            assert_eq!(r.sql, SqlType::Text);
            assert!(r.recognised);
        }
    }

    #[test]
    fn blob_subtypes() {
        let binary = resolve("B", field_type::BLOB, Some(blob_sub_type::BINARY));
        assert_eq!(binary.sql, SqlType::Binary);
        assert!(binary.recognised);

        let text = resolve("B", field_type::BLOB, Some(blob_sub_type::TEXT));
        assert_eq!(text.sql, SqlType::Text);
        assert!(text.recognised);

        // A BLOB's sub-type is NOT NULL in Firebird's own catalog, but treating a
        // missing value defensively as binary (the numerically-lowest, most common
        // case) is safer than panicking on an assumption about the source's own schema.
        let no_subtype = resolve("B", field_type::BLOB, None);
        assert_eq!(no_subtype.sql, SqlType::Binary);
    }

    /// A BLOB sub-type this crate cannot cast to text at all must be excluded from the
    /// query, not attempted and failed against a live source.
    #[test]
    fn an_exotic_blob_subtype_is_unsupported_and_excluded() {
        let r = resolve("SHAPE", field_type::BLOB, Some(16));
        assert_eq!(r.sql, SqlType::Unsupported);
        assert!(!r.recognised);
        assert!(r.excluded());
        assert!(r.cast_as.is_none());
    }

    /// Firebird 4's `INT128`/`DECFLOAT`/`... WITH TIME ZONE` types cannot even be
    /// described by `rsfbclient`'s pure-Rust reader, so they must be selected through a
    /// server-side `CAST`, not the bare column name.
    #[test]
    fn firebird_four_types_are_cast_to_text_server_side() {
        for (code, name) in [
            (field_type::INT128, "INT128"),
            (field_type::DECFLOAT16, "DECFLOAT(16)"),
            (field_type::DECFLOAT34, "DECFLOAT(34)"),
            (field_type::TIME_TZ, "TIME WITH TIME ZONE"),
            (field_type::TIMESTAMP_TZ, "TIMESTAMP WITH TIME ZONE"),
        ] {
            let r = resolve("READING", code, None);
            assert_eq!(r.sql, SqlType::CastToText);
            assert!(
                r.recognised,
                "{name} is understood; the reader cannot describe it"
            );
            assert!(!r.excluded());
            assert_eq!(
                r.cast_as.as_deref(),
                Some("CAST(READING AS VARCHAR(64))"),
                "{name} must be selected through a CAST, not its bare column name"
            );
            assert_eq!(r.source, name);
        }
    }

    #[test]
    fn unknown_field_types_degrade_to_text_and_are_reported() {
        let r = resolve("C", 9999, None);
        assert_eq!(r.sql, SqlType::Text);
        assert!(!r.recognised);
        assert!(!r.excluded());
    }

    #[test]
    fn only_unsupported_columns_are_excluded() {
        assert!(!resolve("C", field_type::INTEGER, None).excluded());
        assert!(!resolve("C", field_type::TIME, None).excluded());
        assert!(!resolve("C", field_type::INT128, None).excluded());
        assert!(resolve("C", field_type::BLOB, Some(4)).excluded());
    }
}
