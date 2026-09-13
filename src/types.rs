//! SQL Server column type to the crate's internal type model.
//!
//! Executes wherever a table's column metadata is read, which is the
//! `INFORMATION_SCHEMA.COLUMNS` lookup done once per table before its rows are fetched
//! (see `crate::pipeline::describe_table`). Nothing here is async and nothing here
//! touches row data: like pgdelta's `types.rs`, this module resolves *what kind of column
//! this is*, and `crate::builders` binds the result to an Arrow `DataType` and does the
//! actual per-value conversion.
//!
//! # Why the catalog, and not tiberius's own `ColumnType`
//!
//! `tiberius::ColumnType` is reported per result-set column and is rich, but it is the
//! *wire* type, and the wire collapses distinctions the schema needs. A nullable `INT`
//! arrives as `ColumnType::Intn` (an n-byte integer) rather than `Int4`, with its actual
//! width carried in TDS metadata `tiberius` does not expose; `Decimaln`/`Numericn`
//! likewise arrive without the column's declared precision and scale, and a decoded
//! `Numeric` value knows only its own. Since the Arrow schema has to be fixed *before*
//! any row arrives (a sync that fetches zero changed rows still opens its Delta table),
//! and since most production columns are nullable, the wire type cannot drive it.
//! `INFORMATION_SCHEMA.COLUMNS` is standard T-SQL, costs one round trip per table, and
//! answers exactly this, which is also what makes this module a close sibling of
//! pgdelta's own type module: both map a *catalog's* type name to the same internal model.
//!
//! # Fidelity policy
//!
//! Type uncertainty degrades and never fails, mirroring pgdelta. A type this crate does
//! not map, or a `NUMERIC`/`DECIMAL` whose precision or scale does not fit Arrow's
//! `Decimal128`, resolves to text, preserving the value as a string, with
//! [`ResolvedType::recognised`] false so the fact is reportable rather than silent.

/// The subset of SQL Server's type system this crate distinguishes.
///
/// Everything not listed resolves to [`SqlType::Text`]. That is a deliberate floor, the
/// same as pgdelta's `PgType::Text`: text is always a faithful representation of whatever
/// the source reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    /// `SMALLINT`. 16-bit, signed.
    SmallInt,
    /// `INT`. 32-bit, signed.
    Integer,
    /// `BIGINT`. 64-bit, signed.
    BigInt,
    /// `TINYINT`. Mapped to `Int16` rather than `Int8`: SQL Server's `TINYINT` is
    /// *unsigned* (0..=255), which does not fit a signed 8-bit Arrow type.
    TinyInt,
    /// `REAL` (`FLOAT(24)`). 32-bit float, widened to 64-bit in Arrow.
    Real,
    /// `FLOAT` (`FLOAT(53)`), and also `MONEY`/`SMALLMONEY`: see [`resolve`] for why the
    /// money types land here rather than on [`SqlType::Decimal`].
    Double,
    /// `DECIMAL`/`NUMERIC`, once the declared precision and scale are confirmed to fit
    /// Arrow's `Decimal128` (`1 <= precision <= 38`, `0 <= scale <= precision`).
    Decimal {
        /// Total significant digits.
        precision: u8,
        /// Digits after the decimal point.
        scale: i8,
    },
    /// `BIT`. Mapped to boolean.
    Bit,
    /// `DATE`.
    Date,
    /// `TIME`. Written as text, because Delta Lake has no time-of-day type at all: see
    /// `crate::builders::arrow_type`. The type is recognised, so this is not reported as
    /// an unmapped column; what degrades is the target format, not this crate's
    /// understanding of the source.
    Time,
    /// `DATETIME`, `DATETIME2`, `SMALLDATETIME`. No timezone of their own; assumed UTC,
    /// see `crate::builders`.
    Timestamp,
    /// `DATETIMEOFFSET`. Carries a real offset, which is normalised to UTC rather than
    /// assumed, the one temporal type here where the zone is not a guess.
    TimestampOffset,
    /// `BINARY`/`VARBINARY`/`IMAGE`, and `TIMESTAMP`/`ROWVERSION`.
    Binary,
    /// Everything textual, and everything unrecognised.
    Text,
}

/// The outcome of resolving one column's catalog-reported type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedType {
    /// The type this column maps to.
    pub sql: SqlType,
    /// False when the source type, or a `DECIMAL`/`NUMERIC`'s precision or scale, could
    /// not be represented and fell back to text.
    pub recognised: bool,
    /// The type name as the source's own catalog spells it, retained for run statistics
    /// and error messages. Never a value: this is schema metadata, not row data.
    pub source: String,
}

/// Arrow's `Decimal128` carries at most 38 significant digits.
const MAX_DECIMAL_PRECISION: u8 = 38;

/// True if a `DECIMAL`/`NUMERIC` of this precision and scale fits Arrow's `Decimal128`.
///
/// `INFORMATION_SCHEMA.COLUMNS` reports both as wider integers than Arrow's `u8`/`i8`,
/// so both bounds are checked explicitly rather than truncated: a precision or scale that
/// does not fit is a declaration this crate cannot represent natively, the same class of
/// problem as pgdelta's own too-wide-`numeric` case, and degrades to text rather than
/// silently wrapping.
fn decimal_fits(precision: i64, scale: i64) -> Option<(u8, i8)> {
    let precision = u8::try_from(precision).ok()?;
    let scale = i8::try_from(scale).ok()?;
    if !(1..=MAX_DECIMAL_PRECISION).contains(&precision) {
        return None;
    }
    if scale < 0 || i16::from(scale) > i16::from(precision) {
        return None;
    }
    Some((precision, scale))
}

/// Resolves one column's type from its `INFORMATION_SCHEMA.COLUMNS` row.
///
/// `data_type` is that view's `DATA_TYPE` (already lowercase in SQL Server, but matched
/// case-insensitively regardless). `precision` and `scale` are its `NUMERIC_PRECISION`
/// and `NUMERIC_SCALE`, which are `NULL` for every non-numeric type and are only
/// consulted for `DECIMAL`/`NUMERIC`.
///
/// Never fails. An unrepresentable declaration resolves to text with
/// [`ResolvedType::recognised`] false.
///
/// `MONEY` and `SMALLMONEY` resolve to [`SqlType::Double`], not [`SqlType::Decimal`],
/// because that is what actually arrives: `tiberius` decodes both to an `f64` (dividing
/// the wire's scaled integer by 10^4 in floating point), so the exactness is already gone
/// before this crate sees the value, and declaring an Arrow decimal would claim a
/// fidelity the data no longer has. Prefer `DECIMAL` over `MONEY` in a source schema
/// where exactness matters.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use tiberiusdelta::types::{SqlType, resolve};
///
/// assert_eq!(resolve("int", None, None).sql, SqlType::Integer);
/// assert_eq!(
///     resolve("decimal", Some(10), Some(2)).sql,
///     SqlType::Decimal { precision: 10, scale: 2 }
/// );
/// assert!(!resolve("geography", None, None).recognised);
/// ```
pub fn resolve(data_type: &str, precision: Option<i64>, scale: Option<i64>) -> ResolvedType {
    let source = data_type.to_string();
    let normalised = data_type.trim().to_ascii_lowercase();

    let sql = match normalised.as_str() {
        "smallint" => Some(SqlType::SmallInt),
        "int" => Some(SqlType::Integer),
        "bigint" => Some(SqlType::BigInt),
        "tinyint" => Some(SqlType::TinyInt),
        "real" => Some(SqlType::Real),
        "float" | "money" | "smallmoney" => Some(SqlType::Double),
        "decimal" | "numeric" => match (precision, scale) {
            (Some(p), Some(s)) => {
                decimal_fits(p, s).map(|(precision, scale)| SqlType::Decimal { precision, scale })
            }
            // The catalog always reports both for these two types; a row that somehow
            // does not is a declaration this crate cannot size, so it degrades rather
            // than guessing a precision.
            _ => None,
        },
        "bit" => Some(SqlType::Bit),
        "date" => Some(SqlType::Date),
        "time" => Some(SqlType::Time),
        "datetime" | "datetime2" | "smalldatetime" => Some(SqlType::Timestamp),
        "datetimeoffset" => Some(SqlType::TimestampOffset),
        // T-SQL's `timestamp` (alias `rowversion`) is *not* a temporal type: it is an
        // 8-byte row version, and arrives as binary. Mapping it by name alone would be a
        // silent, plausible-looking corruption.
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => Some(SqlType::Binary),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" => Some(SqlType::Text),
        // Rendered as text by crate::builders, faithfully and without loss, but named
        // here as recognised so it is not reported as an unmapped type.
        "uniqueidentifier" | "xml" => Some(SqlType::Text),
        _ => None,
    };

    match sql {
        Some(sql) => ResolvedType {
            sql,
            recognised: true,
            source,
        },
        None => ResolvedType {
            sql: SqlType::Text,
            recognised: false,
            source,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(data_type: &str) -> SqlType {
        resolve(data_type, None, None).sql
    }

    #[test]
    fn integer_family() {
        assert_eq!(sql("smallint"), SqlType::SmallInt);
        assert_eq!(sql("int"), SqlType::Integer);
        assert_eq!(sql("bigint"), SqlType::BigInt);
        assert_eq!(sql("tinyint"), SqlType::TinyInt);
    }

    #[test]
    fn float_family() {
        assert_eq!(sql("real"), SqlType::Real);
        assert_eq!(sql("float"), SqlType::Double);
    }

    /// Both money types arrive from tiberius as `f64`, so the declared Arrow type must
    /// say so rather than claim a decimal's exactness.
    #[test]
    fn money_resolves_to_double_not_decimal() {
        assert_eq!(sql("money"), SqlType::Double);
        assert_eq!(sql("smallmoney"), SqlType::Double);
        assert!(resolve("money", None, None).recognised);
    }

    #[test]
    fn decimal_within_range_carries_precision_and_scale() {
        let r = resolve("decimal", Some(10), Some(2));
        assert!(r.recognised);
        assert_eq!(
            r.sql,
            SqlType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            resolve("numeric", Some(5), Some(0)).sql,
            SqlType::Decimal {
                precision: 5,
                scale: 0
            }
        );
    }

    #[test]
    fn decimal_precision_over_38_becomes_text() {
        let r = resolve("decimal", Some(39), Some(2));
        assert!(!r.recognised, "over-wide precision must degrade, not panic");
        assert_eq!(r.sql, SqlType::Text);
    }

    #[test]
    fn decimal_precision_over_u8_max_does_not_panic_or_wrap() {
        // i64 -> u8 failing must degrade cleanly, not wrap into a small, wrong u8 (a
        // naive `as u8` cast would silently do exactly that).
        let r = resolve("decimal", Some(300), Some(2));
        assert!(!r.recognised);
        assert_eq!(r.sql, SqlType::Text);
    }

    #[test]
    fn decimal_negative_scale_becomes_text() {
        let r = resolve("decimal", Some(10), Some(-2));
        assert!(!r.recognised);
        assert_eq!(r.sql, SqlType::Text);
    }

    #[test]
    fn decimal_scale_exceeding_precision_becomes_text() {
        let r = resolve("decimal", Some(2), Some(5));
        assert!(!r.recognised);
        assert_eq!(r.sql, SqlType::Text);
    }

    #[test]
    fn decimal_without_a_reported_precision_becomes_text() {
        let r = resolve("decimal", None, None);
        assert!(!r.recognised);
        assert_eq!(r.sql, SqlType::Text);
    }

    #[test]
    fn temporal_types() {
        assert_eq!(sql("date"), SqlType::Date);
        assert_eq!(sql("time"), SqlType::Time);
        assert_eq!(sql("datetime"), SqlType::Timestamp);
        assert_eq!(sql("datetime2"), SqlType::Timestamp);
        assert_eq!(sql("smalldatetime"), SqlType::Timestamp);
        assert_eq!(sql("datetimeoffset"), SqlType::TimestampOffset);
    }

    /// T-SQL's `timestamp` is a row version, not a point in time. Getting this wrong
    /// would produce a column of plausible-looking but meaningless dates.
    #[test]
    fn tsql_timestamp_is_binary_not_temporal() {
        assert_eq!(sql("timestamp"), SqlType::Binary);
        assert_eq!(sql("rowversion"), SqlType::Binary);
    }

    #[test]
    fn text_types_are_recognised() {
        for t in ["varchar", "nvarchar", "char", "nchar", "text", "ntext"] {
            let r = resolve(t, None, None);
            assert_eq!(r.sql, SqlType::Text);
            assert!(r.recognised, "{t} should be a known textual type");
        }
    }

    #[test]
    fn binary_types() {
        assert_eq!(sql("binary"), SqlType::Binary);
        assert_eq!(sql("varbinary"), SqlType::Binary);
        assert_eq!(sql("image"), SqlType::Binary);
    }

    #[test]
    fn unknown_types_degrade_and_are_reported() {
        for t in ["geography", "geometry", "hierarchyid", "sql_variant"] {
            let r = resolve(t, None, None);
            assert_eq!(r.sql, SqlType::Text);
            assert!(!r.recognised, "{t} should be reported as unmapped");
        }
    }

    #[test]
    fn casing_and_surrounding_space_do_not_change_the_mapping() {
        assert_eq!(sql(" INT "), SqlType::Integer);
        assert_eq!(sql("NVarChar"), SqlType::Text);
    }

    /// The retained `source` is what run statistics report; it must be the catalog's own
    /// spelling, not this module's normalised form.
    #[test]
    fn source_keeps_the_catalogs_own_spelling() {
        assert_eq!(resolve("NVARCHAR", None, None).source, "NVARCHAR");
    }

    #[test]
    fn bit_maps_to_bit() {
        assert_eq!(sql("bit"), SqlType::Bit);
    }
}
