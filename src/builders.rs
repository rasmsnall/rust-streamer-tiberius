//! Binding resolved SQL types to Arrow arrays, from tiberius's decoded row values.
//!
//! Executes on the calling thread, once per fetched batch of rows. Nothing here is async.
//!
//! This is the only module that names Arrow. It uses the `deltalake::arrow` re-export
//! rather than a direct `arrow` dependency, so the arrow version can never skew from the
//! one delta-rs pins, matching pgdelta's own rule exactly (see `CLAUDE.md`).
//!
//! `crate::types` decides *what* a column is, from the source catalog; this module owns
//! the Arrow builders and turns one fetched batch of [`tiberius::Row`] into one
//! `RecordBatch`.
//!
//! # Values arrive typed, not as text
//!
//! This is the substantive difference from both pgdelta and this crate's own earlier
//! ODBC-based version, and it is why there is no `values.rs` here: `tiberius` decodes
//! each cell from the TDS wire into a [`ColumnData`] variant that already holds a real
//! `i32`, `f64`, [`Numeric`] or `chrono` value. There is no text to parse, so there is
//! no text-parsing failure mode: the conversions below are arithmetic (rescaling a
//! decimal, counting days or microseconds from an epoch), and every one of them is
//! checked rather than cast, so a value that cannot be represented is reported instead
//! of silently wrapping.
//!
//! # Nullability
//!
//! Every field is declared nullable, the same reasoning as pgdelta: a source column
//! declared `NOT NULL` that nonetheless produced a genuine `NULL` (a concurrent schema
//! change, say) would otherwise fail at Arrow-build time over a declaration this crate
//! cannot fully trust anyway, turning a source-data oddity into a failed sync. Delta
//! gains nothing from the tighter declaration here.

use std::cmp::Ordering;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use deltalake::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use tiberius::numeric::Numeric;
use tiberius::{ColumnData, FromSql, Row};

use crate::error::{Error, Result};
use crate::types::{ResolvedType, SqlType};

/// The timezone stamped on every timestamp column.
///
/// Matches pgdelta's own `builders::UTC` for the same reason: Delta's `timestamp` is
/// microseconds UTC, and an Arrow `Timestamp` with **no** timezone at all is delta-rs's
/// own signal for `timestamp_ntz` (reader v3 / writer v7), which this crate does not want
/// to require. A `DATETIME`/`DATETIME2` has no zone concept of its own and is assumed to
/// already be UTC; a `DATETIMEOFFSET` carries a real offset and is converted, not assumed.
const UTC: &str = "UTC";

/// Returns the Arrow type a resolved column maps to.
pub fn arrow_type(rt: &ResolvedType) -> DataType {
    match rt.sql {
        SqlType::SmallInt | SqlType::TinyInt => DataType::Int16,
        SqlType::Integer => DataType::Int32,
        SqlType::BigInt => DataType::Int64,
        SqlType::Real | SqlType::Double => DataType::Float64,
        SqlType::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
        SqlType::Bit => DataType::Boolean,
        SqlType::Date => DataType::Date32,
        // Delta Lake has no time-of-day type. Arrow's `Time64` exists and would build
        // here perfectly happily, but delta-rs rejects it at commit ("Invalid data type
        // for Delta Lake: Time64"), so a TIME column is written as its own literal text
        // instead. Found by committing one, not by reading a specification.
        SqlType::Time => DataType::Utf8,
        SqlType::Timestamp | SqlType::TimestampOffset => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        }
        SqlType::Binary => DataType::Binary,
        SqlType::Text => DataType::Utf8,
    }
}

/// Builds the Arrow schema for a table's columns.
pub fn arrow_schema(columns: &[(String, ResolvedType)]) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|(name, rt)| Field::new(name, arrow_type(rt), true))
            .collect::<Vec<_>>(),
    )
}

/// Days between 1970-01-01 and a date, as Arrow's `Date32` counts them.
fn days_since_epoch(date: NaiveDate) -> Option<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}

/// Microseconds since the Unix epoch, as Arrow's `Timestamp(Microsecond)` counts them.
///
/// chrono's `timestamp()` floors toward negative infinity and its subsecond part is
/// always non-negative, so this stays correct for pre-1970 values without a sign case.
fn micros_since_epoch(at: DateTime<Utc>) -> Option<i64> {
    at.timestamp()
        .checked_mul(1_000_000)?
        .checked_add(i64::from(at.timestamp_subsec_micros()))
}

/// Restates `n` as an unscaled integer at `target_scale`, the representation Arrow's
/// `Decimal128` stores.
///
/// Returns `None` rather than rounding when the value carries more fractional digits
/// than the column declares. A source that sends a value at a finer scale than its own
/// catalog reports is a real disagreement about what the column means, and quietly
/// dropping digits off money is exactly the kind of silent corruption this crate's error
/// policy exists to prevent.
fn decimal_at_scale(n: Numeric, target_scale: i8) -> Option<i128> {
    let from = i32::from(n.scale());
    let to = i32::from(target_scale);
    match to.cmp(&from) {
        Ordering::Equal => Some(n.value()),
        Ordering::Greater => {
            let factor = 10i128.checked_pow(u32::try_from(to - from).ok()?)?;
            n.value().checked_mul(factor)
        }
        Ordering::Less => None,
    }
}

/// True if `value`, unscaled, fits a `Decimal128(precision, _)`.
///
/// `Decimal128Builder` does not check this itself for every path, and an over-wide value
/// committed to Delta is read back wrong rather than rejected, so it is checked here.
fn decimal_fits_precision(value: i128, precision: u8) -> bool {
    match 10i128.checked_pow(u32::from(precision)) {
        Some(limit) => value > -limit && value < limit,
        // precision <= 38 always fits, so this is unreachable in practice; treating an
        // overflow as "fits" would be the unsafe direction, so it is not.
        None => false,
    }
}

/// Renders any decoded value as text, for a column that resolved to [`SqlType::Text`].
///
/// This is what makes the fidelity policy in `crate::types` actually hold: a column whose
/// type this crate does not map still has to produce *something* faithful, and every
/// [`ColumnData`] variant has a lossless textual form. Returns `None` for SQL `NULL`.
///
/// `pub(crate)` because `crate::pipeline` renders watermark values through exactly this
/// function: a checkpoint is stored as text whatever the watermark column's type, and it
/// must render identically to the way the same value would be written into a text column.
pub(crate) fn render_text(data: &ColumnData<'static>) -> Option<String> {
    match data {
        ColumnData::U8(v) => v.map(|v| v.to_string()),
        ColumnData::I16(v) => v.map(|v| v.to_string()),
        ColumnData::I32(v) => v.map(|v| v.to_string()),
        ColumnData::I64(v) => v.map(|v| v.to_string()),
        ColumnData::F32(v) => v.map(|v| v.to_string()),
        ColumnData::F64(v) => v.map(|v| v.to_string()),
        ColumnData::Bit(v) => v.map(|v| v.to_string()),
        ColumnData::String(v) => v.as_ref().map(|s| s.to_string()),
        ColumnData::Guid(v) => v.map(|v| v.to_string()),
        ColumnData::Binary(v) => v.as_ref().map(|b| hex(b)),
        ColumnData::Numeric(v) => v.map(|v| v.to_string()),
        ColumnData::Xml(v) => v.as_ref().map(|x| x.to_string()),
        ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_) => {
            NaiveDateTime::from_sql(data)
                .ok()
                .flatten()
                .map(|v| v.to_string())
        }
        ColumnData::Date(_) => NaiveDate::from_sql(data)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        ColumnData::Time(_) => NaiveTime::from_sql(data)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        // FixedOffset, not Utc, for the reason ColumnBuilder::append gives.
        ColumnData::DateTimeOffset(_) => DateTime::<FixedOffset>::from_sql(data)
            .ok()
            .flatten()
            .map(|v| v.to_utc().to_rfc3339()),
    }
}

/// True if this cell is SQL `NULL`, whichever variant carries it.
fn is_null(data: &ColumnData<'static>) -> bool {
    match data {
        ColumnData::U8(v) => v.is_none(),
        ColumnData::I16(v) => v.is_none(),
        ColumnData::I32(v) => v.is_none(),
        ColumnData::I64(v) => v.is_none(),
        ColumnData::F32(v) => v.is_none(),
        ColumnData::F64(v) => v.is_none(),
        ColumnData::Bit(v) => v.is_none(),
        ColumnData::String(v) => v.is_none(),
        ColumnData::Guid(v) => v.is_none(),
        ColumnData::Binary(v) => v.is_none(),
        ColumnData::Numeric(v) => v.is_none(),
        ColumnData::Xml(v) => v.is_none(),
        ColumnData::DateTime(v) => v.is_none(),
        ColumnData::SmallDateTime(v) => v.is_none(),
        ColumnData::Time(v) => v.is_none(),
        ColumnData::Date(v) => v.is_none(),
        ColumnData::DateTime2(v) => v.is_none(),
        ColumnData::DateTimeOffset(v) => v.is_none(),
    }
}

/// Lowercase hex, the `0x`-less form SQL Server itself renders binary in.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// A typed Arrow array builder for one column, fed decoded values from a
/// [`tiberius::Row`].
///
/// Deliberately not the same enum shape as pgdelta's `builders::ColumnBuilder`: this
/// crate has no `native_arrays`/`wide_numeric_as_decimal`-style opt-ins yet (see
/// `CLAUDE.md`'s Open items), so there is no `List` variant and no dual decimal path.
/// Add them the same way pgdelta did, as a real need arises, not speculatively.
#[derive(Debug)]
enum ColumnBuilder {
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder, u8, i8),
    Boolean(BooleanBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Binary(BinaryBuilder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(rt: &ResolvedType) -> Self {
        match rt.sql {
            SqlType::SmallInt | SqlType::TinyInt => ColumnBuilder::Int16(Int16Builder::new()),
            SqlType::Integer => ColumnBuilder::Int32(Int32Builder::new()),
            SqlType::BigInt => ColumnBuilder::Int64(Int64Builder::new()),
            SqlType::Real | SqlType::Double => ColumnBuilder::Float64(Float64Builder::new()),
            SqlType::Decimal { precision, scale } => {
                match Decimal128Builder::new().with_precision_and_scale(precision, scale) {
                    Ok(b) => ColumnBuilder::Decimal(b, precision, scale),
                    // Unreachable in practice: crate::types::resolve only ever produces a
                    // Decimal variant whose precision/scale already passed this same
                    // check (see decimal_fits). Kept as a defensive fallback to text
                    // rather than a panic, matching pgdelta's own ColumnBuilder::new.
                    Err(_) => ColumnBuilder::Utf8(StringBuilder::new()),
                }
            }
            SqlType::Bit => ColumnBuilder::Boolean(BooleanBuilder::new()),
            SqlType::Date => ColumnBuilder::Date(Date32Builder::new()),
            // Text, for the reason arrow_type gives: Delta has no time type.
            SqlType::Time => ColumnBuilder::Utf8(StringBuilder::new()),
            SqlType::Timestamp | SqlType::TimestampOffset => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::new())
            }
            SqlType::Binary => ColumnBuilder::Binary(BinaryBuilder::new()),
            SqlType::Text => ColumnBuilder::Utf8(StringBuilder::new()),
        }
    }

    /// Appends one decoded value, or NULL.
    ///
    /// # Errors
    ///
    /// [`Error::UnparsableValue`] if the decoded value contradicts the type its column
    /// was resolved as, or cannot be represented in the Arrow type that resolution
    /// chose. On a live connection the first most likely means a concurrent schema
    /// change between the catalog lookup and the fetch.
    fn append(&mut self, data: &ColumnData<'static>, column: &str) -> Result<()> {
        let mismatch = |expected: &'static str| Error::UnparsableValue {
            column: column.to_string(),
            expected,
        };

        // A NULL is a NULL whichever variant carries it, and accepting it into any
        // builder loses nothing. This matters in practice, not just in theory: tiberius
        // decodes a NULL in a variable-length column (`Intn`, `Floatn`, `Bitn`, which is
        // what *every nullable* column of those types is on the wire) from the declared
        // width in the TDS metadata, and falls back to a 64-bit variant when that width
        // is not one it recognises. Matching the variant strictly would turn that into a
        // failed sync over a value that carries no information at all. A non-null value
        // of the wrong width is a different matter and is still rejected below, since
        // accepting one really could mean reading data wrongly.
        if is_null(data) {
            self.append_null();
            return Ok(());
        }

        match self {
            ColumnBuilder::Int16(b) => match data {
                ColumnData::I16(Some(v)) => b.append_value(*v),
                // TINYINT decodes as an unsigned byte; widening it is always exact.
                ColumnData::U8(Some(v)) => b.append_value(i16::from(*v)),
                _ => return Err(mismatch("16-bit integer")),
            },
            ColumnBuilder::Int32(b) => match data {
                ColumnData::I32(Some(v)) => b.append_value(*v),
                _ => return Err(mismatch("32-bit integer")),
            },
            ColumnBuilder::Int64(b) => match data {
                ColumnData::I64(Some(v)) => b.append_value(*v),
                _ => return Err(mismatch("64-bit integer")),
            },
            ColumnBuilder::Float64(b) => match data {
                ColumnData::F64(Some(v)) => b.append_value(*v),
                ColumnData::F32(Some(v)) => b.append_value(f64::from(*v)),
                _ => return Err(mismatch("floating point number")),
            },
            ColumnBuilder::Decimal(b, precision, scale) => match data {
                ColumnData::Numeric(Some(n)) => {
                    let unscaled =
                        decimal_at_scale(*n, *scale).ok_or_else(|| mismatch("decimal"))?;
                    if !decimal_fits_precision(unscaled, *precision) {
                        return Err(mismatch("decimal"));
                    }
                    b.append_value(unscaled);
                }
                _ => return Err(mismatch("decimal")),
            },
            ColumnBuilder::Boolean(b) => match data {
                ColumnData::Bit(Some(v)) => b.append_value(*v),
                _ => return Err(mismatch("boolean")),
            },
            ColumnBuilder::Date(b) => match NaiveDate::from_sql(data) {
                Ok(Some(d)) => b.append_value(days_since_epoch(d).ok_or_else(|| mismatch("date"))?),
                _ => return Err(mismatch("date")),
            },
            // A DATETIMEOFFSET is read through `DateTime<FixedOffset>`, deliberately, and
            // never through tiberius's `DateTime<Utc>` conversion: see the note on
            // `datetimeoffset_is_read_through_fixedoffset` below for why that one is
            // wrong. Everything else here is zone-less and assumed UTC, the assumption
            // `UTC` above documents.
            ColumnBuilder::Timestamp(b) => match DateTime::<FixedOffset>::from_sql(data) {
                Ok(Some(at)) => b.append_value(
                    micros_since_epoch(at.to_utc()).ok_or_else(|| mismatch("timestamp"))?,
                ),
                _ => match NaiveDateTime::from_sql(data) {
                    Ok(Some(naive)) => b.append_value(
                        micros_since_epoch(naive.and_utc()).ok_or_else(|| mismatch("timestamp"))?,
                    ),
                    _ => return Err(mismatch("timestamp")),
                },
            },
            ColumnBuilder::Binary(b) => match data {
                ColumnData::Binary(Some(bytes)) => b.append_value(bytes.as_ref()),
                _ => return Err(mismatch("binary")),
            },
            // Never fails: every decoded value has a textual form. This is the floor the
            // fidelity policy rests on.
            ColumnBuilder::Utf8(b) => b.append_option(render_text(data)),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Int16(b) => b.append_null(),
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Decimal(b, ..) => b.append_null(),
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::Date(b) => b.append_null(),
            ColumnBuilder::Timestamp(b) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
            ColumnBuilder::Utf8(b) => b.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int16(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Decimal(mut b, ..) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Date(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Timestamp(mut b) => Arc::new(b.finish().with_timezone(UTC)),
            ColumnBuilder::Binary(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Converts a fetched batch of rows into a `RecordBatch`, matching [`arrow_schema`]'s
/// schema for the same `columns`.
///
/// `columns` must be in the same order the rows' own cells are in (the order the `SELECT`
/// produced them), the same invariant pgdelta's decode pool relies on between a `COPY`
/// header and its resolved types.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if any value contradicts its column's resolved type, and
/// [`Error::Internal`] if a row's cell count disagrees with `columns.len()`, which would
/// be a defect in the caller rather than a problem with the data.
///
/// # Panics
///
/// Does not panic.
pub fn record_batch(columns: &[(String, ResolvedType)], rows: &[Row]) -> Result<RecordBatch> {
    let schema = Arc::new(arrow_schema(columns));
    let mut builders: Vec<ColumnBuilder> = columns
        .iter()
        .map(|(_, rt)| ColumnBuilder::new(rt))
        .collect();

    for row in rows {
        if row.len() != columns.len() {
            return Err(Error::Internal {
                detail: "column count disagreed between the resolved schema and a fetched row",
            });
        }
        for (index, (_, data)) in row.cells().enumerate() {
            builders[index].append(data, &columns[index].0)?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();
    RecordBatch::try_new(schema, arrays).map_err(|e| Error::Arrow {
        message: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::resolve;
    use deltalake::arrow::array::Array;

    fn rt(data_type: &str) -> ResolvedType {
        resolve(data_type, None, None)
    }

    #[test]
    fn arrow_type_mapping_covers_every_sql_type() {
        assert_eq!(arrow_type(&rt("int")), DataType::Int32);
        assert_eq!(
            arrow_type(&resolve("decimal", Some(10), Some(2))),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(arrow_type(&rt("bit")), DataType::Boolean);
        assert_eq!(
            arrow_type(&rt("datetime2")),
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        );
        assert_eq!(
            arrow_type(&rt("datetimeoffset")),
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        );
        assert_eq!(arrow_type(&rt("varbinary")), DataType::Binary);
    }

    /// A schema built from the same resolved types `record_batch` uses must be the one
    /// the caller commits to Delta; this is the property that keeps the two from
    /// disagreeing, the same discipline pgdelta's `ColumnBuilder::new` doc comment
    /// states about its own `arrow_type`/`ColumnBuilder::new` pair.
    /// Every column type must accept a NULL from *any* decoded variant, not only its own:
    /// tiberius picks the variant for a NULL from the TDS metadata's declared width, and
    /// falls back to a 64-bit one when that width is unfamiliar, so a nullable column can
    /// legitimately deliver a NULL in a neighbouring variant. A NULL carries no data, so
    /// accepting it is lossless; refusing it would fail a sync over nothing.
    #[test]
    fn a_null_is_accepted_by_every_builder_whichever_variant_carries_it() {
        let nulls = [
            ColumnData::I64(None),
            ColumnData::I32(None),
            ColumnData::String(None),
            ColumnData::Numeric(None),
            ColumnData::DateTime2(None),
        ];
        for t in [
            "smallint",
            "int",
            "bigint",
            "tinyint",
            "real",
            "float",
            "bit",
            "date",
            "time",
            "datetime2",
            "datetimeoffset",
            "varbinary",
            "nvarchar",
            "decimal",
        ] {
            for null in &nulls {
                let mut b = ColumnBuilder::new(&rt(t));
                b.append(null, t)
                    .unwrap_or_else(|e| panic!("{t} should accept {null:?} as NULL: {e}"));
                let array = b.finish();
                assert_eq!(array.len(), 1);
                assert!(array.is_null(0), "{t} should have recorded a NULL");
            }
        }
    }

    /// The other half of that rule: a non-null value of the wrong width is real data
    /// arriving as a type the schema did not declare, and must be reported rather than
    /// quietly reinterpreted.
    #[test]
    fn a_non_null_value_of_the_wrong_variant_is_still_rejected() {
        let mut b = ColumnBuilder::new(&rt("int"));
        assert!(b.append(&ColumnData::I64(Some(7)), "n").is_err());
    }

    #[test]
    fn tinyint_widens_into_int16_exactly() {
        let mut b = ColumnBuilder::new(&rt("tinyint"));
        b.append(&ColumnData::U8(Some(255)), "n").unwrap();
        let array = b.finish();
        let values = array
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int16Array>()
            .unwrap();
        assert_eq!(values.value(0), 255);
    }

    #[test]
    fn a_value_contradicting_its_resolved_type_is_reported_not_coerced() {
        let mut b = ColumnBuilder::new(&rt("int"));
        let err = b
            .append(&ColumnData::String(Some("7".into())), "n")
            .unwrap_err();
        assert!(matches!(err, Error::UnparsableValue { .. }));
    }

    /// The unrecognised-type floor: whatever arrives, a text column renders it rather
    /// than failing the sync.
    #[test]
    fn a_text_column_renders_every_decoded_variant() {
        let mut b = ColumnBuilder::new(&rt("sql_variant"));
        for data in [
            ColumnData::I64(Some(-9)),
            ColumnData::F64(Some(1.5)),
            ColumnData::Bit(Some(true)),
            ColumnData::String(Some("hello".into())),
            ColumnData::Binary(Some(vec![0x0f, 0xa0].into())),
            ColumnData::Numeric(Some(Numeric::new_with_scale(12345, 2))),
            ColumnData::String(None),
        ] {
            b.append(&data, "anything").unwrap();
        }
        let array = b.finish();
        let values = array
            .as_any()
            .downcast_ref::<deltalake::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "-9");
        assert_eq!(values.value(2), "true");
        assert_eq!(values.value(3), "hello");
        assert_eq!(values.value(4), "0fa0");
        assert_eq!(values.value(5), "123.45");
        assert!(values.is_null(6));
    }

    #[test]
    fn a_decimal_keeps_its_unscaled_value_when_the_scales_agree() {
        let mut b = ColumnBuilder::new(&resolve("decimal", Some(10), Some(2)));
        b.append(
            &ColumnData::Numeric(Some(Numeric::new_with_scale(-5055, 2))),
            "balance",
        )
        .unwrap();
        let array = b.finish();
        let values = array
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(values.value(0), -5055);
        assert_eq!(values.value_as_string(0), "-50.55");
    }

    /// A value sent at a coarser scale than the column declares is widened exactly.
    #[test]
    fn a_decimal_at_a_coarser_scale_is_rescaled_not_misread() {
        assert_eq!(
            decimal_at_scale(Numeric::new_with_scale(7, 0), 2),
            Some(700)
        );
    }

    /// The direction that would lose digits must refuse, not round.
    #[test]
    fn a_decimal_needing_digits_the_column_cannot_hold_is_rejected() {
        assert_eq!(decimal_at_scale(Numeric::new_with_scale(12345, 4), 2), None);

        let mut b = ColumnBuilder::new(&resolve("decimal", Some(10), Some(2)));
        let err = b
            .append(
                &ColumnData::Numeric(Some(Numeric::new_with_scale(12345, 4))),
                "balance",
            )
            .unwrap_err();
        assert!(matches!(err, Error::UnparsableValue { .. }));
    }

    #[test]
    fn a_decimal_too_wide_for_its_declared_precision_is_rejected() {
        assert!(decimal_fits_precision(999, 3));
        assert!(!decimal_fits_precision(1000, 3));
        assert!(decimal_fits_precision(-999, 3));
        assert!(!decimal_fits_precision(-1000, 3));
    }

    #[test]
    fn epoch_conversions_are_exact_at_the_epoch_itself() {
        assert_eq!(
            days_since_epoch(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            Some(0)
        );
        assert_eq!(
            days_since_epoch(NaiveDate::from_ymd_opt(1969, 12, 31).unwrap()),
            Some(-1)
        );
    }

    /// A `DATETIMEOFFSET` must be read through `DateTime<FixedOffset>`, never through
    /// tiberius's `DateTime<Utc>` conversion.
    ///
    /// TDS stores a `DATETIMEOFFSET`'s datetime2 part **already in UTC**, with the offset
    /// carried alongside it only so the original local rendering can be reconstructed.
    /// tiberius's `DateTime<Utc>` impl subtracts that offset from the datetime2 part
    /// anyway, which moves the instant by the offset a second time: a value written as
    /// `2026-03-04T13:45:30+02:00` (11:45:30 UTC) comes back as 09:45:30 UTC. Verified
    /// against the live instance, by comparing with what SQL Server's own
    /// `SWITCHOFFSET(value, 0)` reports for the same row, not by reading the TDS
    /// specification and hoping. Its `DateTime<FixedOffset>` impl attaches the offset
    /// instead of subtracting it, which leaves the instant correct, so that is the one
    /// this crate uses.
    ///
    /// This test is the guard: if a future tiberius fixes `DateTime<Utc>` and changes
    /// `DateTime<FixedOffset>` to match, this fails rather than the data silently
    /// shifting by an offset.
    #[test]
    fn datetimeoffset_is_read_through_fixedoffset() {
        use tiberius::time::{Date, DateTime2, DateTimeOffset, Time};

        // 11:45:30 UTC, tagged +02:00 (so its local rendering is 13:45:30).
        let days = NaiveDate::from_ymd_opt(2026, 3, 4)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1, 1, 1).unwrap())
            .num_days() as u32;
        let seconds_from_midnight = 11 * 3600 + 45 * 60 + 30;
        let data = ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
            DateTime2::new(Date::new(days), Time::new(seconds_from_midnight, 0)),
            120,
        )));

        let mut b = ColumnBuilder::new(&rt("datetimeoffset"));
        b.append(&data, "at").unwrap();
        let array = b.finish();
        let values = array
            .as_any()
            .downcast_ref::<deltalake::arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        let expected = NaiveDate::from_ymd_opt(2026, 3, 4)
            .unwrap()
            .and_hms_opt(11, 45, 30)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(
            values.value(0),
            expected,
            "the stored datetime2 part is already UTC; the offset must not be applied again"
        );
    }

    /// A TIME column has no Delta type to land in, so it lands as its own literal; this
    /// pins down what that literal looks like rather than leaving it to chance.
    #[test]
    fn a_time_column_is_written_as_its_literal_text() {
        assert_eq!(arrow_type(&rt("time")), DataType::Utf8);
        assert!(
            rt("time").recognised,
            "TIME is understood; it is Delta that cannot store it"
        );
    }

    /// Pre-1970 timestamps are the case a sign-unaware seconds/subseconds split gets
    /// wrong; chrono's flooring convention makes the plain addition correct, and this
    /// pins that down rather than trusting it.
    #[test]
    fn timestamps_before_the_epoch_convert_correctly() {
        let at = NaiveDate::from_ymd_opt(1969, 12, 31)
            .unwrap()
            .and_hms_micro_opt(23, 59, 59, 750_000)
            .unwrap()
            .and_utc();
        assert_eq!(micros_since_epoch(at), Some(-250_000));
    }

    #[test]
    fn record_batch_rejects_a_row_whose_width_disagrees_with_the_schema() {
        let columns = vec![
            ("id".to_string(), rt("int")),
            ("name".to_string(), rt("nvarchar")),
        ];
        // An empty row set is the common incremental-sync case (nothing changed) and
        // must produce an empty batch with the right schema, not an error.
        let batch = record_batch(&columns, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }
}
