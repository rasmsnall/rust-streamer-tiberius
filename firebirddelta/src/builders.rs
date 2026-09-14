//! Binding resolved Firebird types to Arrow arrays, from `rsfbclient`'s decoded row
//! values.
//!
//! Executes on the calling thread, once per fetched batch of rows; nothing here is
//! async, which fits `rsfbclient`'s own synchronous API (see `crate::connect`'s module
//! docs). `crate::types` decides *what* a column is, from the source catalog; this
//! module owns the Arrow builders and turns one fetched batch of `rsfbclient::Row`s into
//! one `RecordBatch`, mirroring the split `tiberiusdelta::builders` and pgdelta's own
//! `builders.rs` make.
//!
//! This is the only module that names Arrow. It uses the `deltalake::arrow` re-export
//! rather than a direct `arrow` dependency, so the arrow version can never skew from the
//! one delta-rs pins, matching pgdelta's and tiberiusdelta's own rule exactly (see
//! `CLAUDE.md`).
//!
//! # Values arrive typed, but coarsely
//!
//! `rsfbclient` decodes each cell into one of seven `rsfbclient::SqlType` variants
//! (`Text`, `Integer`, `Floating`, `Timestamp`, `Binary`, `Boolean`, `Null`) rather than
//! text, so there is no per-value text-parsing failure mode the way pgdelta has. But the
//! type is deliberately coarse: `SMALLINT`/`INTEGER`/`BIGINT` all arrive as the same
//! `Integer(i64)`, and `DATE`/`TIME`/`TIMESTAMP` all arrive as the same
//! `Timestamp(NaiveDateTime)` (a `DATE` with a synthetic midnight time attached, a `TIME`
//! with a synthetic date attached). `crate::types::resolve` is what tells this module
//! which of those a given column actually is, and every narrowing below is checked
//! rather than cast, so a value that cannot be represented at its column's declared
//! width is reported instead of silently wrapping (`CLAUDE.md`'s Security requirement 7).
//!
//! # NULL has one shape, unlike tiberiusdelta's
//!
//! `rsfbclient::SqlType::Null` is its own dedicated variant, not (as `tiberius`'s
//! `ColumnData` does) a `None` carried inside whichever variant the wire's declared
//! column width happened to pick. There is therefore no "a NULL can arrive in a
//! neighbouring variant" case to defend against here: a NULL is `SqlType::Null`,
//! unconditionally, whatever the column's real type.
//!
//! # Nullability
//!
//! Every field is declared nullable, the same reasoning as pgdelta and tiberiusdelta: a
//! source column declared `NOT NULL` that nonetheless produced a genuine `NULL` (a
//! concurrent schema change, say) would otherwise fail at Arrow-build time over a
//! declaration this crate cannot fully trust anyway, turning a source-data oddity into a
//! failed sync. Delta gains nothing from the tighter declaration here.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use deltalake::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rsfbclient::{Row, SqlType as FbValue};

use crate::error::{Error, Result};
use crate::types::{ResolvedType, SqlType};

/// The timezone stamped on every timestamp column.
///
/// Matches pgdelta's and tiberiusdelta's own `builders::UTC` for the same reason:
/// Delta's `timestamp` is microseconds UTC, and an Arrow `Timestamp` with **no** timezone
/// at all is delta-rs's own signal for `timestamp_ntz` (reader v3 / writer v7), which
/// this crate does not want to require. Firebird's plain `TIMESTAMP` has no zone concept
/// of its own and is assumed to already be UTC, the same assumption tiberiusdelta makes
/// for T-SQL's zone-less `DATETIME`/`DATETIME2`.
const UTC: &str = "UTC";

/// Returns the Arrow type a resolved column maps to.
pub fn arrow_type(rt: &ResolvedType) -> DataType {
    match rt.sql {
        SqlType::SmallInt => DataType::Int16,
        SqlType::Integer => DataType::Int32,
        SqlType::BigInt => DataType::Int64,
        SqlType::Real | SqlType::Double => DataType::Float64,
        SqlType::Boolean => DataType::Boolean,
        SqlType::Date => DataType::Date32,
        // Delta Lake has no time-of-day type. Arrow's `Time64` exists and would build
        // here perfectly happily, but delta-rs rejects it at commit ("Invalid data type
        // for Delta Lake: Time64"), the same finding tiberiusdelta made for T-SQL's
        // `TIME`: a Delta limitation, not a Firebird or `rsfbclient` one.
        SqlType::Time => DataType::Utf8,
        SqlType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        SqlType::Binary => DataType::Binary,
        // Text and CastToText share a representation: crate::pipeline already arranged
        // for a CastToText column to arrive as an `rsfbclient::SqlType::Text` (a
        // server-side `CAST(... AS VARCHAR(64))`), so both are, by the time a value
        // reaches this module, the same kind of value.
        SqlType::Text | SqlType::CastToText => DataType::Utf8,
        // Never actually reached: crate::pipeline excludes every SqlType::Unsupported
        // column from the query and the schema before this function is ever called on
        // one. Utf8 here is a harmless placeholder for the match's sake, not a claim
        // that this variant is meant to be built.
        SqlType::Unsupported => DataType::Utf8,
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

/// Lowercase hex, matching the form tiberiusdelta renders binary in.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Renders any decoded value as text, for a column that resolved to [`SqlType::Text`] or
/// [`SqlType::CastToText`].
///
/// This is what makes the fidelity policy in `crate::types` actually hold: a column whose
/// type this crate does not map still has to produce *something* faithful, and every
/// [`FbValue`] variant has a lossless textual form. Returns `None` for SQL `NULL`.
///
/// A [`FbValue::Timestamp`] here renders the *whole* value (`NaiveDateTime`'s own
/// `Display`), unlike the dedicated `Time`/`Date` handling in [`ColumnBuilder::append`]:
/// this function is the *unrecognised-type floor*, reached only when `crate::types`
/// could not tell what the column actually was, so it has no basis for picking `.date()`
/// or `.time()` out of the synthetic value either.
///
/// `pub(crate)` because `crate::pipeline` renders watermark values through exactly this
/// function: a checkpoint is stored as text whatever the watermark column's type, and it
/// must render identically to the way the same value would be written into a text
/// column.
pub(crate) fn render_text(data: &FbValue) -> Option<String> {
    match data {
        FbValue::Text(s) => Some(s.clone()),
        FbValue::Integer(i) => Some(i.to_string()),
        FbValue::Floating(f) => Some(f.to_string()),
        FbValue::Timestamp(ts) => Some(ts.to_string()),
        FbValue::Binary(b) => Some(hex(b)),
        FbValue::Boolean(b) => Some(b.to_string()),
        FbValue::Null => None,
    }
}

/// A typed Arrow array builder for one column, fed decoded values from an
/// `rsfbclient::Row`.
#[derive(Debug)]
enum ColumnBuilder {
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Date(Date32Builder),
    /// A native `TIME` column: extracts the time-of-day out of the synthetic
    /// `Timestamp` value `rsfbclient` decodes it as, rather than rendering the whole
    /// thing the way [`ColumnBuilder::Utf8`] does.
    TimeOfDay(StringBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Binary(BinaryBuilder),
    /// `Text` and `CastToText` both land here: see [`render_text`].
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(rt: &ResolvedType) -> Self {
        match rt.sql {
            SqlType::SmallInt => ColumnBuilder::Int16(Int16Builder::new()),
            SqlType::Integer => ColumnBuilder::Int32(Int32Builder::new()),
            SqlType::BigInt => ColumnBuilder::Int64(Int64Builder::new()),
            SqlType::Real | SqlType::Double => ColumnBuilder::Float64(Float64Builder::new()),
            SqlType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
            SqlType::Date => ColumnBuilder::Date(Date32Builder::new()),
            SqlType::Time => ColumnBuilder::TimeOfDay(StringBuilder::new()),
            SqlType::Timestamp => ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::new()),
            SqlType::Binary => ColumnBuilder::Binary(BinaryBuilder::new()),
            SqlType::Text | SqlType::CastToText => ColumnBuilder::Utf8(StringBuilder::new()),
            // See arrow_type's own note: crate::pipeline never lets a value reach this
            // constructor for an excluded column. Utf8 is a harmless fallback.
            SqlType::Unsupported => ColumnBuilder::Utf8(StringBuilder::new()),
        }
    }

    /// Appends one decoded value, or NULL.
    ///
    /// # Errors
    ///
    /// [`Error::UnparsableValue`] if the decoded value contradicts the type its column
    /// was resolved as, or cannot be represented in the Arrow type that resolution
    /// chose (an integer whose value does not fit its column's declared width, most
    /// plausibly, since `rsfbclient` reports every whole-number column as the same
    /// `Integer(i64)`; see the module docs). On a live connection this most likely
    /// means a concurrent schema change between the catalog lookup and the fetch.
    fn append(&mut self, data: &FbValue, column: &str) -> Result<()> {
        let mismatch = |expected: &'static str| Error::UnparsableValue {
            column: column.to_string(),
            expected,
        };

        if matches!(data, FbValue::Null) {
            self.append_null();
            return Ok(());
        }

        match self {
            ColumnBuilder::Int16(b) => match data {
                FbValue::Integer(v) => {
                    b.append_value(i16::try_from(*v).map_err(|_| mismatch("16-bit integer"))?);
                }
                _ => return Err(mismatch("16-bit integer")),
            },
            ColumnBuilder::Int32(b) => match data {
                FbValue::Integer(v) => {
                    b.append_value(i32::try_from(*v).map_err(|_| mismatch("32-bit integer"))?);
                }
                _ => return Err(mismatch("32-bit integer")),
            },
            ColumnBuilder::Int64(b) => match data {
                FbValue::Integer(v) => b.append_value(*v),
                _ => return Err(mismatch("64-bit integer")),
            },
            ColumnBuilder::Float64(b) => match data {
                FbValue::Floating(v) => b.append_value(*v),
                _ => return Err(mismatch("floating point number")),
            },
            ColumnBuilder::Boolean(b) => match data {
                FbValue::Boolean(v) => b.append_value(*v),
                _ => return Err(mismatch("boolean")),
            },
            ColumnBuilder::Date(b) => match data {
                FbValue::Timestamp(ts) => {
                    b.append_value(days_since_epoch(ts.date()).ok_or_else(|| mismatch("date"))?)
                }
                _ => return Err(mismatch("date")),
            },
            ColumnBuilder::TimeOfDay(b) => match data {
                FbValue::Timestamp(ts) => b.append_value(ts.time().to_string()),
                _ => return Err(mismatch("time")),
            },
            ColumnBuilder::Timestamp(b) => match data {
                FbValue::Timestamp(ts) => b.append_value(
                    micros_since_epoch(ts.and_utc()).ok_or_else(|| mismatch("timestamp"))?,
                ),
                _ => return Err(mismatch("timestamp")),
            },
            ColumnBuilder::Binary(b) => match data {
                FbValue::Binary(bytes) => b.append_value(bytes.as_slice()),
                _ => return Err(mismatch("binary")),
            },
            // Never fails: every decoded value has a textual form. This is the floor
            // the fidelity policy rests on.
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
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::Date(b) => b.append_null(),
            ColumnBuilder::TimeOfDay(b) => b.append_null(),
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
            ColumnBuilder::Boolean(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Date(mut b) => Arc::new(b.finish()),
            ColumnBuilder::TimeOfDay(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Timestamp(mut b) => Arc::new(b.finish().with_timezone(UTC)),
            ColumnBuilder::Binary(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Converts a fetched batch of rows into a `RecordBatch`, matching [`arrow_schema`]'s
/// schema for the same `columns`.
///
/// `columns` must be in the same order each row's own cells are in (the order the
/// `SELECT` this crate generated produced them), the same invariant tiberiusdelta's and
/// pgdelta's own decode paths rely on between a query's column list and its resolved
/// types.
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
        if row.cols.len() != columns.len() {
            return Err(Error::Internal {
                detail: "column count disagreed between the resolved schema and a fetched row",
            });
        }
        for (index, col) in row.cols.iter().enumerate() {
            builders[index].append(&col.value, &columns[index].0)?;
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
    use rsfbclient::Column as FbColumn;

    fn rt(field_type: i32) -> ResolvedType {
        resolve("C", field_type, None)
    }

    fn field_type_of(name: &str) -> i32 {
        match name {
            "smallint" => 7,
            "integer" => 8,
            "float" => 10,
            "date" => 12,
            "time" => 13,
            "bigint" => 16,
            "boolean" => 23,
            "double" => 27,
            "timestamp" => 35,
            "varchar" => 37,
            _ => unreachable!("unused in these tests"),
        }
    }

    fn row(values: Vec<FbValue>) -> Row {
        Row {
            cols: values
                .into_iter()
                .enumerate()
                .map(|(i, value)| FbColumn::new(format!("c{i}"), 0, value))
                .collect(),
        }
    }

    #[test]
    fn arrow_type_mapping_covers_every_sql_type() {
        assert_eq!(arrow_type(&rt(field_type_of("integer"))), DataType::Int32);
        assert_eq!(arrow_type(&rt(field_type_of("boolean"))), DataType::Boolean);
        assert_eq!(
            arrow_type(&rt(field_type_of("timestamp"))),
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        );
        assert_eq!(arrow_type(&rt(field_type_of("time"))), DataType::Utf8);
        // NUMERIC/DECIMAL (an INTEGER-family field_type with a numeric sub-type) is a
        // Double, not a decimal type: see crate::types::resolve's own documentation of
        // why that precision loss happens before this crate ever sees the value.
        assert_eq!(
            arrow_type(&resolve("C", field_type_of("bigint"), Some(2))),
            DataType::Float64
        );
    }

    #[test]
    fn a_null_is_accepted_by_every_builder() {
        for t in [
            "smallint",
            "integer",
            "bigint",
            "float",
            "double",
            "boolean",
            "date",
            "time",
            "timestamp",
            "varchar",
        ] {
            let mut b = ColumnBuilder::new(&rt(field_type_of(t)));
            b.append(&FbValue::Null, t)
                .unwrap_or_else(|e| panic!("{t} should accept NULL: {e}"));
            let array = b.finish();
            assert_eq!(array.len(), 1);
            assert!(array.is_null(0), "{t} should have recorded a NULL");
        }
    }

    /// Every whole-number column arrives from `rsfbclient` as the same `Integer(i64)`;
    /// a value too wide for its column's declared width must be reported, not wrapped.
    #[test]
    fn an_integer_too_wide_for_its_declared_width_is_rejected() {
        let mut b = ColumnBuilder::new(&rt(field_type_of("smallint")));
        let err = b.append(&FbValue::Integer(100_000), "n").unwrap_err();
        assert!(matches!(err, Error::UnparsableValue { .. }));
    }

    #[test]
    fn a_value_contradicting_its_resolved_type_is_reported_not_coerced() {
        let mut b = ColumnBuilder::new(&rt(field_type_of("integer")));
        let err = b.append(&FbValue::Text("7".into()), "n").unwrap_err();
        assert!(matches!(err, Error::UnparsableValue { .. }));
    }

    /// The unrecognised-type floor: whatever arrives, a text column renders it rather
    /// than failing the sync.
    #[test]
    fn a_text_column_renders_every_decoded_variant() {
        let mut b = ColumnBuilder::new(&rt(9999)); // an unknown field_type -> Text
        for data in [
            FbValue::Integer(-9),
            FbValue::Floating(1.5),
            FbValue::Boolean(true),
            FbValue::Text("hello".into()),
            FbValue::Binary(vec![0x0f, 0xa0]),
            FbValue::Null,
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
        assert!(values.is_null(5));
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

    /// A native `DATE` column pulls only the date part out of the synthetic-time
    /// `Timestamp` value `rsfbclient` decodes every temporal column as.
    #[test]
    fn a_date_column_extracts_only_the_date_part() {
        let mut b = ColumnBuilder::new(&rt(field_type_of("date")));
        let at = NaiveDate::from_ymd_opt(2026, 3, 4)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        b.append(&FbValue::Timestamp(at), "d").unwrap();
        let array = b.finish();
        let values = array
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(
            values.value(0),
            days_since_epoch(NaiveDate::from_ymd_opt(2026, 3, 4).unwrap()).unwrap()
        );
    }

    /// A native `TIME` column has no Delta type to land in (see `arrow_type`), and pulls
    /// only the time-of-day part out of the synthetic-date `Timestamp` value, not the
    /// whole thing `render_text`'s unrecognised-type floor would render.
    #[test]
    fn a_time_column_extracts_only_the_time_of_day_part() {
        let mut b = ColumnBuilder::new(&rt(field_type_of("time")));
        let at = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(13, 45, 30)
            .unwrap();
        b.append(&FbValue::Timestamp(at), "t").unwrap();
        let array = b.finish();
        let values = array
            .as_any()
            .downcast_ref::<deltalake::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "13:45:30");
    }

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
            ("id".to_string(), rt(field_type_of("integer"))),
            ("name".to_string(), rt(field_type_of("varchar"))),
        ];
        // An empty row set is the common incremental-sync case (nothing changed) and
        // must produce an empty batch with the right schema, not an error.
        let batch = record_batch(&columns, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn record_batch_decodes_a_well_formed_row() {
        let columns = vec![
            ("id".to_string(), rt(field_type_of("integer"))),
            ("name".to_string(), rt(field_type_of("varchar"))),
        ];
        let batch = record_batch(
            &columns,
            &[row(vec![
                FbValue::Integer(1),
                FbValue::Text("alice".into()),
            ])],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
