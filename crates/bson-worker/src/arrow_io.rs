//! Arrow input/output helpers shared across the scalar and table functions:
//! reading a BLOB/VARCHAR input cell and small nullable column builders, plus the
//! shared `well_formed` STRUCT type.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, ListBuilder, StringBuilder, TimestampMicrosecondBuilder,
    UInt32Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use vgi_rpc::{Result, RpcError};

/// `TIMESTAMPTZ` — microsecond UTC timestamp.
pub fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// Borrow the raw bytes of a BLOB/VARCHAR input cell at `row`, or `None` if null.
pub fn blob_bytes(col: &ArrayRef, row: usize) -> Result<Option<&[u8]>> {
    if col.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match col.data_type() {
        DataType::Binary => col.as_binary::<i32>().value(row),
        DataType::LargeBinary => col.as_binary::<i64>().value(row),
        DataType::Utf8 => col.as_string::<i32>().value(row).as_bytes(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row).as_bytes(),
        other => {
            return Err(RpcError::value_error(format!(
                "expected a BLOB or VARCHAR argument, got {other:?}"
            )))
        }
    }))
}

/// Borrow the UTF-8 text of a VARCHAR input cell at `row`, or `None` if null.
pub fn text_str(col: &ArrayRef, row: usize) -> Result<Option<&str>> {
    if col.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match col.data_type() {
        DataType::Utf8 => col.as_string::<i32>().value(row),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row),
        other => {
            return Err(RpcError::value_error(format!(
                "expected a VARCHAR argument, got {other:?}"
            )))
        }
    }))
}

/// Build a nullable VARCHAR column from per-row optional strings.
pub fn string_array(col: &[Option<String>]) -> ArrayRef {
    let mut b = StringBuilder::new();
    for v in col {
        match v {
            Some(s) => b.append_value(s),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

/// Build a nullable BLOB column from per-row optional byte vectors.
pub fn binary_array(col: &[Option<Vec<u8>>]) -> ArrayRef {
    let mut b = BinaryBuilder::new();
    for v in col {
        match v {
            Some(bytes) => b.append_value(bytes),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

/// Build a nullable BOOLEAN column.
pub fn bool_opt_array(col: &[Option<bool>]) -> ArrayRef {
    let mut b = BooleanBuilder::new();
    for v in col {
        match v {
            Some(x) => b.append_value(*x),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

/// Build a nullable `TIMESTAMPTZ` column from per-row epoch milliseconds.
pub fn ts_millis_array(col: &[Option<i64>]) -> ArrayRef {
    let mut b = TimestampMicrosecondBuilder::new();
    for v in col {
        match v {
            Some(ms) => b.append_value(ms.saturating_mul(1_000)),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().with_timezone("UTC"))
}

/// Build a nullable `LIST<VARCHAR>` column from per-row optional string lists.
pub fn list_string_array(col: &[Option<Vec<String>>]) -> ArrayRef {
    let mut b = ListBuilder::new(StringBuilder::new());
    for v in col {
        match v {
            Some(items) => {
                for it in items {
                    b.values().append_value(it);
                }
                b.append(true);
            }
            None => b.append(false),
        }
    }
    Arc::new(b.finish())
}

/// The `well_formed` STRUCT type: `STRUCT(ok BOOL, error VARCHAR, kind VARCHAR)`.
pub fn well_formed_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("ok", DataType::Boolean, true),
        Field::new("error", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
    ]))
}

/// Assemble a `well_formed` STRUCT array from per-row component columns. A `None`
/// row (NULL input blob) is a NULL struct.
pub fn well_formed_array(
    ok: &[Option<bool>],
    error: &[Option<String>],
    kind: &[Option<String>],
    valid: Vec<bool>,
) -> ArrayRef {
    let DataType::Struct(fields) = well_formed_type() else {
        unreachable!()
    };
    let arrays = vec![bool_opt_array(ok), string_array(error), string_array(kind)];
    Arc::new(StructArray::new(
        fields,
        arrays,
        Some(NullBuffer::from(valid)),
    ))
}

/// The `timestamp_parts` STRUCT type: `STRUCT(t UINTEGER, i UINTEGER)`.
pub fn timestamp_parts_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("t", DataType::UInt32, true),
        Field::new("i", DataType::UInt32, true),
    ]))
}

/// Build a `timestamp_parts` STRUCT array; a `None` row is a NULL struct.
pub fn timestamp_parts_array(t: &[Option<u32>], i: &[Option<u32>], valid: Vec<bool>) -> ArrayRef {
    let DataType::Struct(fields) = timestamp_parts_type() else {
        unreachable!()
    };
    let mut tb = UInt32Builder::new();
    let mut ib = UInt32Builder::new();
    for (tv, iv) in t.iter().zip(i.iter()) {
        match tv {
            Some(x) => tb.append_value(*x),
            None => tb.append_null(),
        }
        match iv {
            Some(x) => ib.append_value(*x),
            None => ib.append_null(),
        }
    }
    Arc::new(StructArray::new(
        fields,
        vec![Arc::new(tb.finish()), Arc::new(ib.finish())],
        Some(NullBuffer::from(valid)),
    ))
}
