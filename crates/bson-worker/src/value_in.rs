//! Convert an Arrow input cell into a BSON [`bson::Bson`] for the `encode` path.
//!
//! STRUCT → embedded document, MAP → document, LIST → array, TIMESTAMP/TIMESTAMPTZ
//! → `0x09` UTCDateTime, UUID (FixedSizeBinary(16)) → Binary subtype `0x04`, BLOB
//! → Binary subtype `0x00`, integers → Int32/Int64 by range, DOUBLE → Double,
//! DECIMAL → Decimal128. The top-level value must be a document (BSON has no
//! top-level scalar).

use std::str::FromStr;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, TimeUnit};
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Decimal128, Document};
use vgi_rpc::{Result, RpcError};

fn rt(e: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(e.to_string())
}

/// Read element `row` of `array` as a BSON value.
pub fn value_at(array: &ArrayRef, row: usize) -> Result<Bson> {
    if array.is_null(row) {
        return Ok(Bson::Null);
    }
    Ok(match array.data_type() {
        DataType::Null => Bson::Null,
        DataType::Boolean => Bson::Boolean(array.as_boolean().value(row)),
        DataType::Int8 => Bson::Int32(array.as_primitive::<Int8Type>().value(row) as i32),
        DataType::Int16 => Bson::Int32(array.as_primitive::<Int16Type>().value(row) as i32),
        DataType::Int32 => Bson::Int32(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Bson::Int64(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Bson::Int32(array.as_primitive::<UInt8Type>().value(row) as i32),
        DataType::UInt16 => Bson::Int32(array.as_primitive::<UInt16Type>().value(row) as i32),
        DataType::UInt32 => Bson::Int64(array.as_primitive::<UInt32Type>().value(row) as i64),
        DataType::UInt64 => int_from_u64(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => Bson::Double(array.as_primitive::<Float32Type>().value(row) as f64),
        DataType::Float64 => Bson::Double(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, scale) => {
            let raw = array.as_primitive::<Decimal128Type>().value(row);
            decimal128_from_scaled(raw, *scale)
        }
        DataType::Utf8 => Bson::String(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Bson::String(array.as_string::<i64>().value(row).to_string()),
        DataType::Binary => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: array.as_binary::<i32>().value(row).to_vec(),
        }),
        DataType::LargeBinary => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: array.as_binary::<i64>().value(row).to_vec(),
        }),
        DataType::FixedSizeBinary(16) => {
            // DuckDB UUID arrives as a 16-byte fixed binary.
            let fb = array.as_fixed_size_binary();
            Bson::Binary(Binary {
                subtype: BinarySubtype::Uuid,
                bytes: fb.value(row).to_vec(),
            })
        }
        DataType::FixedSizeBinary(_) => {
            let fb = array.as_fixed_size_binary();
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: fb.value(row).to_vec(),
            })
        }
        DataType::Timestamp(unit, _) => {
            Bson::DateTime(DateTime::from_millis(timestamp_millis(array, row, *unit)))
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let items = list.value(row);
            let mut out = Vec::with_capacity(items.len());
            for i in 0..items.len() {
                out.push(value_at(&items, i)?);
            }
            Bson::Array(out)
        }
        DataType::Struct(fields) => {
            let sa = array.as_struct();
            let mut doc = Document::new();
            for (i, f) in fields.iter().enumerate() {
                doc.insert(f.name().clone(), value_at(sa.column(i), row)?);
            }
            Bson::Document(doc)
        }
        DataType::Map(_, _) => {
            let ma = array.as_map();
            let entries = ma.value(row);
            let keys = entries.column(0);
            let vals = entries.column(1);
            let mut doc = Document::new();
            for i in 0..entries.len() {
                let key = map_key(keys, i)?;
                doc.insert(key, value_at(vals, i)?);
            }
            Bson::Document(doc)
        }
        other => return Err(rt(format!("encode: unsupported input type {other:?}"))),
    })
}

/// Encode element `row` of `array` as a top-level BSON document's bytes. Errors
/// if the value is not a document-shaped value (STRUCT / MAP).
pub fn encode_document(array: &ArrayRef, row: usize) -> Result<Option<Vec<u8>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = value_at(array, row)?;
    let doc = match value {
        Bson::Document(d) => d,
        _ => return Err(rt(
            "encode: the top-level value must be a STRUCT or MAP (BSON has no top-level scalar)",
        )),
    };
    let mut out = Vec::new();
    doc.to_writer(&mut out).map_err(rt)?;
    Ok(Some(out))
}

fn int_from_u64(v: u64) -> Bson {
    if let Ok(i) = i32::try_from(v) {
        Bson::Int32(i)
    } else if let Ok(i) = i64::try_from(v) {
        Bson::Int64(i)
    } else {
        // Beyond i64 — fall back to a double (lossy on the very top of the range).
        Bson::Double(v as f64)
    }
}

/// Build a BSON Decimal128 from a scaled i128 mantissa and decimal scale by
/// formatting the exact decimal string and parsing it (lossless within 38 digits).
fn decimal128_from_scaled(raw: i128, scale: i8) -> Bson {
    let s = format_decimal(raw, scale);
    match Decimal128::from_str(&s) {
        Ok(d) => Bson::Decimal128(d),
        // Should not happen for an in-range DECIMAL; degrade to a double.
        Err(_) => Bson::Double(raw as f64 / 10f64.powi(scale as i32)),
    }
}

/// Format a scaled integer `raw` with `scale` fractional digits as a decimal
/// string, e.g. `(12345, 2) -> "123.45"`, `(−5, 0) -> "-5"`.
fn format_decimal(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        // Negative scale means trailing zeros; rare for DuckDB, handle simply.
        let mut s = raw.to_string();
        for _ in 0..(-scale) {
            s.push('0');
        }
        return s;
    }
    let neg = raw < 0;
    let digits = raw.unsigned_abs().to_string();
    let scale = scale as usize;
    let s = if digits.len() <= scale {
        let pad = "0".repeat(scale - digits.len());
        format!("0.{pad}{digits}")
    } else {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    if neg {
        format!("-{s}")
    } else {
        s
    }
}

fn map_key(array: &ArrayRef, row: usize) -> Result<String> {
    Ok(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        _ => match value_at(array, row)? {
            Bson::String(s) => s,
            other => other.to_string(),
        },
    })
}

fn timestamp_millis(array: &ArrayRef, row: usize, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => array
            .as_primitive::<TimestampSecondType>()
            .value(row)
            .saturating_mul(1_000),
        TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
        TimeUnit::Microsecond => {
            array.as_primitive::<TimestampMicrosecondType>().value(row) / 1_000
        }
        TimeUnit::Nanosecond => {
            array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000_000
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_decimal_cases() {
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(-5, 0), "-5");
        assert_eq!(format_decimal(5, 4), "0.0005");
        assert_eq!(format_decimal(-12345, 4), "-1.2345");
    }
}
