//! ObjectId helper scalars: `objectid_timestamp`, `objectid_hex`,
//! `objectid_from_hex`.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use bson_core::objectid;
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::{Result, RpcError};

use crate::arrow_io::{self, blob_bytes, text_str};

/// `objectid_timestamp(oid)` — the embedded creation time as a TIMESTAMPTZ.
/// Accepts a 24-hex VARCHAR or a 12-byte BLOB.
pub struct ObjectIdTimestamp;

impl ScalarFunction for ObjectIdTimestamp {
    fn name(&self) -> &str {
        "objectid_timestamp"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "ObjectId Embedded Timestamp",
            "ObjectId",
            "Return the creation time embedded in an ObjectId as a `TIMESTAMPTZ` — the first 4 bytes \
             of every ObjectId are the seconds since the Unix epoch. Accepts either a 24-char hex \
             `VARCHAR` or a 12-byte `BLOB`. This is the cheap \"when was this document first written\" \
             probe — no separate createdAt field needed. Returns NULL for a value that is not a \
             valid ObjectId.",
            "Creation time embedded in an ObjectId → `TIMESTAMPTZ`. Accepts 24-hex `VARCHAR` or 12-byte \
             `BLOB`.",
            "bson, objectid, timestamp, created, creation time, oid, embedded, when written",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Creation time of an ObjectId.\",\"sql\":\"SELECT bson.main.objectid_timestamp('54759eb3c090d83494e2d804') AS created\"}]".into(),
        ));
        FunctionMetadata {
            description: "Return the creation time embedded in an ObjectId (TIMESTAMPTZ)".into(),
            return_type: Some(arrow_io::ts_type()),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column(
            "oid",
            0,
            "An ObjectId, given either as its 24-character hexadecimal string or as its 12 raw \
             bytes.",
        )]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(arrow_io::ts_type()))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let is_text = matches!(col.data_type(), DataType::Utf8 | DataType::LargeUtf8);
        let rows = batch.num_rows();
        let mut out: Vec<Option<i64>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let oid = if is_text {
                objectid::parse_objectid_any(text_str(col, i)?, None)
            } else {
                objectid::parse_objectid_any(None, blob_bytes(col, i)?)
            };
            out.push(oid.map(|o| objectid::objectid_epoch_millis(&o)));
        }
        let arr = arrow_io::ts_millis_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// `objectid_hex(blob)` — 12 raw bytes → 24-char lowercase hex.
pub struct ObjectIdHex;

impl ScalarFunction for ObjectIdHex {
    fn name(&self) -> &str {
        "objectid_hex"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "ObjectId to Hex",
            "ObjectId",
            "Convert a 12-byte ObjectId `BLOB` to its 24-char lowercase hexadecimal string. Returns \
             NULL when the input is not exactly 12 bytes. The inverse of objectid_from_hex.",
            "12-byte ObjectId `BLOB` → 24-char hex `VARCHAR`.",
            "bson, objectid, hex, oid, to hex, bytes to hex, encode",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Hex of a 12-byte ObjectId.\",\"sql\":\"SELECT bson.main.objectid_hex(bson.main.objectid_from_hex('54759eb3c090d83494e2d804')) AS h\"}]".into(),
        ));
        FunctionMetadata {
            description: "Convert a 12-byte ObjectId BLOB to its 24-char hex string".into(),
            return_type: Some(DataType::Utf8),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column("oid", 0, "A 12-byte ObjectId BLOB.")]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Utf8))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(blob_bytes(col, i)?.and_then(|b| objectid::objectid_hex(b).ok()));
        }
        let arr = arrow_io::string_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// `objectid_from_hex(hex)` — 24-char hex → 12-byte BLOB.
pub struct ObjectIdFromHex;

impl ScalarFunction for ObjectIdFromHex {
    fn name(&self) -> &str {
        "objectid_from_hex"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "Parse an ObjectId Hex String to Bytes",
            "ObjectId",
            "Convert a 24-char hexadecimal ObjectId string to its 12-byte `BLOB` form. Returns NULL \
             for a string that is not a valid 24-char ObjectId. The inverse of objectid_hex.",
            "24-char hex `VARCHAR` → 12-byte ObjectId `BLOB`.",
            "bson, objectid, hex, oid, from hex, hex to bytes, decode",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"12-byte ObjectId from hex.\",\"sql\":\"SELECT to_hex(bson.main.objectid_from_hex('54759eb3c090d83494e2d804')) AS b\"}]".into(),
        ));
        FunctionMetadata {
            description: "Convert a 24-char hex ObjectId string to its 12-byte BLOB".into(),
            return_type: Some(DataType::Binary),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column_typed(
            "hex",
            0,
            DataType::Utf8,
            "A 24-char hexadecimal ObjectId string.",
        )]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Binary))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(text_str(col, i)?.and_then(|s| objectid::objectid_from_hex(s).ok()));
        }
        let arr = arrow_io::binary_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// Read a `u32`-ish integer field named `name` from a STRUCT column at `row`.
pub(crate) fn struct_u32(col: &ArrayRef, name: &str, row: usize) -> Option<u32> {
    use arrow_array::types::{Int32Type, Int64Type, UInt32Type, UInt64Type};
    if col.is_null(row) {
        return None;
    }
    let sa = col.as_struct();
    let child = sa.column_by_name(name)?;
    if child.is_null(row) {
        return None;
    }
    Some(match child.data_type() {
        DataType::UInt32 => child.as_primitive::<UInt32Type>().value(row),
        DataType::UInt64 => child.as_primitive::<UInt64Type>().value(row) as u32,
        DataType::Int32 => child.as_primitive::<Int32Type>().value(row) as u32,
        DataType::Int64 => child.as_primitive::<Int64Type>().value(row) as u32,
        _ => return None,
    })
}
