//! BSON internal-Timestamp (`0x11`) helper scalars: `timestamp_to_ts` and
//! `timestamp_parts`. A BSON Timestamp decodes to `STRUCT(t UINTEGER, i UINTEGER)`
//! (the replication clock — NOT a wall-clock time), so both helpers take that
//! decoded struct shape as input.

use arrow_array::{ArrayRef, RecordBatch};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::{Result, RpcError};

use crate::arrow_io;
use crate::scalar::objectid::struct_u32;

/// The fixed STRUCT(t, i) input type a BSON timestamp decodes to.
fn ts_input_spec() -> ArgSpec {
    ArgSpec::any_column(
        "ts",
        0,
        "A BSON internal replication-clock value in its decoded (t, i) form — the (seconds, \
         increment) pair.",
    )
}

/// `timestamp_to_ts(ts)` — the `t` (seconds) component as a TIMESTAMPTZ.
pub struct TimestampToTs;

impl ScalarFunction for TimestampToTs {
    fn name(&self) -> &str {
        "timestamp_to_ts"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Timestamp → TIMESTAMPTZ",
            "Timestamp",
            "Convert a BSON internal Timestamp (the decoded `STRUCT(t, i)` replication clock) to a \
             second-resolution `TIMESTAMPTZ` from its `t` (seconds-since-epoch) component — the \
             oplog ordering clock. NOTE this is the MongoDB-internal 0x11 Timestamp, NOT the \
             user-facing 0x09 UTCDateTime: a 0x11 value is a replication marker, not a wall-clock \
             time. Returns NULL when the input struct has no integer `t`.",
            "BSON Timestamp `STRUCT(t,i)` → `TIMESTAMPTZ` from the `t` seconds (the oplog clock).",
            "bson, timestamp, oplog, replication, ts, to_ts, clock, 0x11, increment",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Convert a Timestamp struct to a TIMESTAMPTZ.\",\"sql\":\"SELECT bson.main.timestamp_to_ts({'t': 1416994483, 'i': 1}) AS ts\"}]".into(),
        ));
        FunctionMetadata {
            description: "Convert a BSON Timestamp STRUCT(t,i) to a TIMESTAMPTZ (from `t`)".into(),
            return_type: Some(arrow_io::ts_type()),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ts_input_spec()]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(arrow_io::ts_type()))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<i64>> = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(struct_u32(col, "t", i).map(|t| (t as i64) * 1_000));
        }
        let arr = arrow_io::ts_millis_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// `timestamp_parts(ts)` — the raw `(time, increment)` pair as STRUCT(t,i).
pub struct TimestampParts;

impl ScalarFunction for TimestampParts {
    fn name(&self) -> &str {
        "timestamp_parts"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Timestamp Parts",
            "Timestamp",
            "Normalize a BSON internal Timestamp to `STRUCT(t UINTEGER, i UINTEGER)` — the raw \
             (time-in-seconds, increment) pair of the 0x11 replication clock. `t` orders ops to \
             the second; `i` breaks ties within the same second. Use timestamp_to_ts for a \
             `TIMESTAMPTZ` from `t`. Returns a NULL struct for a NULL input.",
            "BSON Timestamp → `STRUCT(t UINTEGER, i UINTEGER)`: the raw (seconds, increment) pair.",
            "bson, timestamp, parts, increment, time, oplog, replication, 0x11, t, i",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Read the (t, i) parts of a Timestamp.\",\"sql\":\"SELECT (bson.main.timestamp_parts({'t': 1416994483, 'i': 2})).i AS inc\"}]".into(),
        ));
        FunctionMetadata {
            description: "Return the raw (time, increment) pair of a BSON Timestamp as STRUCT(t,i)"
                .into(),
            return_type: Some(arrow_io::timestamp_parts_type()),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ts_input_spec()]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(arrow_io::timestamp_parts_type()))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut t = Vec::with_capacity(rows);
        let mut inc = Vec::with_capacity(rows);
        let mut valid = Vec::with_capacity(rows);
        for i in 0..rows {
            if col.is_null(i) {
                t.push(None);
                inc.push(None);
                valid.push(false);
            } else {
                t.push(struct_u32(col, "t", i));
                inc.push(struct_u32(col, "i", i));
                valid.push(true);
            }
        }
        let arr: ArrayRef = arrow_io::timestamp_parts_array(&t, &inc, valid);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}
