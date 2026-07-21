//! `bson_seq(stream)` — table function over a BLOB holding N concatenated
//! length-prefixed BSON documents (the wire shape of a `mongodump` `.bson` file
//! body, an oplog batch, or a GridFS reassembly). Fans one blob into one row per
//! document.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, Int64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bson_core::seq;
use vgi::table_function::{TableFunction, TableProducer};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi_rpc::{OutputCollector, Result, RpcError};

pub struct BsonSeq;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("doc", DataType::Binary, false),
    ]))
}

impl TableFunction for BsonSeq {
    fn name(&self) -> &str {
        "bson_seq"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Sequence Split",
            "Streams",
            "Split a `BLOB` holding N concatenated length-prefixed BSON documents (each is `int32 \
             length ++ body ++ 0x00`) into one row per document: columns idx (`BIGINT`, zero-based \
             position) and doc (`BLOB`, the single document's bytes). This is exactly the wire shape \
             of a mongodump `.bson` file body, an oplog batch, or a GridFS reassembly. Stops \
             cleanly at the first malformed length prefix — partial trailing bytes are ignored \
             (never panics). Feed `doc` to decode / to_json / field. Use as a LATERAL table \
             function over a column of concatenated-BSON blobs.",
            "LATERAL: fan a concatenated length-prefixed BSON stream into rows of `(idx BIGINT, \
             doc BLOB)`.",
            "bson, sequence, bson_seq, mongodump, oplog, gridfs, split, fan-out, lateral, stream, \
             concatenated, documents",
        );
        tags.push((
            "vgi.result_columns_schema".into(),
            r#"[
  {"name": "idx", "type": "BIGINT", "description": "Zero-based position of the document within the concatenated stream."},
  {"name": "doc", "type": "BLOB", "description": "The raw bytes of one framed BSON document, ready to feed to decode / to_json / field."}
]"#
                .into(),
        ));
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Split two concatenated empty documents.\",\"sql\":\"SELECT idx, octet_length(doc) AS n FROM bson.main.bson_seq(from_hex('05000000000500000000')) ORDER BY idx\"}]".into(),
        ));
        FunctionMetadata {
            description:
                "Split a concatenated length-prefixed BSON stream into one row per document".into(),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "stream",
            0,
            "blob",
            "N concatenated length-prefixed BSON documents — the wire body of a mongodump `.bson` \
             file, an oplog batch, or a GridFS reassembly.",
        )]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema(),
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let docs = params
            .arguments
            .const_bytes(0)
            .map(|b| seq::split(&b))
            .unwrap_or_default();
        Ok(Box::new(SeqProducer {
            schema: params.output_schema.clone(),
            docs: Some(docs),
        }))
    }
}

struct SeqProducer {
    schema: SchemaRef,
    docs: Option<Vec<seq::SeqDoc>>,
}

impl TableProducer for SeqProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        let Some(docs) = self.docs.take() else {
            return Ok(None);
        };
        let mut idx = Int64Builder::new();
        let mut doc = BinaryBuilder::new();
        for d in &docs {
            idx.append_value(d.idx);
            doc.append_value(&d.doc);
        }
        let columns: Vec<ArrayRef> = vec![Arc::new(idx.finish()), Arc::new(doc.finish())];
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), columns)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}
