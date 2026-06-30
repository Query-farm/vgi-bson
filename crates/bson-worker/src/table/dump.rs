//! `mongodump_read(glob)` — read a glob of `.bson` files and apply the
//! `bson_seq` splitter per file, tagging each row with its source file.
//!
//! The mongodump on-ramp: point it at a backup directory, get one row per
//! document, feed `doc` to decode / to_json / field. Stateless per file; no
//! externalized scan-state (note for the proxy).
//!
//! v1 reads **local-filesystem** globs. Cloud paths (`s3://`, `http(s)://`) are a
//! roadmap item — for now, use DuckDB's own `read_blob('s3://…/*.bson')` +
//! `bson_seq(content)` for remote dumps (the worker opens no sockets of its own).

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bson_core::seq;
use vgi::table_function::{TableFunction, TableProducer};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi_rpc::{OutputCollector, Result, RpcError};

pub struct MongodumpRead;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("doc", DataType::Binary, false),
        Field::new("file", DataType::Utf8, false),
    ]))
}

impl TableFunction for MongodumpRead {
    fn name(&self) -> &str {
        "mongodump_read"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "Read mongodump .bson Files",
            "Read a glob of mongodump `.bson` files (each a concatenation of length-prefixed BSON \
             documents) and fan them into one row per document: columns idx (BIGINT, per-file \
             zero-based position), doc (BLOB, one document), and file (VARCHAR, the source path). \
             Point it at a backup directory (e.g. '/backups/mydb/*.bson') to get every document \
             joinable in SQL, then feed `doc` to decode / to_json / field. Stops cleanly at any \
             trailing partial document. v1 reads local-filesystem globs; for cloud dumps use \
             read_blob('s3://…/*.bson') + bson_seq(content). The worker opens no sockets of its \
             own.",
            "Read a glob of mongodump `.bson` files into rows of `(idx BIGINT, doc BLOB, file \
             VARCHAR)`. Local filesystem in v1.",
            "bson, mongodump, mongodb, dump, backup, .bson, read, glob, files, oplog, gridfs, \
             migration, fan-out",
        );
        tags.push((
            "vgi.result_columns_md".into(),
            "One row per document across the matched files:\n\n\
             | column | type | description |\n\
             |---|---|---|\n\
             | `idx` | BIGINT | Zero-based position within its file. |\n\
             | `doc` | BLOB | The bytes of one BSON document. |\n\
             | `file` | VARCHAR | The source `.bson` file path. |"
                .into(),
        ));
        // NOTE: no `vgi.example_queries` — `mongodump_read` always reads external
        // files, so any example returns zero rows where the files are absent
        // (VGI902). Documented usage lives in `doc_md` / the schema examples.
        FunctionMetadata {
            description: "Read a glob of mongodump `.bson` files into one row per document".into(),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "glob",
            0,
            "varchar",
            "A local-filesystem glob of `.bson` files, e.g. '/backups/mydb/*.bson'.",
        )]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema(),
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let pattern = params
            .arguments
            .const_str(0)
            .ok_or_else(|| RpcError::value_error("mongodump_read: a constant glob is required"))?;
        let files = expand_glob(&pattern)?;
        Ok(Box::new(DumpProducer {
            schema: params.output_schema.clone(),
            files,
            done: false,
        }))
    }
}

/// Expand a local-filesystem glob into a sorted list of matching paths. A literal
/// (non-glob) path is returned as-is if it exists.
fn expand_glob(pattern: &str) -> Result<Vec<String>> {
    let mut out: Vec<String> = match glob::glob(pattern) {
        Ok(paths) => paths
            .filter_map(|p| p.ok())
            .filter(|p| p.is_file())
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        Err(e) => return Err(RpcError::value_error(format!("invalid glob: {e}"))),
    };
    out.sort();
    Ok(out)
}

struct DumpProducer {
    schema: SchemaRef,
    files: Vec<String>,
    done: bool,
}

impl TableProducer for DumpProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut idx_b = Int64Builder::new();
        let mut doc_b = BinaryBuilder::new();
        let mut file_b = StringBuilder::new();
        for path in &self.files {
            let bytes = std::fs::read(path)
                .map_err(|e| RpcError::runtime_error(format!("read {path}: {e}")))?;
            for d in seq::split(&bytes) {
                idx_b.append_value(d.idx);
                doc_b.append_value(&d.doc);
                file_b.append_value(path);
            }
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(idx_b.finish()),
            Arc::new(doc_b.finish()),
            Arc::new(file_b.finish()),
        ];
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), columns)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}
