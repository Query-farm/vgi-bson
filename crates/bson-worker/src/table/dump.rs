//! `mongodump_read(glob)` — read a glob of `.bson` files and apply the
//! `bson_seq` splitter per file, tagging each row with its source file.
//!
//! The mongodump on-ramp: point it at a backup directory, get one row per
//! document, feed `doc` to decode / to_json / field. HTTP continuations carry
//! the original file list and next document offset.
//!
//! v1 reads **local-filesystem** globs. Cloud paths (`s3://`, `http(s)://`) are a
//! roadmap item — for now, load one remote file with DuckDB's own `read_blob`
//! into a variable and split it with `bson_seq(getvariable(…))` (the worker opens
//! no sockets of its own; `bson_seq` takes a constant, not a LATERAL column).

use std::sync::Arc;

use crate::file_scan::FileScan;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi_rpc::{Result, RpcError};

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
            "Streams",
            "Read a glob of mongodump `.bson` files (each a concatenation of length-prefixed BSON \
             documents) and fan them into one row per document: columns idx (`BIGINT`, per-file \
             zero-based position), doc (`BLOB`, one document), and file (`VARCHAR`, the source path). \
             Point it at a backup directory (e.g. '/backups/mydb/*.bson') to get every document \
             joinable in SQL, then feed `doc` to decode / to_json / field. Stops cleanly at any \
             trailing partial document. v1 reads local-filesystem globs; for a cloud dump, store \
             one file's read_blob content in a DuckDB variable and split it with bson_seq over \
             getvariable. The worker opens no sockets of its own.",
            "Read a glob of mongodump `.bson` files into rows of `(idx BIGINT, doc BLOB, file \
             VARCHAR)`. Local filesystem in v1.",
            "bson, mongodump, mongodb, dump, backup, .bson, read, glob, files, oplog, gridfs, \
             migration, fan-out",
        );
        tags.push((
            "vgi.result_columns_schema".into(),
            r#"[
  {"name": "idx", "type": "BIGINT", "description": "Zero-based position of the document within its own source file."},
  {"name": "doc", "type": "BLOB", "description": "The raw bytes of one framed BSON document, ready to feed to decode / to_json / field."},
  {"name": "file", "type": "VARCHAR", "description": "Path of the source .bson file the document was read from."}
]"#
                .into(),
        ));
        // mongodump_read only reads the worker's local filesystem, so a
        // repo-relative fixture path would return no rows for anyone outside
        // this checkout. The example is therefore a guaranteed-runnable
        // executable example: it writes a three-document file with
        // `COPY ... TO (FORMAT 'bson.bson')` and reads it straight back.
        tags.push((
            "vgi.executable_examples".into(),
            r#"[
  {
    "name": "copy_then_mongodump_read",
    "description": "Write three documents to a mongodump-style .bson file with COPY ... TO, then read the file back with mongodump_read: one row per document with its zero-based idx, rendered as relaxed Extended JSON.",
    "sql": [
      {
        "description": "Export three documents built with from_json to a raw .bson file (the COPY reports 3 rows).",
        "sql": "COPY (SELECT bson.main.from_json(j) AS doc FROM (VALUES (1, '{\"name\":\"alice\",\"age\":30}'), (2, '{\"name\":\"bob\",\"age\":25}'), (3, '{\"name\":\"carol\",\"age\":41}')) AS v(n, j) ORDER BY n) TO '/tmp/vgi_bson_example.bson' (FORMAT 'bson.bson')"
      },
      {
        "description": "Read the file back with mongodump_read: alice, bob and carol at idx 0, 1 and 2.",
        "sql": "SELECT idx, bson.main.to_json(doc, 'relaxed') AS doc_json FROM bson.main.mongodump_read('/tmp/vgi_bson_example.bson') ORDER BY idx"
      }
    ]
  }
]"#
            .into(),
        ));
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
        Ok(Box::new(FileScan::dump(
            params.output_schema.clone(),
            files,
        )))
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
