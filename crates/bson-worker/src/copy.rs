//! Strict, byte-preserving COPY of concatenated BSON documents.

use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use bson_core::validate;
use vgi::arguments::Arguments;
use vgi::copy_from::{CopyFromFunction, CopyFromReadContext, CopyFromTable};
use vgi::copy_to::{CopyToCloseContext, CopyToFunction, CopyToWriteContext};
use vgi::storage::FunctionStorage;
use vgi::table_function::{TableFunction, TableProducer};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, Worker};
use vgi_rpc::{OutputCollector, Result, RpcError};

use crate::file_scan::{local_path, FileScan};

const DEFAULT_MAX_DOCUMENT_SIZE: usize = 16 * 1024 * 1024;
const SHARD_BYTES: usize = 8 * 1024 * 1024;
const MANIFEST: &[u8] = b"bson-copy-manifest";

pub(crate) fn register(worker: &mut Worker) {
    // vgi 0.51's COPY adapter does not forward producer continuation state.
    // Register the resumable factory first: equal-signature overloads resolve
    // in registration order. Keep the SDK registration for format discovery,
    // and hide the internal handler's overloads from direct SQL invocation.
    worker.register_table(ResumableCopyFrom);
    worker.register_copy_from(BsonCopy);
    worker.register_copy_to(BsonCopy);
    worker.hide_function("copy_bson");
}

struct BsonCopy;

struct ResumableCopyFrom;

impl TableFunction for ResumableCopyFrom {
    fn name(&self) -> &str {
        "copy_bson"
    }
    fn metadata(&self) -> FunctionMetadata {
        metadata()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        specs()
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let mut response = CopyFromTable(Arc::new(BsonCopy)).on_bind(params)?;
        raw_schema(&response.output_schema)?;
        options(&params.arguments)?;
        // The SDK also omits copy_from when reconstructing an HTTP producer.
        // Bind state is carried through every continuation, so keep the path
        // there. The adapter above has already checked that COPY supplied it.
        let cf = params.copy_from.as_ref().expect("validated by adapter");
        local_path(&cf.file_path)?;
        response.opaque_data = cf.file_path.as_bytes().to_vec();
        Ok(response)
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let path = std::str::from_utf8(&params.bind_opaque_data)
            .map_err(|_| RpcError::value_error("COPY BSON: invalid bound path"))?;
        raw_schema(&params.output_schema)?;
        Ok(Box::new(FileScan::raw(
            params.output_schema.clone(),
            path.into(),
            options(&params.arguments)?,
        )?))
    }
}

fn metadata() -> FunctionMetadata {
    let description = "Import or export raw BSON documents using COPY with FORMAT 'bson.bson' \
        (replace the first bson with your attached catalog name). Requires exactly one `BLOB` \
        column, one complete BSON document per row. MODE defaults to 'raw'; typed rows are not \
        supported. Local files only; reads stream in bounded batches. Invalid documents, \
        duplicate keys, truncated files, NULL rows, and oversized documents fail the COPY. \
        MAX_DOCUMENT_SIZE defaults to 16777216 bytes (16 MiB), and may be raised up to \
        2147483647. Bytes are preserved without decoding/re-encoding. Export preserves source \
        order (use ORDER BY for deterministic ordering), stages batches in shared storage, and \
        atomically replaces the destination only after successful writing. Empty export creates \
        a zero-byte file.";
    let markdown = format!(
        "{description}\n\n\
        ### Import and export\n\n\
        Create the destination table in a writable DuckDB schema, then import a local \
        mongodump document file. Qualify the format by the catalog's ATTACH alias.\n\n\
        ```sql\nCOPY <target_table> FROM '<input_path>' (FORMAT '<catalog>.bson');\n\
        COPY (<query_returning_one_blob_column>) TO '<output_path>'\n\
        (FORMAT '<catalog>.bson', MODE 'raw');\n```\n\n\
        A bad import aborts the statement and reports its filename and document byte offset. \
        A bad export reports its source row and leaves an existing destination intact. \
        COPY accepts one uncompressed file; cloud paths, globs, collection metadata and \
        mongodump archive containers are outside this format. Paths are resolved on the \
        worker's filesystem. MAX_DOCUMENT_SIZE can be raised explicitly for larger documents."
    );
    let mut tags = crate::meta::object_tags(
        "BSON COPY",
        "Streams",
        description,
        &markdown,
        "bson, copy, copy from, copy to, import, export, mongodump, raw, lossless, bulk, files",
    );
    tags.push((
        "vgi.result_dynamic_columns_md".into(),
        "COPY FROM fills the target table's single `BLOB` column, using its name. COPY TO writes \
         the source's single `BLOB` column. DuckDB reports the copied row count. \
         For a target table `docs(doc BLOB)`, the imported columns are:\n\n\
         | Name | Type | Description |\n\
         | --- | --- | --- |\n\
         | doc | BLOB | One complete validated BSON document, with its original bytes. |\n\n\
         The column name follows the target schema; the type must be `BLOB`. Invoke through \
         COPY, not as a table function."
            .into(),
    ));
    FunctionMetadata {
        description: "Read and write raw BSON files with COPY".into(),
        tags,
        ..Default::default()
    }
}

fn specs() -> Vec<ArgSpec> {
    vec![
        ArgSpec::column("mode", -1, "varchar", "Document representation: 'raw' (default), exactly one BLOB column.")
            .with_default("raw"),
        ArgSpec::column("max_document_size", -1, "int64", "Maximum bytes per BSON document, including its prefix; default 16 MiB. Applies to import and export.")
            .with_ge(5.0).with_le(i32::MAX as f64).with_default(DEFAULT_MAX_DOCUMENT_SIZE as i64),
    ]
}

fn options(args: &Arguments) -> Result<usize> {
    let mode = args.named_str("mode").unwrap_or_else(|| "raw".into());
    if mode != "raw" {
        return Err(RpcError::value_error("COPY BSON: mode must be 'raw'"));
    }
    let max = args
        .named_i64("max_document_size")
        .unwrap_or(DEFAULT_MAX_DOCUMENT_SIZE as i64);
    if !(5..=i32::MAX as i64).contains(&max) {
        return Err(RpcError::value_error(
            "COPY BSON: max_document_size must be between 5 and 2147483647",
        ));
    }
    Ok(max as usize)
}

fn raw_schema(schema: &Schema) -> Result<()> {
    if schema.fields().len() != 1
        || !matches!(
            schema.field(0).data_type(),
            DataType::Binary | DataType::LargeBinary
        )
    {
        return Err(RpcError::value_error(
            "COPY BSON raw mode requires exactly one BLOB column",
        ));
    }
    Ok(())
}

impl CopyFromFunction for BsonCopy {
    fn format(&self) -> &str {
        "bson"
    }
    fn handler_name(&self) -> &str {
        "copy_bson"
    }
    fn comment(&self) -> Option<String> {
        Some("Byte-preserving BSON file import and export".into())
    }
    fn metadata(&self) -> FunctionMetadata {
        metadata()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        specs()
    }

    fn read(
        &self,
        _ctx: &CopyFromReadContext,
        _out: &mut OutputCollector,
    ) -> Result<Vec<RecordBatch>> {
        // The SDK always uses read_stream when it returns a producer. Do not
        // accidentally provide a whole-file buffering fallback.
        Err(RpcError::runtime_error(
            "COPY BSON requires the streaming reader",
        ))
    }

    fn read_stream(&self, ctx: &CopyFromReadContext) -> Result<Option<Box<dyn TableProducer>>> {
        raw_schema(ctx.expected_schema)?;
        let max = options(ctx.options)?;
        Ok(Some(Box::new(FileScan::raw(
            ctx.expected_schema.clone(),
            ctx.path.into(),
            max,
        )?)))
    }
}

impl CopyToFunction for BsonCopy {
    fn format(&self) -> &str {
        "bson"
    }
    // VGI's bidirectional format carries one handler name for both adapters.
    fn handler_name(&self) -> &str {
        "copy_bson"
    }
    fn comment(&self) -> Option<String> {
        Some("Byte-preserving BSON file import and export".into())
    }
    fn metadata(&self) -> FunctionMetadata {
        metadata()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        specs()
    }
    fn ordered(&self) -> bool {
        true
    }

    fn write(&self, ctx: &CopyToWriteContext, batch: &RecordBatch) -> Result<()> {
        local_path(ctx.path)?;
        stage_batch(
            ctx.storage.as_ref(),
            ctx.execution_id,
            batch,
            options(ctx.options)?,
        )
    }

    fn close(&self, ctx: &CopyToCloseContext) -> Result<i64> {
        options(ctx.options)?;
        let schema = ctx
            .input_schema
            .ok_or_else(|| RpcError::runtime_error("COPY BSON: missing source schema"))?;
        raw_schema(schema)?;
        finish(ctx.storage.as_ref(), ctx.execution_id, ctx.path)
    }
}

// The ordered sink serializes write calls. State lives in execution-scoped
// shared storage, never on the handler: close may execute in another process.
// A manifest lets close detect missing shards instead of silently truncating.
#[derive(Default)]
struct Manifest {
    shards: u64,
    rows: i64,
}

impl Manifest {
    fn load(store: &dyn FunctionStorage, scope: &[u8]) -> Result<Self> {
        let Some(bytes) = store.kv_get(scope, MANIFEST) else {
            if store.kv_get(scope, &shard_key(0)).is_some() {
                return Err(storage_error());
            }
            return Ok(Self::default());
        };
        let bytes: [u8; 16] = bytes.try_into().map_err(|_| storage_error())?;
        let state = Self {
            shards: u64::from_le_bytes(bytes[..8].try_into().map_err(|_| storage_error())?),
            rows: i64::from_le_bytes(bytes[8..].try_into().map_err(|_| storage_error())?),
        };
        if state.rows < 0 || (state.shards == 0) != (state.rows == 0) {
            return Err(storage_error());
        }
        Ok(state)
    }

    fn save(&self, store: &dyn FunctionStorage, scope: &[u8]) -> Result<()> {
        let bytes = [self.shards.to_le_bytes(), self.rows.to_le_bytes()].concat();
        put_checked(store, scope, MANIFEST, &bytes)
    }
}

fn storage_error() -> RpcError {
    RpcError::runtime_error("COPY BSON: missing or invalid staged data in shared storage")
}

fn shard_key(index: u64) -> Vec<u8> {
    [b"bson-copy-shard-".as_slice(), &index.to_le_bytes()].concat()
}

// SDK storage writes are best-effort; verify them so failed persistence fails
// the export rather than reporting a successful, incomplete file.
fn put_checked(store: &dyn FunctionStorage, scope: &[u8], key: &[u8], bytes: &[u8]) -> Result<()> {
    store.kv_put(scope, key, bytes);
    if store.kv_get(scope, key).as_deref() != Some(bytes) {
        return Err(storage_error());
    }
    Ok(())
}

fn stage_batch(
    store: &dyn FunctionStorage,
    scope: &[u8],
    batch: &RecordBatch,
    max: usize,
) -> Result<()> {
    raw_schema(&batch.schema())?;
    let mut state = Manifest::load(store, scope)?;
    let first_row = state.rows;
    let mut shard = Vec::new();
    let mut rows = 0i64;
    for row in 0..batch.num_rows() {
        let ordinal = first_row + row as i64;
        let doc = crate::arrow_io::blob_bytes(batch.column(0), row)?.ok_or_else(|| {
            RpcError::value_error(format!("COPY BSON: row {ordinal}: NULL document"))
        })?;
        if doc.len() > max {
            return Err(RpcError::value_error(format!(
                "COPY BSON: row {ordinal}: document size {} exceeds max_document_size {max}",
                doc.len()
            )));
        }
        validate::validate(doc).map_err(|e| {
            RpcError::value_error(format!("COPY BSON: row {ordinal}: {} ({e})", e.kind()))
        })?;
        if rows > 0 && shard.len() + doc.len() > SHARD_BYTES {
            stage_shard(store, scope, &mut state, &shard, rows)?;
            shard.clear();
            rows = 0;
        }
        shard.extend_from_slice(doc);
        rows += 1;
    }
    if rows > 0 {
        stage_shard(store, scope, &mut state, &shard, rows)?;
    }
    state.save(store, scope)
}

fn stage_shard(
    store: &dyn FunctionStorage,
    scope: &[u8],
    state: &mut Manifest,
    bytes: &[u8],
    rows: i64,
) -> Result<()> {
    let value = [rows.to_le_bytes().as_slice(), bytes].concat();
    put_checked(store, scope, &shard_key(state.shards), &value)?;
    state.shards += 1;
    state.rows += rows;
    Ok(())
}

fn finish(store: &dyn FunctionStorage, scope: &[u8], path: &str) -> Result<i64> {
    local_path(path)?;
    let state = Manifest::load(store, scope)?;
    let destination = Path::new(path);
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let io_error = |e| RpcError::runtime_error(format!("COPY BSON: write {path}: {e}"));
    // Same-directory temporary file keeps publication atomic. Failure drops it
    // and leaves an existing destination intact, including on empty exports.
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
    let mut rows = 0;
    {
        let mut writer = BufWriter::new(temp.as_file_mut());
        for index in 0..state.shards {
            let shard = store
                .kv_get(scope, &shard_key(index))
                .ok_or_else(storage_error)?;
            let count = shard.get(..8).ok_or_else(storage_error)?;
            let count = i64::from_le_bytes(count.try_into().map_err(|_| storage_error())?);
            if count <= 0 {
                return Err(storage_error());
            }
            rows += count;
            writer.write_all(&shard[8..]).map_err(io_error)?;
        }
        if rows != state.rows {
            return Err(storage_error());
        }
        writer.flush().map_err(io_error)?;
    }
    temp.as_file().sync_all().map_err(io_error)?;
    temp.persist(destination).map_err(|e| io_error(e.error))?;
    // The SDK releases execution-scoped storage on buffering cleanup.
    Ok(rows)
}

#[cfg(test)]
mod tests;
