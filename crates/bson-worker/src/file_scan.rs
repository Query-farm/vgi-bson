//! Incremental local-file scanning shared by COPY FROM and mongodump_read.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow_array::builder::{GenericBinaryBuilder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, OffsetSizeTrait, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use bson_core::seq::{DocumentReader, StreamErrorKind};
use bson_core::validate;
use vgi::table_function::{decode_resume_state, encode_resume_state, TableProducer};
use vgi_rpc::{OutputCollector, Result, RpcError};

const BATCH_ROWS: usize = 2048;
const BATCH_BYTES: usize = 8 * 1024 * 1024;

struct OpenFile {
    path: String,
    reader: DocumentReader<BufReader<File>>,
}

struct Row {
    idx: i64,
    doc: Vec<u8>,
    file: String,
    end: u64,
}

pub(crate) struct FileScan {
    schema: SchemaRef,
    files: Vec<String>,
    // Position of the next unconsumed row, not the reader's lookahead position.
    file_index: usize,
    offset: u64,
    idx: i64,
    invalid_resume: bool,
    current: Option<OpenFile>,
    pending: Option<Row>,
    strict: bool,
    max_document_size: usize,
    with_provenance: bool,
}

impl FileScan {
    pub(crate) fn dump(schema: SchemaRef, files: Vec<String>) -> Self {
        Self::new(schema, files, false, i32::MAX as usize, true)
    }

    pub(crate) fn raw(schema: SchemaRef, path: String, max_document_size: usize) -> Result<Self> {
        local_path(&path)?;
        Ok(Self::new(
            schema,
            vec![path],
            true,
            max_document_size,
            false,
        ))
    }

    fn new(
        schema: SchemaRef,
        files: Vec<String>,
        strict: bool,
        max_document_size: usize,
        with_provenance: bool,
    ) -> Self {
        Self {
            schema,
            files,
            file_index: 0,
            offset: 0,
            idx: 0,
            invalid_resume: false,
            current: None,
            pending: None,
            strict,
            max_document_size,
            with_provenance,
        }
    }

    fn next_row(&mut self) -> Result<Option<Row>> {
        if self.invalid_resume {
            return Err(RpcError::runtime_error("invalid BSON scan continuation"));
        }
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        loop {
            if self.current.is_none() {
                let Some(path) = self.files.get(self.file_index) else {
                    return Ok(None);
                };
                let file = File::open(path)
                    .map_err(|e| RpcError::runtime_error(format!("read {path}: {e}")))?;
                let mut reader = DocumentReader::new(BufReader::new(file), self.max_document_size);
                reader
                    .seek_to(self.offset)
                    .map_err(|e| RpcError::runtime_error(format!("seek {path}: {e}")))?;
                self.current = Some(OpenFile {
                    path: path.clone(),
                    reader,
                });
            }
            let current = self.current.as_mut().expect("opened above");
            let offset = current.reader.offset();
            match current.reader.next_document() {
                Ok(Some(doc)) => {
                    if self.strict {
                        validate::validate(&doc).map_err(|e| {
                            RpcError::value_error(format!(
                                "COPY BSON: {}: byte offset {offset}: {} ({e})",
                                current.path,
                                e.kind()
                            ))
                        })?;
                    }
                    let row = Row {
                        idx: self.idx,
                        doc,
                        file: current.path.clone(),
                        end: current.reader.offset(),
                    };
                    return Ok(Some(row));
                }
                Ok(None) => self.advance_file(),
                Err(e) => {
                    // Preserve mongodump_read's historical stop-at-corruption behavior,
                    // but never suppress an actual filesystem I/O error.
                    if self.strict || matches!(e.kind, StreamErrorKind::Io(_)) {
                        return Err(RpcError::runtime_error(format!(
                            "read {}: {e}",
                            current.path
                        )));
                    }
                    self.advance_file();
                }
            }
        }
    }

    fn advance_file(&mut self) {
        self.current = None;
        self.file_index += 1;
        self.offset = 0;
        self.idx = 0;
    }

    fn batch<O: OffsetSizeTrait>(&mut self) -> Result<Option<RecordBatch>> {
        let mut docs = GenericBinaryBuilder::<O>::new();
        let mut indices = Int64Builder::new();
        let mut files = StringBuilder::new();
        let mut rows = 0;
        let mut bytes = 0;
        while rows < BATCH_ROWS && bytes < BATCH_BYTES {
            let Some(row) = self.next_row()? else {
                break;
            };
            if rows > 0 && bytes + row.doc.len() > BATCH_BYTES {
                self.pending = Some(row);
                break;
            }
            docs.append_value(&row.doc);
            self.offset = row.end;
            self.idx = row.idx + 1;
            bytes += row.doc.len();
            rows += 1;
            if self.with_provenance {
                indices.append_value(row.idx);
                files.append_value(&row.file);
            }
        }
        if rows == 0 {
            return Ok(None);
        }
        let mut columns: Vec<ArrayRef> = vec![];
        if self.with_provenance {
            columns.push(Arc::new(indices.finish()));
        }
        columns.push(Arc::new(docs.finish()));
        if self.with_provenance {
            columns.push(Arc::new(files.finish()));
        }
        RecordBatch::try_new(self.schema.clone(), columns)
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

impl TableProducer for FileScan {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if !self.with_provenance && self.schema.field(0).data_type() == &DataType::LargeBinary {
            self.batch::<i64>()
        } else {
            self.batch::<i32>()
        }
    }

    fn resume_supported(&self) -> bool {
        true
    }

    fn encode_resume(&self) -> Vec<u8> {
        // Carry the original glob expansion and next unread position. Never put
        // a potentially huge lookahead document or an OS handle in the token.
        encode_resume_state(&(&self.files, self.file_index, self.offset, self.idx))
    }

    fn restore_resume(&mut self, bytes: &[u8]) {
        self.current = None;
        self.pending = None;
        self.invalid_resume = true;
        if let Some((files, file_index, offset, idx)) =
            decode_resume_state::<(Vec<String>, usize, u64, i64)>(bytes)
        {
            if file_index <= files.len() && idx >= 0 {
                self.files = files;
                self.file_index = file_index;
                self.offset = offset;
                self.idx = idx;
                self.invalid_resume = false;
            }
        }
    }
}

pub(crate) fn local_path(path: &str) -> Result<()> {
    if path.is_empty() || path.contains("://") {
        return Err(RpcError::value_error(
            "COPY BSON requires a local filesystem path",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BinaryArray, Int64Array, LargeBinaryArray};
    use arrow_schema::{Field, Schema};

    fn raw_schema(large: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            if large {
                DataType::LargeBinary
            } else {
                DataType::Binary
            },
            true,
        )]))
    }

    #[test]
    fn streams_multiple_batches_and_matches_target_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.bson");
        let doc = [5, 0, 0, 0, 0];
        std::fs::write(&path, doc.repeat(BATCH_ROWS + 3)).unwrap();
        let schema = raw_schema(false);
        let mut scan = FileScan::raw(schema.clone(), path.to_str().unwrap().into(), 1024).unwrap();
        let first = scan.batch::<i32>().unwrap().unwrap();
        assert_eq!(first.schema(), schema);
        assert_eq!(first.num_rows(), BATCH_ROWS);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            doc
        );
        assert_eq!(scan.batch::<i32>().unwrap().unwrap().num_rows(), 3);
        assert!(scan.batch::<i32>().unwrap().is_none());
        let mut scan =
            FileScan::raw(raw_schema(true), path.to_str().unwrap().into(), 1024).unwrap();
        assert!(scan
            .batch::<i64>()
            .unwrap()
            .unwrap()
            .column(0)
            .as_any()
            .is::<LargeBinaryArray>());
    }

    #[test]
    fn strict_scan_reports_offset_while_dump_stops_and_continues_next_file() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bson");
        let b = dir.path().join("b.bson");
        std::fs::write(&a, [5, 0, 0, 0, 0, 12, 0]).unwrap();
        std::fs::write(&b, [5, 0, 0, 0, 0]).unwrap();
        let mut scan = FileScan::raw(raw_schema(false), a.to_str().unwrap().into(), 1024).unwrap();
        let err = scan.batch::<i32>().unwrap_err().to_string();
        assert!(err.contains("a.bson"), "{err}");
        assert!(err.contains("byte offset 5"), "{err}");
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("doc", DataType::Binary, false),
            Field::new("file", DataType::Utf8, false),
        ]));
        let mut scan = FileScan::dump(
            schema,
            vec![a.to_str().unwrap().into(), b.to_str().unwrap().into()],
        );
        let batch = scan.batch::<i32>().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let idx = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(idx.values().as_ref(), &[0, 0]);
    }

    #[test]
    fn byte_budget_splits_batches_without_losing_pending_documents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bson");
        let doc = bson_core::extjson::from_json(&format!(
            "{{\"s\":\"{}\"}}",
            "x".repeat(BATCH_BYTES / 2)
        ))
        .unwrap();
        std::fs::write(&path, doc.repeat(3)).unwrap();
        let mut scan = FileScan::raw(
            raw_schema(false),
            path.to_str().unwrap().into(),
            BATCH_BYTES,
        )
        .unwrap();
        for _ in 0..3 {
            let batch = scan.batch::<i32>().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0),
                doc
            );
            // HTTP rebuilds a producer between batches. Its token must point
            // before the lookahead row, not after it, and remain small.
            let token = scan.encode_resume();
            assert!(token.len() < 1024);
            scan = FileScan::raw(
                raw_schema(false),
                path.to_str().unwrap().into(),
                BATCH_BYTES,
            )
            .unwrap();
            scan.restore_resume(&token);
        }
        assert!(scan.batch::<i32>().unwrap().is_none());
    }

    #[test]
    fn continuation_preserves_file_list_and_per_file_indices() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bson");
        let b = dir.path().join("b.bson");
        std::fs::write(&a, [5, 0, 0, 0, 0].repeat(BATCH_ROWS - 1)).unwrap();
        std::fs::write(&b, [5, 0, 0, 0, 0].repeat(3)).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("doc", DataType::Binary, false),
            Field::new("file", DataType::Utf8, false),
        ]));
        let mut scan = FileScan::dump(
            schema.clone(),
            vec![a.to_str().unwrap().into(), b.to_str().unwrap().into()],
        );
        assert_eq!(scan.batch::<i32>().unwrap().unwrap().num_rows(), BATCH_ROWS);
        let token = scan.encode_resume();
        // A changed glob expansion must not change the in-flight scan's files.
        let mut resumed = FileScan::dump(schema, vec![]);
        resumed.restore_resume(&token);
        let batch = resumed.batch::<i32>().unwrap().unwrap();
        let idx = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(idx.values().as_ref(), &[1, 2]);
        let files = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(files.value(0), b.to_str().unwrap());
        assert!(resumed.batch::<i32>().unwrap().is_none());
        let token = resumed.encode_resume();
        scan.restore_resume(&token);
        assert!(scan.batch::<i32>().unwrap().is_none());
    }

    #[test]
    fn continuation_preserves_strict_error_offsets_and_rejects_bad_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.bson");
        let mut bytes = [5, 0, 0, 0, 0].repeat(BATCH_ROWS);
        bytes.extend_from_slice(&[12, 0]);
        std::fs::write(&path, bytes).unwrap();
        let mut scan =
            FileScan::raw(raw_schema(false), path.to_str().unwrap().into(), 1024).unwrap();
        assert_eq!(scan.batch::<i32>().unwrap().unwrap().num_rows(), BATCH_ROWS);
        let token = scan.encode_resume();
        let mut resumed =
            FileScan::raw(raw_schema(false), path.to_str().unwrap().into(), 1024).unwrap();
        resumed.restore_resume(&token);
        let error = resumed.batch::<i32>().unwrap_err().to_string();
        assert!(
            error.contains(&format!("byte offset {}", 5 * BATCH_ROWS)),
            "{error}"
        );
        scan.restore_resume(b"invalid");
        assert!(scan
            .batch::<i32>()
            .unwrap_err()
            .to_string()
            .contains("invalid BSON scan continuation"));
    }
}
