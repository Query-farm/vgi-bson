//! `bson_seq` — split a `BLOB` holding **N concatenated length-prefixed BSON
//! documents** (each `int32 length ++ body ++ 0x00`) into one slice per
//! document. This is exactly the wire shape of a `mongodump` `.bson` file body,
//! an oplog batch, or a GridFS reassembly.
//!
//! Splitting stops cleanly at the first malformed length prefix: partial trailing
//! bytes are ignored (the documents parsed so far are still returned). It never
//! panics — the bounded [`crate::validate::validate_one`] walks each document's
//! framing without allocating a value tree.

use crate::validate::validate_one;
use crate::validate::DecodeError;
use std::io::{self, Read};

/// A framing or I/O failure at a zero-based document offset in a BSON stream.
#[derive(Debug)]
pub struct StreamError {
    /// Byte offset of the document that could not be read.
    pub offset: u64,
    /// Structural failure, size limit, or underlying I/O error.
    pub kind: StreamErrorKind,
}

/// Why an incremental BSON read failed.
#[derive(Debug)]
pub enum StreamErrorKind {
    /// Invalid or incomplete BSON document.
    Invalid(DecodeError),
    /// The declared document size exceeds the caller's limit.
    TooLarge {
        /// Declared document size, including its length prefix.
        length: usize,
        /// Maximum accepted document size.
        limit: usize,
    },
    /// Underlying reader failure.
    Io(io::Error),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "byte offset {}: ", self.offset)?;
        match &self.kind {
            StreamErrorKind::Invalid(e) => write!(f, "{} ({e})", e.kind()),
            StreamErrorKind::TooLarge { length, limit } => {
                write!(
                    f,
                    "document size {length} exceeds max_document_size {limit}"
                )
            }
            StreamErrorKind::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for StreamError {}

/// Incrementally frame and validate concatenated BSON documents from a reader.
///
/// Holds only the current document. Never reserves memory based on an untrusted
/// length prefix: allocation grows with bytes actually read, up to the supplied
/// document limit. EOF is successful only between documents. After any error the
/// reader is exhausted; callers cannot accidentally resume at an unknown boundary.
pub struct DocumentReader<R> {
    reader: R,
    offset: u64,
    max_document_size: usize,
    done: bool,
}

impl<R: Read> DocumentReader<R> {
    /// Construct a reader with an explicit maximum document size in bytes.
    pub fn new(reader: R, max_document_size: usize) -> Self {
        Self {
            reader,
            offset: 0,
            max_document_size,
            done: false,
        }
    }

    /// Zero-based byte offset at which the next document begins.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Read one complete validated document, preserving its original bytes.
    /// Like [`split`], framing permits duplicate keys; a strict importer may
    /// additionally apply [`crate::validate::validate`].
    pub fn next_document(&mut self) -> Result<Option<Vec<u8>>, StreamError> {
        if self.done {
            return Ok(None);
        }
        let result = self.read_document();
        match result {
            Ok(Some(doc)) => {
                self.offset += doc.len() as u64;
                Ok(Some(doc))
            }
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(kind) => {
                self.done = true;
                Err(StreamError {
                    offset: self.offset,
                    kind,
                })
            }
        }
    }

    fn read_document(&mut self) -> Result<Option<Vec<u8>>, StreamErrorKind> {
        let mut prefix = [0; 4];
        match self.reader.read_exact(&mut prefix[..1]) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(StreamErrorKind::Io(e)),
        }
        self.reader
            .read_exact(&mut prefix[1..])
            .map_err(read_error)?;
        let length = i32::from_le_bytes(prefix);
        if length < 5 {
            return Err(StreamErrorKind::Invalid(DecodeError::LengthMismatch));
        }
        let length = length as usize;
        if length > self.max_document_size {
            return Err(StreamErrorKind::TooLarge {
                length,
                limit: self.max_document_size,
            });
        }
        let mut doc = prefix.to_vec();
        let mut chunk = [0; 8192];
        while doc.len() < length {
            let n = chunk.len().min(length - doc.len());
            self.reader
                .read_exact(&mut chunk[..n])
                .map_err(read_error)?;
            doc.extend_from_slice(&chunk[..n]);
        }
        validate_one(&doc, &mut 0).map_err(StreamErrorKind::Invalid)?;
        Ok(Some(doc))
    }
}

fn read_error(e: io::Error) -> StreamErrorKind {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        StreamErrorKind::Invalid(DecodeError::Truncated)
    } else {
        StreamErrorKind::Io(e)
    }
}

/// One document carved out of a concatenated stream.
#[derive(Debug, Clone)]
pub struct SeqDoc {
    /// Zero-based position in the stream.
    pub idx: i64,
    /// The raw bytes of this single BSON document.
    pub doc: Vec<u8>,
}

/// Split `stream` into its constituent BSON documents. Stops at the first byte
/// offset that does not begin a well-framed document.
pub fn split(stream: &[u8]) -> Vec<SeqDoc> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut idx = 0i64;
    while pos < stream.len() {
        let start = pos;
        match validate_one(stream, &mut pos) {
            Ok(()) => {
                // Guard against a zero-advance loop on a pathological input.
                if pos <= start {
                    break;
                }
                out.push(SeqDoc {
                    idx,
                    doc: stream[start..pos].to_vec(),
                });
                idx += 1;
            }
            Err(_) => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn enc(d: &bson::Document) -> Vec<u8> {
        let mut v = Vec::new();
        d.to_writer(&mut v).unwrap();
        v
    }

    #[test]
    fn splits_three_docs() {
        let mut stream = Vec::new();
        stream.extend(enc(&doc! { "i": 0i32 }));
        stream.extend(enc(&doc! { "i": 1i32 }));
        stream.extend(enc(&doc! { "i": 2i32 }));
        let docs = split(&stream);
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[2].idx, 2);
    }

    #[test]
    fn trailing_partial_is_dropped() {
        let mut stream = enc(&doc! { "ok": true });
        stream.extend_from_slice(&[0x10, 0x00, 0x00]); // partial trailing prefix
        let docs = split(&stream);
        assert_eq!(
            docs.len(),
            1,
            "the complete doc is returned, the residue dropped"
        );
    }

    #[test]
    fn empty_stream_yields_nothing() {
        assert!(split(&[]).is_empty());
    }

    #[test]
    fn incremental_reader_handles_short_reads_and_preserves_bytes() {
        struct ShortReads<'a>(&'a [u8]);
        impl Read for ShortReads<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let n = buf.len().min(1);
                self.0.read(&mut buf[..n])
            }
        }
        let documents = [
            enc(&doc! {"n": 1i32}),
            enc(&doc! {"s": "hello", "a": [2, 3]}),
        ];
        let stream = documents.concat();
        let mut reader = DocumentReader::new(ShortReads(&stream), 1024);
        for doc in &documents {
            assert_eq!(reader.next_document().unwrap().as_ref(), Some(doc));
        }
        assert_eq!(reader.offset(), stream.len() as u64);
        assert!(reader.next_document().unwrap().is_none());
    }

    #[test]
    fn incremental_reader_reports_every_truncation_at_document_start() {
        let first = enc(&doc! {});
        let second = enc(&doc! {"x": "hello"});
        for length in 1..second.len() {
            let bytes = [first.as_slice(), &second[..length]].concat();
            let mut reader = DocumentReader::new(bytes.as_slice(), 1024);
            assert_eq!(reader.next_document().unwrap(), Some(first.clone()));
            let err = reader.next_document().unwrap_err();
            assert_eq!(err.offset, 5);
            assert!(matches!(
                err.kind,
                StreamErrorKind::Invalid(DecodeError::Truncated)
            ));
            assert!(reader.next_document().unwrap().is_none());
        }
    }

    #[test]
    fn incremental_reader_rejects_corruption_limits_and_lying_prefixes() {
        for length in [i32::MIN, -1, 0, 4] {
            let bytes = length.to_le_bytes();
            let mut reader = DocumentReader::new(bytes.as_slice(), 1024);
            assert!(matches!(
                reader.next_document().unwrap_err().kind,
                StreamErrorKind::Invalid(DecodeError::LengthMismatch)
            ));
        }
        let bytes = i32::MAX.to_le_bytes();
        let mut reader = DocumentReader::new(bytes.as_slice(), 1024);
        assert!(matches!(
            reader.next_document().unwrap_err().kind,
            StreamErrorKind::TooLarge { .. }
        ));
        // Even an explicitly large limit must not allocate from a lying prefix.
        let mut reader = DocumentReader::new(bytes.as_slice(), i32::MAX as usize);
        assert!(matches!(
            reader.next_document().unwrap_err().kind,
            StreamErrorKind::Invalid(DecodeError::Truncated)
        ));
        let bytes = [5, 0, 0, 0, 1];
        assert!(DocumentReader::new(bytes.as_slice(), 1024)
            .next_document()
            .is_err());
    }

    #[test]
    fn incremental_reader_propagates_io_errors() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("broken reader"))
            }
        }
        let mut reader = DocumentReader::new(Broken, 1024);
        assert!(matches!(
            reader.next_document().unwrap_err().kind,
            StreamErrorKind::Io(_)
        ));
    }
}
