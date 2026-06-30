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
}
