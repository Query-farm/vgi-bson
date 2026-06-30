//! Safe full decode of a BSON blob into the `bson` crate's dynamic
//! [`bson::Document`] value model.
//!
//! Every entry point first runs the bounded structural [`crate::validate::precheck`]
//! so a hostile deeply-nested or length-lying blob is rejected **before** the
//! `bson` crate allocates a recursive value tree — the worker never stack-overflows
//! or OOMs on untrusted input.

use std::io::Cursor;

use bson::Document;

use crate::validate::{precheck, DecodeError};

/// Decode the first BSON document in `bytes` into a [`Document`]. Bounded and
/// total: malformed/hostile input returns [`DecodeError`], never panics.
pub fn decode_document(bytes: &[u8]) -> Result<Document, DecodeError> {
    // Bound nesting + framing before handing the bytes to the crate decoder.
    precheck(bytes)?;
    let mut cur = Cursor::new(bytes);
    Document::from_reader(&mut cur).map_err(classify)
}

/// Map a `bson` crate decode error onto the [`DecodeError`] taxonomy. The
/// structural [`precheck`] already catches the precise framing kinds; this is the
/// fallback for anything the crate rejects that the precheck tolerated.
pub fn classify(err: bson::error::Error) -> DecodeError {
    let msg = err.to_string().to_ascii_lowercase();
    if msg.contains("utf-8") || msg.contains("utf8") {
        DecodeError::BadUtf8
    } else if msg.contains("decimal") {
        DecodeError::BadDecimal128
    } else if msg.contains("eof") || msg.contains("unexpected end") {
        DecodeError::Truncated
    } else {
        DecodeError::InvalidType
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn decodes_a_document() {
        let mut bytes = Vec::new();
        doc! { "a": 1i32, "b": "x" }.to_writer(&mut bytes).unwrap();
        let d = decode_document(&bytes).unwrap();
        assert_eq!(d.get_i32("a").unwrap(), 1);
        assert_eq!(d.get_str("b").unwrap(), "x");
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode_document(&[0xff, 0x00, 0x01]).is_err());
        assert!(decode_document(&[]).is_err());
    }
}
