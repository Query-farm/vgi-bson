//! MongoDB **Extended JSON v2** bridge: `to_json` (BSON → canonical/relaxed
//! Extended JSON) and `from_json` (Extended JSON → BSON document bytes).
//!
//! Canonical Extended JSON is the lossless migration/diff surface — it wraps
//! every typed value (`{"$oid":…}`, `{"$numberDecimal":…}`, `{"$date":…}`,
//! `{"$timestamp":…}`, `{"$binary":…}`, `{"$minKey":1}`). Relaxed renders
//! in-range numbers and dates natively for readability, keeping wrappers only for
//! out-of-range / typed values.

use bson::{Bson, Document};

use crate::validate::DecodeError;
use crate::value::decode_document;

/// The Extended JSON rendering mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonMode {
    /// Type-preserving, lossless (`$oid`, `$numberDecimal`, `$date`, …).
    Canonical,
    /// Human-readable; native numbers/dates where in range.
    Relaxed,
}

impl JsonMode {
    /// Parse a mode string (`canonical` / `relaxed`), case-insensitively.
    pub fn parse(s: &str) -> Result<JsonMode, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "canonical" => Ok(JsonMode::Canonical),
            "relaxed" => Ok(JsonMode::Relaxed),
            other => Err(format!(
                "unknown to_json mode '{other}' (expected canonical | relaxed)"
            )),
        }
    }
}

/// Render a decoded BSON document as an Extended JSON v2 string in `mode`.
pub fn document_to_extjson(doc: Document, mode: JsonMode) -> String {
    let bson = Bson::Document(doc);
    let value = match mode {
        JsonMode::Canonical => bson.into_canonical_extjson(),
        JsonMode::Relaxed => bson.into_relaxed_extjson(),
    };
    value.to_string()
}

/// `bson.to_json(blob, mode)` — decode a BSON blob to Extended JSON v2 text.
/// `to_json` targets the self-describing Extended-JSON sink, so it succeeds on
/// any well-formed document.
pub fn to_json(bytes: &[u8], mode: JsonMode) -> Result<String, DecodeError> {
    let doc = decode_document(bytes)?;
    Ok(document_to_extjson(doc, mode))
}

/// `bson.from_json(extjson)` — parse Extended JSON v2 (canonical **or** relaxed)
/// into BSON document bytes. Round-trips `to_json('canonical')` to byte-identity
/// on canonical inputs.
pub fn from_json(text: &str) -> Result<Vec<u8>, String> {
    let json: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("invalid JSON: {e}"))?;
    // Extended JSON parsing: serde_json::Value -> Bson (interprets $oid / $date /
    // $numberDecimal / … wrappers).
    let bson = Bson::try_from(json).map_err(|e| format!("invalid Extended JSON: {e}"))?;
    let doc = match bson {
        Bson::Document(d) => d,
        other => {
            return Err(format!(
                "Extended JSON must encode a document, got {}",
                other.element_type().tag_name()
            ))
        }
    };
    let mut out = Vec::new();
    doc.to_writer(&mut out)
        .map_err(|e| format!("encode: {e}"))?;
    Ok(out)
}

/// Best-effort human label for a BSON element type (used in error text).
trait ElementTypeName {
    fn tag_name(&self) -> &'static str;
}

impl ElementTypeName for bson::spec::ElementType {
    fn tag_name(&self) -> &'static str {
        use bson::spec::ElementType::*;
        match self {
            Double => "double",
            String => "string",
            EmbeddedDocument => "object",
            Array => "array",
            Binary => "binData",
            Undefined => "undefined",
            ObjectId => "objectId",
            Boolean => "bool",
            DateTime => "date",
            Null => "null",
            RegularExpression => "regex",
            DbPointer => "dbPointer",
            JavaScriptCode => "javascript",
            Symbol => "symbol",
            JavaScriptCodeWithScope => "javascriptWithScope",
            Int32 => "int",
            Timestamp => "timestamp",
            Int64 => "long",
            Decimal128 => "decimal",
            MaxKey => "maxKey",
            MinKey => "minKey",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn enc(d: &Document) -> Vec<u8> {
        let mut v = Vec::new();
        d.to_writer(&mut v).unwrap();
        v
    }

    #[test]
    fn canonical_wraps_typed_values() {
        let b = enc(&doc! { "n": 7i64 });
        let j = to_json(&b, JsonMode::Canonical).unwrap();
        assert!(j.contains("$numberLong"), "canonical wraps int64: {j}");
    }

    #[test]
    fn relaxed_renders_native_numbers() {
        let b = enc(&doc! { "n": 7i32 });
        let j = to_json(&b, JsonMode::Relaxed).unwrap();
        assert_eq!(j, "{\"n\":7}");
    }

    #[test]
    fn canonical_roundtrips_to_bytes() {
        let b = enc(&doc! { "a": 1i32, "s": "hi", "k": 2i64 });
        let j = to_json(&b, JsonMode::Canonical).unwrap();
        let back = from_json(&j).unwrap();
        assert_eq!(
            b, back,
            "decode∘from_json∘to_json('canonical') is byte-identical"
        );
    }

    #[test]
    fn from_json_parses_oid_wrapper() {
        let j = r#"{"_id":{"$oid":"54759eb3c090d83494e2d804"}}"#;
        let bytes = from_json(j).unwrap();
        let doc = decode_document(&bytes).unwrap();
        assert_eq!(
            doc.get_object_id("_id").unwrap().to_hex(),
            "54759eb3c090d83494e2d804"
        );
    }
}
