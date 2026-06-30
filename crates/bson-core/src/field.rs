//! `bson.field(doc, path[, as])` — extract one field by dotted path, optionally
//! coercing the leaf to a typed scalar string.
//!
//! Path syntax is dotted with numeric array indices: `'o._id'`, `'items.0.sku'`.
//! A missing path yields `None` (SQL `NULL`). The coercion modes return **bare,
//! cast-ready** strings (`objectid` → 24-hex, `decimal` → the canonical decimal
//! literal, `uuid` → canonical UUID, `datetime`/`timestamp` → RFC 3339), so a
//! caller can write `bson.field(doc,'amount','decimal')::DECIMAL(38,4)`.

use bson::spec::BinarySubtype;
use bson::Bson;

use crate::objectid;
use crate::validate::DecodeError;
use crate::value::decode_document;

/// The typed coercion requested via the `as` argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coerce {
    /// §A inference applied to the leaf (default).
    Infer,
    /// ObjectId → 24-char lowercase hex.
    ObjectId,
    /// Decimal128 / number → canonical decimal string.
    Decimal,
    /// Binary subtype 0x04 → canonical UUID string.
    Uuid,
    /// BSON Timestamp `(t,i)` → RFC 3339 of the `t` seconds.
    Timestamp,
    /// UTCDateTime → RFC 3339.
    DateTime,
    /// Bytes → lowercase hex.
    Blob,
    /// Canonical Extended JSON of the leaf.
    Json,
}

impl Coerce {
    /// Parse the `as` mode, case-insensitively. An empty / NULL value is [`Coerce::Infer`].
    pub fn parse(s: &str) -> Result<Coerce, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Coerce::Infer),
            "objectid" => Ok(Coerce::ObjectId),
            "decimal" => Ok(Coerce::Decimal),
            "uuid" => Ok(Coerce::Uuid),
            "timestamp" => Ok(Coerce::Timestamp),
            "datetime" => Ok(Coerce::DateTime),
            "blob" => Ok(Coerce::Blob),
            "json" => Ok(Coerce::Json),
            other => Err(format!(
                "unknown field coercion '{other}' (expected objectid | decimal | uuid | \
                 timestamp | datetime | blob | json)"
            )),
        }
    }
}

/// Extract `path` from the BSON blob, coercing per `mode`. Missing path → `None`.
pub fn field(bytes: &[u8], path: &str, mode: Coerce) -> Result<Option<String>, DecodeError> {
    let doc = decode_document(bytes)?;
    let root = Bson::Document(doc);
    Ok(navigate(&root, path).map(|leaf| coerce(leaf, mode)))
}

/// Walk the dotted `path` from `root`, returning the leaf value if present.
fn navigate<'a>(root: &'a Bson, path: &str) -> Option<&'a Bson> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = match cur {
            Bson::Document(d) => d.get(seg)?,
            Bson::Array(a) => {
                let idx: usize = seg.parse().ok()?;
                a.get(idx)?
            }
            _ => return None,
        };
    }
    Some(cur)
}

/// Render the leaf per the requested coercion.
fn coerce(leaf: &Bson, mode: Coerce) -> String {
    match mode {
        Coerce::Json => Bson::clone(leaf).into_canonical_extjson().to_string(),
        Coerce::ObjectId => match leaf {
            Bson::ObjectId(oid) => oid.to_hex(),
            other => infer(other),
        },
        Coerce::Decimal => match leaf {
            Bson::Decimal128(d) => d.to_string(),
            Bson::Double(f) => f.to_string(),
            Bson::Int32(i) => i.to_string(),
            Bson::Int64(i) => i.to_string(),
            other => infer(other),
        },
        Coerce::Uuid => match leaf {
            Bson::Binary(b) if b.subtype == BinarySubtype::Uuid => uuid_string(&b.bytes),
            other => infer(other),
        },
        Coerce::Timestamp => match leaf {
            Bson::Timestamp(ts) => objectid::epoch_secs_to_rfc3339(ts.time as i64),
            other => infer(other),
        },
        Coerce::DateTime => match leaf {
            Bson::DateTime(dt) => objectid::epoch_millis_to_rfc3339(dt.timestamp_millis()),
            other => infer(other),
        },
        Coerce::Blob => match leaf {
            Bson::Binary(b) => hex::encode(&b.bytes),
            other => infer(other),
        },
        Coerce::Infer => infer(leaf),
    }
}

/// §A leaf inference for the default (no `as`) mode: scalars render as their bare
/// value, everything typed/structural renders as canonical Extended JSON.
fn infer(leaf: &Bson) -> String {
    match leaf {
        Bson::String(s) => s.clone(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::Decimal128(d) => d.to_string(),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::DateTime(dt) => objectid::epoch_millis_to_rfc3339(dt.timestamp_millis()),
        Bson::Null => "null".to_string(),
        other => Bson::clone(other).into_canonical_extjson().to_string(),
    }
}

/// Format 16 raw bytes as a canonical 8-4-4-4-12 UUID string.
fn uuid_string(bytes: &[u8]) -> String {
    if bytes.len() != 16 {
        return hex::encode(bytes);
    }
    let h = hex::encode(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, oid::ObjectId, Binary};

    fn enc(d: &bson::Document) -> Vec<u8> {
        let mut v = Vec::new();
        d.to_writer(&mut v).unwrap();
        v
    }

    #[test]
    fn dotted_and_indexed_paths() {
        let b = enc(&doc! {
            "o": { "sku": "abc" },
            "items": ["x", "y", "z"],
        });
        assert_eq!(
            field(&b, "o.sku", Coerce::Infer).unwrap().as_deref(),
            Some("abc")
        );
        assert_eq!(
            field(&b, "items.1", Coerce::Infer).unwrap().as_deref(),
            Some("y")
        );
        assert_eq!(field(&b, "missing.path", Coerce::Infer).unwrap(), None);
    }

    #[test]
    fn objectid_coercion() {
        let oid = ObjectId::parse_str("54759eb3c090d83494e2d804").unwrap();
        let b = enc(&doc! { "_id": oid });
        assert_eq!(
            field(&b, "_id", Coerce::ObjectId).unwrap().as_deref(),
            Some("54759eb3c090d83494e2d804")
        );
    }

    #[test]
    fn uuid_coercion() {
        let raw = [
            0x01u8, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ];
        let b = enc(&doc! {
            "tenant": Binary { subtype: BinarySubtype::Uuid, bytes: raw.to_vec() }
        });
        assert_eq!(
            field(&b, "tenant", Coerce::Uuid).unwrap().as_deref(),
            Some("01234567-89ab-cdef-0123-456789abcdef")
        );
    }
}
