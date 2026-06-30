//! `bson.type_of(doc[, path])` — the BSON type name at a path — and
//! `bson.keys(doc)` — the top-level field names in document order.

use bson::spec::BinarySubtype;
use bson::Bson;

use crate::validate::DecodeError;
use crate::value::decode_document;

/// The BSON type name of `value`, e.g. `objectId`, `decimal128`, `timestamp`,
/// `date`, `binData:uuid`, `binData:generic`, `minKey`, `object`.
pub fn type_name(value: &Bson) -> String {
    match value {
        Bson::Double(_) => "double".into(),
        Bson::String(_) => "string".into(),
        Bson::Document(_) => "object".into(),
        Bson::Array(_) => "array".into(),
        Bson::Boolean(_) => "bool".into(),
        Bson::Null => "null".into(),
        Bson::RegularExpression(_) => "regex".into(),
        Bson::JavaScriptCode(_) => "javascript".into(),
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope".into(),
        Bson::Int32(_) => "int".into(),
        Bson::Int64(_) => "long".into(),
        Bson::Timestamp(_) => "timestamp".into(),
        Bson::ObjectId(_) => "objectId".into(),
        Bson::DateTime(_) => "date".into(),
        Bson::Symbol(_) => "symbol".into(),
        Bson::Decimal128(_) => "decimal128".into(),
        Bson::Undefined => "undefined".into(),
        Bson::MaxKey => "maxKey".into(),
        Bson::MinKey => "minKey".into(),
        Bson::DbPointer(_) => "dbPointer".into(),
        Bson::Binary(b) => format!("binData:{}", subtype_name(b.subtype)),
    }
}

/// A short label for a Binary subtype, surfaced by `type_of`.
fn subtype_name(st: BinarySubtype) -> &'static str {
    match st {
        BinarySubtype::Generic => "generic",
        BinarySubtype::Function => "function",
        BinarySubtype::BinaryOld => "binaryOld",
        BinarySubtype::UuidOld => "uuidOld",
        BinarySubtype::Uuid => "uuid",
        BinarySubtype::Md5 => "md5",
        BinarySubtype::Encrypted => "encrypted",
        BinarySubtype::Column => "column",
        BinarySubtype::Sensitive => "sensitive",
        BinarySubtype::Vector => "vector",
        BinarySubtype::UserDefined(_) => "userDefined",
        _ => "reserved",
    }
}

/// `bson.type_of(doc, path)` — the type name at `path` (or the top-level
/// `object` when `path` is `None`). Missing path → `None` (SQL `NULL`).
pub fn type_of(bytes: &[u8], path: Option<&str>) -> Result<Option<String>, DecodeError> {
    let doc = decode_document(bytes)?;
    let root = Bson::Document(doc);
    let target = match path {
        None => Some(&root),
        Some(p) => navigate(&root, p),
    };
    Ok(target.map(type_name))
}

/// Walk a dotted path (with numeric array indices) from `root`.
fn navigate<'a>(root: &'a Bson, path: &str) -> Option<&'a Bson> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = match cur {
            Bson::Document(d) => d.get(seg)?,
            Bson::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// `bson.keys(doc)` — the top-level field names in document order.
pub fn keys(bytes: &[u8]) -> Result<Vec<String>, DecodeError> {
    let doc = decode_document(bytes)?;
    Ok(doc.keys().cloned().collect())
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
    fn names_the_types() {
        let b = enc(&doc! {
            "id": ObjectId::new(),
            "n": 1i64,
            "u": Binary { subtype: BinarySubtype::Uuid, bytes: vec![0u8; 16] },
        });
        assert_eq!(type_of(&b, None).unwrap().as_deref(), Some("object"));
        assert_eq!(
            type_of(&b, Some("id")).unwrap().as_deref(),
            Some("objectId")
        );
        assert_eq!(type_of(&b, Some("n")).unwrap().as_deref(), Some("long"));
        assert_eq!(
            type_of(&b, Some("u")).unwrap().as_deref(),
            Some("binData:uuid")
        );
        assert_eq!(type_of(&b, Some("nope")).unwrap(), None);
    }

    #[test]
    fn lists_keys_in_order() {
        let b = enc(&doc! { "z": 1i32, "a": 2i32, "m": 3i32 });
        assert_eq!(keys(&b).unwrap(), vec!["z", "a", "m"]);
    }
}
