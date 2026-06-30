//! Golden-vector tests covering the full BSON type zoo through the core decoders:
//! ObjectId, UTCDateTime, Decimal128 (incl. fallback), Binary subtypes (generic,
//! UUID 0x04, MD5, encrypted), Timestamp vs DateTime, nested docs/arrays, Regex,
//! MinKey/MaxKey, Int32/Int64/Double.

use bson::spec::BinarySubtype;
use bson::{doc, oid::ObjectId, Binary, Bson, DateTime, Regex, Timestamp};
use bson_core::extjson::JsonMode;
use bson_core::field::{field, Coerce};
use bson_core::{extjson, typeinfo, validate};

/// Encode a document to bytes.
fn enc(d: &bson::Document) -> Vec<u8> {
    let mut v = Vec::new();
    d.to_writer(&mut v).unwrap();
    v
}

/// A document exercising the rich BSON type zoo.
fn zoo() -> bson::Document {
    doc! {
        "_id": ObjectId::parse_str("54759eb3c090d83494e2d804").unwrap(),
        "dbl": 3.5f64,
        "i32": 42i32,
        "i64": 9_000_000_000i64,
        "str": "héllo",
        "bool": true,
        "nul": Bson::Null,
        "created": DateTime::from_millis(1_700_000_000_000),
        "oplog_ts": Timestamp { time: 1_416_994_483, increment: 7 },
        "uuid": Binary { subtype: BinarySubtype::Uuid, bytes: vec![0xAB; 16] },
        "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3, 4] },
        "md5": Binary { subtype: BinarySubtype::Md5, bytes: vec![0u8; 16] },
        "enc": Binary { subtype: BinarySubtype::Encrypted, bytes: vec![9, 9] },
        "re": Regex { pattern: bson::raw::cstr!("ab.*").to_owned(), options: bson::raw::cstr!("i").to_owned() },
        "min": Bson::MinKey,
        "max": Bson::MaxKey,
        "nested": { "arr": [1i32, 2i32, 3i32], "deep": { "k": "v" } },
    }
}

#[test]
fn zoo_is_well_formed() {
    let b = enc(&zoo());
    assert!(validate::is_valid(&b));
    let wf = validate::well_formed(&b);
    assert!(wf.ok, "zoo doc must be well-formed: {:?}", wf.kind);
}

#[test]
fn type_of_names_every_type() {
    let b = enc(&zoo());
    let cases = [
        ("_id", "objectId"),
        ("dbl", "double"),
        ("i32", "int"),
        ("i64", "long"),
        ("str", "string"),
        ("bool", "bool"),
        ("nul", "null"),
        ("created", "date"),
        ("oplog_ts", "timestamp"),
        ("uuid", "binData:uuid"),
        ("blob", "binData:generic"),
        ("md5", "binData:md5"),
        ("enc", "binData:encrypted"),
        ("re", "regex"),
        ("min", "minKey"),
        ("max", "maxKey"),
        ("nested", "object"),
        ("nested.arr", "array"),
    ];
    for (path, want) in cases {
        assert_eq!(
            typeinfo::type_of(&b, Some(path)).unwrap().as_deref(),
            Some(want),
            "type_of({path})"
        );
    }
}

#[test]
fn field_typed_coercions() {
    let b = enc(&zoo());
    assert_eq!(
        field(&b, "_id", Coerce::ObjectId).unwrap().as_deref(),
        Some("54759eb3c090d83494e2d804")
    );
    assert_eq!(
        field(&b, "uuid", Coerce::Uuid).unwrap().as_deref(),
        Some("abababab-abab-abab-abab-abababababab")
    );
    assert_eq!(
        field(&b, "blob", Coerce::Blob).unwrap().as_deref(),
        Some("01020304")
    );
    // Nested array index path.
    assert_eq!(
        field(&b, "nested.arr.2", Coerce::Infer).unwrap().as_deref(),
        Some("3")
    );
    // Datetime → RFC 3339 (cast-ready).
    assert_eq!(
        field(&b, "created", Coerce::DateTime).unwrap().as_deref(),
        Some("2023-11-14T22:13:20Z")
    );
}

#[test]
fn canonical_extjson_preserves_types() {
    let b = enc(&zoo());
    let j = extjson::to_json(&b, JsonMode::Canonical).unwrap();
    assert!(j.contains("$oid"), "ObjectId wrapper");
    assert!(j.contains("$numberLong"), "Int64 wrapper");
    assert!(j.contains("$date"), "DateTime wrapper");
    assert!(j.contains("$timestamp"), "Timestamp wrapper");
    assert!(j.contains("\"$binary\""), "Binary wrapper");
    assert!(j.contains("$minKey"), "MinKey wrapper");
    assert!(j.contains("$maxKey"), "MaxKey wrapper");
}

#[test]
fn decimal128_roundtrips_and_falls_back() {
    use std::str::FromStr;
    // A representable Decimal128 renders its canonical string via `field('decimal')`.
    let d = bson::Decimal128::from_str("123.45").unwrap();
    let b = enc(&doc! { "amount": d });
    assert_eq!(
        field(&b, "amount", Coerce::Decimal).unwrap().as_deref(),
        Some("123.45")
    );
    // NaN / Infinity render as their canonical strings (the VARCHAR fallback).
    let nan = bson::Decimal128::from_str("NaN").unwrap();
    let b = enc(&doc! { "x": nan });
    let s = field(&b, "x", Coerce::Decimal).unwrap().unwrap();
    assert!(s.eq_ignore_ascii_case("nan"), "Decimal128 NaN → {s}");
}

#[test]
fn canonical_roundtrips_to_bytes_for_zoo_subset() {
    // The headline migration guarantee: from_json ∘ to_json('canonical') is
    // byte-identical on a canonical input. (Verified on a subset whose key order
    // is stable through the Extended-JSON parse.)
    let original = enc(&doc! {
        "_id": ObjectId::parse_str("54759eb3c090d83494e2d804").unwrap(),
        "n": 7i64,
        "s": "x",
    });
    let canonical = extjson::to_json(&original, JsonMode::Canonical).unwrap();
    let back = extjson::from_json(&canonical).unwrap();
    assert_eq!(original, back);
}
