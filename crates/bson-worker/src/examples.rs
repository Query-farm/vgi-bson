//! Composed example queries: realistic SQL that combines two or more of the
//! worker's own objects (a table function feeding scalars, a scan joined to the
//! `bson_types` registry, …).
//!
//! Each example is defined once here and referenced from the schema-level
//! `vgi.example_queries` list and from the example list of the object that
//! drives the query (the agent simulator only shows per-object examples).
//!
//! Each worker scalar that takes a constant argument (`field`, `type_of`,
//! `to_json`, …) appears at most once per SELECT list: the vgi extension's bind
//! data currently compares equal across different constants, so DuckDB's
//! common-subexpression pass would merge, e.g., `field(doc, 'name')` and
//! `field(doc, 'age')` into one call.

use crate::meta::Example;

/// The committed `test/data/users.bson` fixture (three documents with ObjectId
/// `_id`s) inlined as hex, so the examples below run for any user: the worker's
/// file readers only see the local filesystem, and a repo-relative path exists
/// only in this checkout.
macro_rules! users_dump_hex {
    () => {
        "2f000000075f69640054759eb3c090d83494e2d804026e616d650006000000616c6963650010616765001e000000002d000000075f69640054759eb3c090d83494e2d805026e616d650004000000626f6200106167650019000000002f000000075f69640054759eb3c090d83494e2d806026e616d6500060000006361726f6c0010616765002900000000"
    };
}

/// `bson_seq` over a mongodump file body → `type_of` → joined to the
/// `bson_types` registry view.
pub const SEQ_ID_TYPE_AUDIT: Example = (
    "Audit which BSON type every document's _id uses in a mongodump file body (three documents, inlined as hex and split with bson_seq), looking up its one-byte type code and Extended JSON wrapper in the bson_types registry.",
    concat!(
        "SELECT t.type_name, t.type_byte, t.extjson_wrapper, count(*) AS docs FROM bson.main.bson_seq(from_hex('",
        users_dump_hex!(),
        "')) AS d JOIN bson.main.bson_types AS t ON t.type_name = bson.main.type_of(d.doc, '_id') GROUP BY t.type_name, t.type_byte, t.extjson_wrapper ORDER BY docs DESC, t.type_name"
    ),
);

/// `bson_seq` → `field(…, 'objectid')` → `objectid_timestamp`, with `to_json`
/// rendering each document.
pub const SEQ_CREATED_FROM_OBJECTID: Example = (
    "Recover when each document in a mongodump file body (three documents, inlined as hex and split with bson_seq) was created from the timestamp embedded in its ObjectId _id, next to the document as relaxed Extended JSON.",
    concat!(
        "SELECT d.idx, bson.main.objectid_timestamp(bson.main.field(d.doc, '_id', 'objectid')) AS created, bson.main.to_json(d.doc, 'relaxed') AS doc_json FROM bson.main.bson_seq(from_hex('",
        users_dump_hex!(),
        "')) AS d ORDER BY d.idx"
    ),
);

/// `from_json` builds a concatenated stream → `bson_seq` splits it → `field`
/// projects each entry's operation for an aggregate.
pub const SEQ_OPLOG_OP_COUNTS: Example = (
    "Split a concatenated oplog-style BSON stream into documents and count the entries per operation type (i = insert, u = update).",
    "SELECT bson.main.field(s.doc, 'op') AS op, count(*) AS entries FROM bson.main.bson_seq(bson.main.from_json('{\"op\":\"i\",\"ns\":\"shop.orders\"}') || bson.main.from_json('{\"op\":\"u\",\"ns\":\"shop.orders\"}') || bson.main.from_json('{\"op\":\"i\",\"ns\":\"shop.users\"}')) AS s GROUP BY op ORDER BY op",
);
