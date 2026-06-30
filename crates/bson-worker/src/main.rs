//! The `bson` VGI worker.
//!
//! A standalone binary DuckDB launches and talks to over Apache Arrow IPC
//! (`ATTACH 'bson' (TYPE vgi, LOCATION '…')`). It decodes and encodes **BSON**
//! (Binary JSON, bsonspec.org) blobs to and from DuckDB values and MongoDB
//! Extended JSON v2, with first-class handling of BSON's rich type zoo —
//! ObjectId, Decimal128, Binary subtypes (incl. UUID 0x04), Timestamp vs
//! UTCDateTime — under the catalog `bson`, schema `main`.
//!
//! Pure in-engine scalar/table compute over a `BLOB` column: no network, no
//! state, zero egress. It makes the raw BSON the live `mongo` connector never
//! sees — mongodump `.bson` files, oplog / change-stream captures, GridFS chunks,
//! and BSON-at-rest in the warehouse — joinable in SQL at scan scale.

mod arrow_io;
mod meta;
mod scalar;
mod table;
mod value_in;

use vgi::catalog::{CatSchema, CatalogModel};
use vgi::Worker;

/// Catalog + schema metadata surfaced to DuckDB and the `vgi-lint` metadata
/// linter. The function objects themselves are served from the registered
/// scalars / table functions.
fn catalog_metadata(name: &str) -> CatalogModel {
    CatalogModel {
        name: name.to_string(),
        comment: Some(
            "BSON (Binary JSON) decode & encode plus Extended JSON v2, typed field extraction, \
             and mongodump/oplog/GridFS stream splitting for SQL."
                .to_string(),
        ),
        tags: vec![
            (
                "vgi.title".to_string(),
                "BSON Decode / Extended JSON / Typed Field Codec".to_string(),
            ),
            (
                "vgi.keywords".to_string(),
                meta::keywords_json(
                    "bson, binary json, bsonspec, mongodb, mongodump, oplog, change stream, \
                     gridfs, extended json, extjson, objectid, decimal128, uuid, timestamp, \
                     utcdatetime, binary subtype, decode, encode, to_json, from_json, field, \
                     well_formed, type_of, migration, forensics, cdc, backup, at-rest",
                ),
            ),
            (
                "vgi.doc_llm".to_string(),
                "Decode and encode BSON (Binary JSON, bsonspec.org) blobs in SQL with lossless, \
                 typed handling of BSON's rich type zoo: ObjectId, Decimal128, Binary subtypes \
                 (incl. UUID 0x04 and encrypted/FLE), Timestamp-vs-UTCDateTime, Int32/Int64/Double, \
                 Regex, MinKey/MaxKey, DBPointer, Code/CodeWScope. `decode` / `to_json` render any \
                 document to MongoDB Extended JSON v2 (canonical = lossless, relaxed = readable); \
                 `from_json` / `encode` go the other way; `field(doc, path, as)` projects one typed \
                 leaf by dotted path. `is_valid` / `well_formed` give untrusted-input-safe checks \
                 that never crash the scan. ObjectId helpers (objectid_timestamp / objectid_hex / \
                 objectid_from_hex) and Timestamp helpers (timestamp_to_ts / timestamp_parts) read \
                 the typed sub-values. The `bson_seq` and `mongodump_read` table functions fan a \
                 concatenated length-prefixed BSON stream (a mongodump `.bson` file, an oplog \
                 batch, a GridFS reassembly) into one row per document. It decodes the raw BSON the \
                 live mongo connector never sees — dumps, oplog, GridFS, BSON-at-rest. Pure \
                 in-engine compute over a BLOB column: no network, no state, zero egress."
                    .to_string(),
            ),
            (
                "vgi.doc_md".to_string(),
                "# bson\n\nDecode and encode **BSON** (Binary JSON, [bsonspec.org](https://bsonspec.org)) \
                 in SQL, with lossless typed handling of ObjectId, Decimal128, UUID-subtype Binary, \
                 and the Timestamp-vs-UTCDateTime footgun — plus correct **MongoDB Extended JSON \
                 v2** (canonical & relaxed). The value is in-engine decode of the raw BSON the live \
                 `mongo` connector never sees: **mongodump backups**, **oplog / change-stream** \
                 captures, **GridFS chunks**, and **BSON columns at rest**. The `bson_seq` / \
                 `mongodump_read` table functions fan a concatenated `.bson` stream into one row \
                 per document. Pure scalar / table compute over a `BLOB` column — no network, no \
                 state, zero egress.\n\n**Out of scope:** any live MongoDB connection (that is the \
                 `mongo` extension's job — this is the offline/at-rest complement) and FLE / \
                 Queryable-Encryption decryption (encrypted Binary subtypes stay opaque `BLOB`)."
                    .to_string(),
            ),
            ("vgi.author".to_string(), "Query.Farm".to_string()),
            (
                "vgi.copyright".to_string(),
                "Copyright 2026 Query Farm LLC - https://query.farm".to_string(),
            ),
            ("vgi.license".to_string(), "MIT".to_string()),
            (
                "vgi.support_contact".to_string(),
                "https://github.com/Query-farm/vgi-bson/issues".to_string(),
            ),
            (
                "vgi.support_policy_url".to_string(),
                "https://github.com/Query-farm/vgi-bson/blob/main/README.md".to_string(),
            ),
        ],
        source_url: Some("https://github.com/Query-farm/vgi-bson".to_string()),
        schemas: vec![CatSchema {
            name: "main".to_string(),
            comment: Some(
                "BSON decode / encode, Extended JSON v2, typed field extraction, and \
                 mongodump/oplog/GridFS stream splitting."
                    .to_string(),
            ),
            tags: vec![
                ("vgi.title".to_string(), "BSON — main".to_string()),
                (
                    "vgi.keywords".to_string(),
                    meta::keywords_json(
                        "bson, mongodb, decode, encode, to_json, from_json, field, type_of, keys, \
                         is_valid, well_formed, objectid, timestamp, bson_seq, mongodump_read, \
                         extended json",
                    ),
                ),
                ("domain".to_string(), "data-serialization".to_string()),
                ("category".to_string(), "parsing-and-serialization".to_string()),
                ("topic".to_string(), "bson-mongodb".to_string()),
                (
                    "vgi.doc_llm".to_string(),
                    "Functions for BSON / MongoDB Extended JSON. Codec: `decode`, `to_json`, \
                     `from_json`, `encode`, `is_valid`, `well_formed`. Projection: `field`, \
                     `type_of`, `keys`. ObjectId: `objectid_timestamp`, `objectid_hex`, \
                     `objectid_from_hex`. Timestamp: `timestamp_to_ts`, `timestamp_parts`. Table \
                     functions: `bson_seq` (concatenated stream) and `mongodump_read` (`.bson` \
                     file glob)."
                        .to_string(),
                ),
                (
                    "vgi.doc_md".to_string(),
                    "The single schema for the `bson` worker — the catalog name matches the \
                     `ATTACH` name, so qualify calls as `bson.main.<fn>(...)`. Holds the BSON \
                     codec scalars, the Extended-JSON bridge, typed field extraction, the ObjectId \
                     / Timestamp helpers, and the `bson_seq` / `mongodump_read` table functions."
                        .to_string(),
                ),
                (
                    "vgi.example_queries".to_string(),
                    "SELECT bson.main.to_json(bson.main.from_json('{\"a\":1}'));\n\
                     SELECT bson.main.to_json(bson.main.from_json('{\"a\":1}'), 'relaxed');\n\
                     SELECT bson.main.field(bson.main.from_json('{\"o\":{\"sku\":\"abc\"}}'), 'o.sku');\n\
                     SELECT (bson.main.well_formed(from_hex('0500000000'))).ok;\n\
                     SELECT bson.main.type_of(bson.main.from_json('{\"a\":1}'));\n\
                     SELECT idx, octet_length(doc) FROM bson.main.bson_seq(from_hex('05000000000500000000'));"
                        .to_string(),
                ),
            ],
            views: Vec::new(),
            macros: Vec::new(),
            tables: Vec::new(),
        }],
        ..Default::default()
    }
}

fn main() {
    // Logs MUST go to stderr — stdout is the Arrow-IPC channel.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().filter_or("VGI_LOG", "info"))
        .format_timestamp_millis()
        .try_init();

    if std::env::var_os("VGI_WORKER_CATALOG_NAME").is_none() {
        std::env::set_var("VGI_WORKER_CATALOG_NAME", "bson");
    }
    let catalog_name =
        std::env::var("VGI_WORKER_CATALOG_NAME").unwrap_or_else(|_| "bson".to_string());

    let mut worker = Worker::new();
    scalar::register(&mut worker);
    table::register(&mut worker);
    worker.set_catalog(catalog_metadata(&catalog_name));
    worker.run();
}
