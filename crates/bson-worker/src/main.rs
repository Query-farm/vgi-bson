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
                "Decode and encode BSON (Binary JSON, bsonspec.org) in SQL with lossless, typed \
                 handling of BSON's rich type zoo — ObjectId, Decimal128, Binary subtypes \
                 (including UUID 0x04 and encrypted / FLE payloads), the Timestamp-vs-UTCDateTime \
                 footgun, Int32 / Int64 / Double, Regex, MinKey / MaxKey, DBPointer, and Code — \
                 plus correct MongoDB Extended JSON v2 in both canonical (type-preserving, \
                 lossless) and relaxed (human-readable) forms. Reach for this worker to put the raw \
                 BSON the live MongoDB connector never sees to work in SQL: mongodump backups, \
                 oplog and change-stream captures, GridFS chunks, and BSON columns already at rest \
                 in the warehouse. Everything runs as pure in-engine scalar and table compute over \
                 a BLOB column — no network, no state, zero egress — so it is the offline / \
                 at-rest complement to a live connection, not a driver. Encrypted (FLE / \
                 Queryable-Encryption) values stay opaque. Discover the concrete functions by \
                 listing the schema."
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
            (
                "vgi.agent_test_tasks".to_string(),
                // Deterministic, self-contained analyst tasks for `vgi-lint simulate`
                // (VGI152/VGI920). Each reference query builds its own BSON from a
                // literal (from_json / from_hex) so there is no external-file or
                // ordering dependency; `ignore_column_names` lets the analyst pick any
                // alias, and `unordered` frees the row order of the stream-split task.
                r#"[
  {
    "name": "json-roundtrip",
    "prompt": "Using the bson worker, encode the JSON document {\"a\":1} to BSON bytes and then render those bytes back to canonical MongoDB Extended JSON.",
    "reference_sql": "SELECT bson.main.to_json(bson.main.from_json('{\"a\":1}'))",
    "ignore_column_names": true
  },
  {
    "name": "field-extract",
    "prompt": "Using the bson worker, extract the value at the dotted path o.sku from the BSON encoding of the JSON document {\"o\":{\"sku\":\"abc\"}}.",
    "reference_sql": "SELECT bson.main.field(bson.main.from_json('{\"o\":{\"sku\":\"abc\"}}'), 'o.sku')",
    "ignore_column_names": true
  },
  {
    "name": "objectid-created-time",
    "prompt": "Using the bson worker, return the creation time embedded in the ObjectId whose hexadecimal string is 54759eb3c090d83494e2d804.",
    "reference_sql": "SELECT bson.main.objectid_timestamp('54759eb3c090d83494e2d804')",
    "ignore_column_names": true
  },
  {
    "name": "validate-document",
    "prompt": "Using the bson worker, determine whether the bytes produced by from_hex('0500000000') are a single well-formed BSON document.",
    "reference_sql": "SELECT bson.main.is_valid(from_hex('0500000000'))",
    "ignore_column_names": true
  },
  {
    "name": "split-stream",
    "prompt": "Using the bson worker, split the concatenated BSON byte stream from_hex('05000000000500000000') into one row per contained document and return each document's zero-based index.",
    "reference_sql": "SELECT idx FROM bson.main.bson_seq(from_hex('05000000000500000000'))",
    "unordered": true,
    "ignore_column_names": true
  }
]"#
                .to_string(),
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
                    "vgi.categories".to_string(),
                    r#"[
  {"name": "Codec", "description": "Decode and encode BSON, bridging documents to and from MongoDB Extended JSON v2 and DuckDB values."},
  {"name": "Validation", "description": "Structural well-formedness checks that stay total on hostile input."},
  {"name": "Projection", "description": "Extract a typed leaf, its BSON type, or the key set from a document by dotted path."},
  {"name": "ObjectId", "description": "Convert ObjectIds between hex and bytes and read their embedded creation time."},
  {"name": "Timestamp", "description": "Read the BSON internal (replication-clock) Timestamp's parts and wall-clock second."},
  {"name": "Streams", "description": "Fan a concatenated length-prefixed BSON stream (mongodump / oplog / GridFS) into rows."},
  {"name": "Diagnostics", "description": "Introspect the running worker, such as its build version."}
]"#
                    .to_string(),
                ),
                (
                    "vgi.doc_llm".to_string(),
                    "The single schema for the bson worker; because the catalog name matches the \
                     ATTACH name, qualify calls as `bson.main.<name>`. It groups the worker's \
                     surface into a few families: a BSON ↔ Extended JSON v2 codec; structural \
                     validity checks that stay total on hostile input (a bad row never crashes the \
                     scan); typed field, type, and shape projection by dotted path; ObjectId and \
                     internal-Timestamp (replication-clock) helpers; and table functions that fan \
                     a concatenated length-prefixed BSON stream — a mongodump file, an oplog \
                     batch, a GridFS reassembly — into one row per document. List the schema to \
                     see the individual functions and their signatures."
                        .to_string(),
                ),
                (
                    "vgi.doc_md".to_string(),
                    "## bson.main\n\n\
                     The single schema of the `bson` worker. The catalog name matches the \
                     `ATTACH` name, so qualify calls as `bson.main.<name>`.\n\n\
                     It covers the whole offline BSON surface, grouped into a few families: a \
                     BSON ↔ Extended JSON v2 **codec**, structural **validity** checks that stay \
                     total on hostile input, typed dotted-path **projection**, **ObjectId** and \
                     internal-**Timestamp** helpers, and **stream**-splitting table functions for \
                     mongodump / oplog / GridFS data.\n\n\
                     List the schema to browse the individual functions and their signatures, or \
                     use the `vgi.categories` groupings to see where each one fits."
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
