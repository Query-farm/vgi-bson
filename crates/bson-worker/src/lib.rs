//! The `bson` VGI worker (library).
//!
//! Function registration and catalog metadata live here so both entrypoints
//! share them verbatim: `main.rs` (the native binary, stdio/HTTP transport) and
//! the `bson-wasm` crate (the browser build, which serves the same `Worker`
//! over a SharedArrayBuffer byte channel instead).
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

use vgi::catalog::{CatSchema, CatView, CatalogModel};
use vgi::Worker;

/// A credential-free, browsable reference view: the BSON element-type registry.
/// It gives an agent a real table to scan (VGI146) — every BSON type, its type
/// byte, and its canonical Extended JSON v2 wrapper — before it has to guess a
/// table-function argument. Backed by an inline `VALUES` list, so it scans purely
/// inside DuckDB with no worker round-trip and no external state.
fn bson_types_view() -> CatView {
    let definition = r#"SELECT * FROM (VALUES
  ('double',        '0x01', 'numeric',    '{"$numberDouble":"…"}',                              '64-bit IEEE-754 binary floating point.'),
  ('string',        '0x02', 'text',       'plain JSON string',                                  'UTF-8 string.'),
  ('object',        '0x03', 'document',   'nested {…}',                                         'Embedded document (preserves field order).'),
  ('array',         '0x04', 'document',   'JSON array […]',                                     'Array; BSON stores keys as the numeric strings "0","1",… .'),
  ('binData',       '0x05', 'binary',     '{"$binary":{"base64":"…","subType":"…"}}',           'Binary with a one-byte subtype (0x00 generic, 0x04 UUID, 0x06 encrypted, …).'),
  ('undefined',     '0x06', 'deprecated', '{"$undefined":true}',                                'Deprecated; modern data uses null.'),
  ('objectId',      '0x07', 'identifier', '{"$oid":"…"}',                                       '12-byte identifier whose first 4 bytes embed the creation time.'),
  ('bool',          '0x08', 'scalar',     'true / false',                                       'Boolean.'),
  ('date',          '0x09', 'temporal',   '{"$date":{"$numberLong":"…"}}',                      'UTCDateTime: milliseconds since the Unix epoch (a wall-clock instant).'),
  ('null',          '0x0A', 'scalar',     'null',                                               'Null value.'),
  ('regex',         '0x0B', 'text',       '{"$regularExpression":{"pattern":"…","options":"…"}}','Regular expression.'),
  ('dbPointer',     '0x0C', 'deprecated', '{"$dbPointer":{"$ref":"…","$id":{"$oid":"…"}}}',     'Deprecated database pointer.'),
  ('code',          '0x0D', 'code',       '{"$code":"…"}',                                      'JavaScript source without a scope.'),
  ('symbol',        '0x0E', 'deprecated', '{"$symbol":"…"}',                                    'Deprecated symbol type.'),
  ('codeWithScope', '0x0F', 'code',       '{"$code":"…","$scope":{…}}',                         'JavaScript source bundled with a scope document.'),
  ('int',           '0x10', 'numeric',    '{"$numberInt":"…"}',                                 '32-bit signed integer.'),
  ('timestamp',     '0x11', 'temporal',   '{"$timestamp":{"t":…,"i":…}}',                       'Internal replication clock (t seconds, i increment) — NOT a wall-clock time.'),
  ('long',          '0x12', 'numeric',    '{"$numberLong":"…"}',                                '64-bit signed integer.'),
  ('decimal128',    '0x13', 'numeric',    '{"$numberDecimal":"…"}',                             '128-bit IEEE-754 decimal (exact base-10).'),
  ('minKey',        '0xFF', 'sentinel',   '{"$minKey":1}',                                      'Sorts below every other BSON value.'),
  ('maxKey',        '0x7F', 'sentinel',   '{"$maxKey":1}',                                      'Sorts above every other BSON value.')
) AS t(type_name, type_byte, type_class, extjson_wrapper, notes)"#;

    CatView {
        name: "bson_types".to_string(),
        definition: definition.to_string(),
        comment: Some(
            "The BSON element-type registry: every BSON type, its one-byte type code, and its \
             canonical Extended JSON v2 wrapper."
                .to_string(),
        ),
        column_comments: vec![
            (
                "type_name".to_string(),
                "The BSON type name as reported by `type_of`, e.g. 'objectId', 'decimal128', \
                 'timestamp'."
                    .to_string(),
            ),
            (
                "type_byte".to_string(),
                "The one-byte BSON element type code, as a 0x?? hex string (0x01–0x13, plus 0xFF \
                 minKey and 0x7F maxKey)."
                    .to_string(),
            ),
            (
                "type_class".to_string(),
                "A coarse grouping for browsing: numeric, text, document, binary, temporal, \
                 identifier, code, scalar, sentinel, or deprecated."
                    .to_string(),
            ),
            (
                "extjson_wrapper".to_string(),
                "The canonical Extended JSON v2 shape a value of this type serializes to (… marks \
                 the value slot)."
                    .to_string(),
            ),
            (
                "notes".to_string(),
                "A one-line semantic note, including the Timestamp-vs-UTCDateTime and \
                 deprecated-type caveats.".to_string(),
            ),
        ],
        tags: vec![
            ("vgi.title".to_string(), "BSON Type Registry".to_string()),
            ("vgi.category".to_string(), "Reference".to_string()),
            ("domain".to_string(), "data-serialization".to_string()),
            ("topic".to_string(), "bson-mongodb".to_string()),
            (
                "vgi.doc_llm".to_string(),
                "A static, credential-free reference view listing all 21 BSON element types: the \
                 type name `type_of` returns, the one-byte type code (0x01–0x13, plus 0xFF minKey \
                 and 0x7F maxKey), a coarse `type_class` for browsing, the canonical Extended JSON \
                 v2 wrapper the type serializes to, and a one-line note. Use it to map a `type_of` \
                 result to its wire code, to see how a type renders in canonical Extended JSON, or \
                 to spot the footguns — Timestamp (0x11) is a replication clock, not a wall-clock \
                 time; undefined / dbPointer / symbol are deprecated. It scans instantly with no \
                 argument, so it is the cheapest way to see real data from this worker."
                    .to_string(),
            ),
            (
                "vgi.doc_md".to_string(),
                "A static reference view of the **BSON element-type registry**. One row per BSON \
                 type, with the name `type_of` returns, its one-byte type code, a coarse \
                 `type_class`, the canonical **Extended JSON v2** wrapper, and a note.\n\n\
                 Handy for mapping a `type_of` result to its wire code, seeing how a value renders \
                 in canonical Extended JSON, or spotting the footguns: `timestamp` (0x11) is the \
                 replication clock (not a wall-clock time), and `undefined` / `dbPointer` / \
                 `symbol` are deprecated."
                    .to_string(),
            ),
            (
                "vgi.keywords".to_string(),
                meta::keywords_json(
                    "bson, type registry, type code, type byte, element type, extended json, \
                     extjson, wrapper, objectid, decimal128, timestamp, binData, subtype, \
                     reference, catalog",
                ),
            ),
            (
                "vgi.example_queries".to_string(),
                "[{\"description\":\"List the temporal BSON types and their Extended JSON wrappers.\",\"sql\":\"SELECT type_name, type_byte, extjson_wrapper FROM bson.main.bson_types WHERE type_class = 'temporal' ORDER BY type_name\"}]".to_string(),
            ),
        ],
    }
}

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
                 (including `UUID` 0x04 and encrypted / FLE payloads), the Timestamp-vs-UTCDateTime \
                 footgun, Int32 / Int64 / Double, Regex, MinKey / MaxKey, DBPointer, and Code — \
                 plus correct MongoDB Extended JSON v2 in both canonical (type-preserving, \
                 lossless) and relaxed (human-readable) forms. Reach for this worker to put the raw \
                 BSON the live MongoDB connector never sees to work in SQL: mongodump backups, \
                 oplog and change-stream captures, GridFS chunks, and BSON columns already at rest \
                 in the warehouse. Everything runs as pure in-engine scalar and table compute over \
                 a `BLOB` column — no network, no state, zero egress — so it is the offline / \
                 at-rest complement to a live connection, not a driver. Encrypted (FLE / \
                 Queryable-Encryption) values stay opaque."
                    .to_string(),
            ),
            (
                "vgi.doc_md".to_string(),
                "# bson\n\nDecode and encode **BSON** (Binary JSON, [bsonspec.org](https://bsonspec.org)) \
                 in SQL, with lossless typed handling of ObjectId, Decimal128, `UUID`-subtype Binary, \
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
    "prompt": "Using the bson worker, confirm that the BSON encoding of the JSON document {\"a\":1} round-trips losslessly: rendering it to canonical MongoDB Extended JSON with to_json and parsing that back with from_json must reproduce the original BSON bytes. Return the single boolean result.",
    "reference_sql": "SELECT bson.main.from_json(bson.main.to_json(bson.main.from_json('{\"a\":1}'))) = bson.main.from_json('{\"a\":1}')",
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
    "prompt": "Using the bson worker's is_valid function, return the single boolean telling whether the bytes produced by from_hex('0500000000') are exactly one well-formed BSON document.",
    "reference_sql": "SELECT bson.main.is_valid(from_hex('0500000000'))",
    "ignore_column_names": true
  },
  {
    "name": "split-stream",
    "prompt": "Using the bson worker, split the concatenated BSON byte stream from_hex('05000000000500000000') into one row per contained document and return each document's zero-based index.",
    "reference_sql": "SELECT idx FROM bson.main.bson_seq(from_hex('05000000000500000000'))",
    "unordered": true,
    "ignore_column_names": true
  },
  {
    "name": "decode-to-extjson",
    "prompt": "Using the bson worker, confirm that decode and to_json agree on the BSON encoding of {\"a\":1}: decode of those bytes must equal to_json of the same bytes in canonical mode. Return the single boolean result.",
    "reference_sql": "SELECT bson.main.decode(bson.main.from_json('{\"a\":1}')) = bson.main.to_json(bson.main.from_json('{\"a\":1}'), 'canonical')",
    "ignore_column_names": true
  },
  {
    "name": "encode-struct",
    "prompt": "Using the bson worker, encode the DuckDB struct {'a': 1} to BSON bytes with the encode function and return the result as an uppercase hexadecimal string.",
    "reference_sql": "SELECT to_hex(bson.main.encode({'a': 1}))",
    "ignore_column_names": true
  },
  {
    "name": "list-top-level-keys",
    "prompt": "Using the bson worker, list the top-level field names, in document order, of the BSON encoding of the JSON document {\"z\":1,\"a\":2,\"m\":3}.",
    "reference_sql": "SELECT bson.main.keys(bson.main.from_json('{\"z\":1,\"a\":2,\"m\":3}'))",
    "ignore_column_names": true
  },
  {
    "name": "type-name-at-path",
    "prompt": "Using the bson worker, report the BSON type name at the field path _id of the document {\"_id\":{\"$oid\":\"54759eb3c090d83494e2d804\"}} encoded with from_json.",
    "reference_sql": "SELECT bson.main.type_of(bson.main.from_json('{\"_id\":{\"$oid\":\"54759eb3c090d83494e2d804\"}}'), '_id')",
    "ignore_column_names": true
  },
  {
    "name": "diagnose-truncated",
    "prompt": "Using the bson worker's well_formed function, return the single boolean telling whether the failure kind it reports for the malformed bytes from_hex('0c00000010610001') is 'truncated'.",
    "reference_sql": "SELECT (bson.main.well_formed(from_hex('0c00000010610001'))).kind = 'truncated'",
    "ignore_column_names": true
  },
  {
    "name": "objectid-hex-roundtrip",
    "prompt": "Using the bson worker, convert the ObjectId hex string 54759eb3c090d83494e2d804 to its 12 raw bytes with objectid_from_hex and then back to a hex string with objectid_hex; return that hex string.",
    "reference_sql": "SELECT bson.main.objectid_hex(bson.main.objectid_from_hex('54759eb3c090d83494e2d804'))",
    "ignore_column_names": true
  },
  {
    "name": "timestamp-increment",
    "prompt": "Using the bson worker, read the increment (i) component from the BSON internal Timestamp struct {'t': 1416994483, 'i': 7} using timestamp_parts.",
    "reference_sql": "SELECT (bson.main.timestamp_parts({'t': 1416994483, 'i': 7})).i",
    "ignore_column_names": true
  },
  {
    "name": "timestamp-to-wallclock",
    "prompt": "Using the bson worker, convert the BSON internal Timestamp struct {'t': 1416994483, 'i': 7} to a TIMESTAMPTZ from its seconds component using timestamp_to_ts.",
    "reference_sql": "SELECT bson.main.timestamp_to_ts({'t': 1416994483, 'i': 7})",
    "ignore_column_names": true
  },
  {
    "name": "mongodump-document-count",
    "prompt": "Using the bson worker's mongodump_read function on the file at the relative path test/data/users.bson, return the single boolean telling whether it contains exactly 3 BSON documents.",
    "reference_sql": "SELECT count(*) = 3 FROM bson.main.mongodump_read('test/data/users.bson')",
    "ignore_column_names": true
  },
  {
    "name": "type-registry-lookup",
    "prompt": "Using the bson worker's bson_types reference view, return the single boolean telling whether the BSON decimal128 type is listed with type byte '0x13'.",
    "reference_sql": "SELECT type_byte = '0x13' FROM bson.main.bson_types WHERE type_name = 'decimal128'",
    "ignore_column_names": true
  }
]"#
                .to_string(),
            ),
        ],
        source_url: Some("https://github.com/Query-farm/vgi-bson".to_string()),
        // The worker's own build version, advertised on the catalog so an agent
        // reads it from vgi_catalogs() without spending a query (and it cannot
        // drift from the running binary). Replaces a parameterless version()
        // scalar (VGI328). Kept in lockstep with the release tag by
        // ci/check-version.sh.
        implementation_version: Some(bson_core::version().to_string()),
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
  {"name": "Reference", "description": "Static, browsable lookup data such as the BSON element-type registry."}
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
                     batch, a GridFS reassembly — into one row per document. The codec round-trips \
                     canonical Extended JSON byte-for-byte, so it doubles as the migration and \
                     diff surface for BSON at rest."
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
                     The `vgi.categories` groupings show where each function fits; the codec's \
                     canonical Extended JSON round-trips byte-for-byte, making it the migration \
                     and diff surface for BSON at rest."
                        .to_string(),
                ),
                (
                    "vgi.example_queries".to_string(),
                    r#"[
  {"description": "Render a BSON document as canonical (lossless) Extended JSON v2.", "sql": "SELECT bson.main.to_json(bson.main.from_json('{\"a\":1}'))"},
  {"description": "Render the same document as relaxed, human-readable Extended JSON.", "sql": "SELECT bson.main.to_json(bson.main.from_json('{\"a\":1}'), 'relaxed')"},
  {"description": "Extract a nested leaf by dotted path without decoding the whole document.", "sql": "SELECT bson.main.field(bson.main.from_json('{\"o\":{\"sku\":\"abc\"}}'), 'o.sku')"},
  {"description": "Check that a tiny empty document is structurally well-formed.", "sql": "SELECT (bson.main.well_formed(from_hex('0500000000'))).ok"},
  {"description": "Report the top-level BSON type name of a document.", "sql": "SELECT bson.main.type_of(bson.main.from_json('{\"a\":1}'))"},
  {"description": "Fan a concatenated BSON stream into one row per document with its byte length.", "sql": "SELECT idx, octet_length(doc) AS n FROM bson.main.bson_seq(from_hex('05000000000500000000')) ORDER BY idx"}
]"#
                        .to_string(),
                ),
                (
                    // At least one guaranteed-runnable, verified example (VGI509).
                    // Both assert a deterministic boolean, independent of exact
                    // Extended-JSON spacing, so they stay stable across builds.
                    "vgi.executable_examples".to_string(),
                    r#"[
  {"description": "Canonical Extended JSON round-trips BSON byte-for-byte (the migration guarantee).", "sql": "SELECT bson.main.from_json(bson.main.to_json(bson.main.from_json('{\"a\":1}'))) = bson.main.from_json('{\"a\":1}') AS roundtrips", "expected_result": [{"roundtrips": true}]},
  {"description": "A minimal empty document (05 00 00 00 00) is well-formed.", "sql": "SELECT (bson.main.well_formed(from_hex('0500000000'))).ok AS ok", "expected_result": [{"ok": true}]}
]"#
                    .to_string(),
                ),
            ],
            views: vec![bson_types_view()],
            macros: Vec::new(),
            tables: Vec::new(),
        }],
        ..Default::default()
    }
}

/// The catalog name DuckDB sees in `ATTACH 'bson' (TYPE vgi, …)`. Defaults to
/// `bson`, but honors an explicit override so a test harness can rename it.
/// Also exports the variable so downstream SDK code observes the same default.
pub fn catalog_name() -> String {
    if std::env::var_os("VGI_WORKER_CATALOG_NAME").is_none() {
        std::env::set_var("VGI_WORKER_CATALOG_NAME", "bson");
    }
    std::env::var("VGI_WORKER_CATALOG_NAME").unwrap_or_else(|_| "bson".to_string())
}

/// Build a fully-registered worker: every scalar and table function plus the
/// catalog metadata. Callers choose the transport — `run()` natively,
/// `serve_reader_writer()` in the browser.
pub fn build_worker() -> Worker {
    let name = catalog_name();
    let mut worker = Worker::new();
    scalar::register(&mut worker);
    table::register(&mut worker);
    worker.set_catalog(catalog_metadata(&name));
    worker
}
