<p align="center">
  <img src="https://query.farm/logo.svg" alt="Query.Farm" height="64">
</p>

# vgi-bson

A [VGI](https://query.farm) worker that decodes and encodes **BSON** (Binary
JSON, [bsonspec.org](https://bsonspec.org)) blobs to and from DuckDB values and
**MongoDB Extended JSON v2**, with first-class handling of BSON's rich type zoo:
**ObjectId**, **Decimal128**, **Binary** subtypes (incl. UUID `0x04` and
encrypted/FLE), **Timestamp** vs **UTCDateTime**, Int32/Int64/Double, Regex,
MinKey/MaxKey, DBPointer, and Code/CodeWScope.

It decodes the raw BSON the live `mongo` connector never sees — **mongodump
`.bson` files**, **oplog / change-stream** captures, **GridFS chunks**, and
**BSON columns at rest** in the warehouse — and makes them joinable in SQL at
scan scale, with correct Extended JSON v2 and lossless typed decode that a naive
`json` path silently flattens or drops. Pure in-engine scalar/table compute over
a `BLOB` column: **no network, no state, zero egress** (safe for air-gapped /
regulated data).

> **Complement, not replacement.** The live DuckDB `mongo` community extension is
> a network connector to a running `mongod`/Atlas. `vgi-bson` is the
> **offline / at-rest** complement: once your data is a dump, an oplog capture, a
> GridFS table, or a BSON column in Parquet/Iceberg, `vgi-bson` decodes it where
> the connector cannot.

## Install & attach

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'bson' AS bson (TYPE vgi);   -- spawns the worker binary
SET search_path = 'bson.main';
```

## Examples

```sql
-- 1. Read a whole mongodump directory (concatenated length-prefixed BSON docs),
--    decode each document to canonical Extended JSON, and project a typed field.
SELECT d.idx,
       bson.to_json(d.doc, 'canonical')        AS extjson,   -- Extended JSON v2 canonical
       bson.field(d.doc, 'name')               AS name
FROM bson.mongodump_read('/backups/2026-06-28/mydb/users.bson') AS d;

-- 2. Explode an oplog capture: one row per change op, with typed ObjectId + ts.
SELECT bson.objectid_timestamp(bson.field(entry, '_id', 'objectid')) AS doc_created,
       bson.field(entry, 'op')                                       AS op,
       bson.field(entry, 'ns')                                       AS ns
FROM read_blob('oplog/*.bson') AS f,
     LATERAL bson.bson_seq(f.content) AS s(idx, entry);

-- 3. Lossless typed projection a JSON path would corrupt: ObjectId, Decimal128, UUID.
SELECT bson.field(doc, '_id', 'objectid')                  AS id_hex,
       bson.field(doc, 'amount', 'decimal')::DECIMAL(38,4) AS amount,
       bson.field(doc, 'tenant', 'uuid')::UUID             AS tenant_uuid
FROM read_blob('warehouse/orders_bson/*.bson') AS f,
     LATERAL bson.bson_seq(f.content) AS s(idx, doc);

-- 4. Triage: which blobs in this column are not well-formed BSON?
SELECT path, bson.well_formed(payload).kind AS why
FROM gridfs_chunks
WHERE NOT bson.is_valid(payload);
```

## Function catalog

All functions live in the `bson.main` schema.

### Codec

| Function | Returns | Notes |
| --- | --- | --- |
| `decode(doc BLOB [, mode])` | `JSON` (VARCHAR) | Richest self-describing form. A DuckDB scalar's output type is fixed at bind time with no data sample, so every `mode` (`auto`/`struct`/`map`/`json`) returns canonical Extended JSON — the lossless, stable column. |
| `to_json(doc BLOB [, mode])` | `JSON` | MongoDB Extended JSON v2. `mode = 'canonical'` (default, lossless) or `'relaxed'` (readable). |
| `from_json(extjson VARCHAR)` | `BLOB` | Extended JSON v2 (canonical **or** relaxed) → BSON bytes. Round-trips `to_json('canonical')` to byte-identity. |
| `encode(value ANY)` | `BLOB` | DuckDB STRUCT/MAP → BSON document. Top-level must be a document. |

### Typed projection

| Function | Returns | Notes |
| --- | --- | --- |
| `field(doc BLOB, path VARCHAR [, as])` | `VARCHAR` | Extract one field by dotted path (`'o._id'`, `'items.0.sku'`). `as ∈ {objectid, decimal, uuid, timestamp, datetime, blob, json}` coerces the leaf to a **cast-ready** string. Missing path → `NULL`. |
| `type_of(doc BLOB [, path])` | `VARCHAR` | BSON type name (`'objectId'`, `'decimal128'`, `'binData:uuid'`, `'minKey'`, …). NULL path → top-level `'object'`. |
| `keys(doc BLOB)` | `LIST<VARCHAR>` | Top-level field names in document order. |

### Validation (untrusted-input safe — never crashes the scan)

| Function | Returns | Notes |
| --- | --- | --- |
| `is_valid(doc BLOB)` | `BOOLEAN` | Cheap structural pass; no value-tree allocation. |
| `well_formed(doc BLOB)` | `STRUCT(ok BOOL, error VARCHAR, kind VARCHAR)` | `kind ∈ {truncated, length-mismatch, trailing-bytes, invalid-type, bad-cstring, bad-utf8, duplicate-key, nesting-limit, bad-decimal128, bad-subtype}`. |

### ObjectId & Timestamp helpers

| Function | Returns | Notes |
| --- | --- | --- |
| `objectid_timestamp(oid ANY)` | `TIMESTAMPTZ` | Embedded creation time (accepts 24-hex VARCHAR or 12-byte BLOB). |
| `objectid_hex(oid BLOB)` | `VARCHAR` | 12 bytes → 24-char hex. |
| `objectid_from_hex(hex VARCHAR)` | `BLOB` | 24-char hex → 12 bytes. |
| `timestamp_to_ts(ts STRUCT(t,i))` | `TIMESTAMPTZ` | The `t` (seconds) component of a BSON internal Timestamp (the oplog clock — **not** a wall-clock time). |
| `timestamp_parts(ts STRUCT(t,i))` | `STRUCT(t UINTEGER, i UINTEGER)` | The raw `(time, increment)` pair. |

### Table functions — concatenated / dump BSON

| Function | Returns | Notes |
| --- | --- | --- |
| `bson_seq(stream BLOB)` | `TABLE(idx BIGINT, doc BLOB)` | Split N concatenated length-prefixed BSON documents (mongodump body / oplog batch / GridFS reassembly) into rows. Stops cleanly at a trailing partial document. |
| `mongodump_read(glob VARCHAR)` | `TABLE(idx BIGINT, doc BLOB, file VARCHAR)` | Read a local-filesystem glob of `.bson` files and apply `bson_seq` per file. For cloud dumps use `read_blob('s3://…/*.bson') + bson_seq(content)`. |

The worker's build version is published as the catalog `implementation_version`
(read it via `duckdb_databases().tags`), not as a scalar function.

## BSON → DuckDB type mapping

`decode`/`to_json` target the self-describing Extended-JSON sink, so they render
every document. The typed-leaf surface (`field(…, as)`) maps:

| BSON type | DuckDB surface |
| --- | --- |
| Double / Int32 / Int64 | `DOUBLE` / `INTEGER` / `BIGINT` |
| String | `VARCHAR` |
| ObjectId | 24-hex `VARCHAR` (or 12-byte `BLOB`) |
| UTCDateTime (`0x09`) | `TIMESTAMPTZ` |
| Timestamp (`0x11`) | `STRUCT(t,i)` → `timestamp_to_ts` for a `TIMESTAMPTZ` |
| Decimal128 (`0x13`) | canonical decimal `VARCHAR` (cast `::DECIMAL(38,s)`); NaN/Inf/>38-digit keep the string |
| Binary `0x04` (UUID) | canonical `UUID` |
| Binary `0x00`/`0x05`/`0x06`…/user | `BLOB` (subtype surfaced by `type_of`) |
| Regex / DBPointer / Code-with-scope | Extended JSON wrappers |
| MinKey / MaxKey | `'$minKey'` / `'$maxKey'` sentinels |

## Robustness

Every decoder is wrapped per row and funnels through a bounded structural
validator (hard nesting limit, fully bounds-checked reads) **before** the `bson`
crate is allowed to allocate a value tree. A hostile blob — a lying
multi-gigabyte length prefix, a NUL-less cstring, a 5000-level-deep document —
fails its own row (`well_formed(ok=false, kind=…)`), it never OOMs or
stack-overflows the worker. A `proptest` zero-panic gate exercises the full
decoder surface on arbitrary and truncated bytes.

## Non-goals

- Any **live MongoDB connection** (that is the `mongo` community extension's job
  — `vgi-bson` is the offline/at-rest complement).
- **FLE / Queryable-Encryption decryption** — encrypted Binary subtypes (`0x06`)
  stay opaque `BLOB`; no key management, no crypto egress.
- BSON `$jsonSchema` validation; a resumable streaming cursor over a directory of
  dumps (use `mongodump_read`'s glob or `read_blob` + `bson_seq`).

## Building & testing

```sh
cargo build --release                       # the bson-worker binary
cargo test --workspace                      # unit + golden vectors + proptest fuzz
cargo clippy --all-targets -- -D warnings
./run_tests.sh                              # haybarn SQLLogic E2E (needs the vgi ext)
```

The release binary is used directly as a DuckDB `vgi` `LOCATION`:

```sql
ATTACH 'bson' (TYPE vgi, LOCATION '/path/to/bson-worker');
```

## License

MIT — see [LICENSE](LICENSE). Every dependency is permissive (the `bson` crate is
MIT; `serde_json`, `uuid`, `arrow` are MIT/Apache-2.0). Copyright 2026 Query Farm
LLC — https://query.farm
