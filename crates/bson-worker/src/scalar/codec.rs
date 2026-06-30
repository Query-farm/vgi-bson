//! Core BSON codec scalars: `decode`, `to_json`, `from_json`, `encode`,
//! `is_valid`, `well_formed`, `type_of`, `keys`.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;
use bson_core::extjson::{self, JsonMode};
use bson_core::field::Coerce;
use bson_core::{field, typeinfo, validate};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::{Result, RpcError};

use crate::arrow_io::{self, blob_bytes};
use crate::blob_scalar;
use crate::value_in::encode_document;

fn ve(e: impl std::fmt::Display) -> RpcError {
    RpcError::value_error(e.to_string())
}

// --- builders used by the macro-generated scalars -------------------------

fn build_is_valid(rows: &[Option<&[u8]>]) -> Result<ArrayRef> {
    let col: Vec<Option<bool>> = rows.iter().map(|b| b.map(validate::is_valid)).collect();
    Ok(arrow_io::bool_opt_array(&col))
}

fn build_well_formed(rows: &[Option<&[u8]>]) -> Result<ArrayRef> {
    let mut ok = Vec::with_capacity(rows.len());
    let mut error = Vec::with_capacity(rows.len());
    let mut kind = Vec::with_capacity(rows.len());
    let mut valid = Vec::with_capacity(rows.len());
    for b in rows {
        match b {
            None => {
                ok.push(None);
                error.push(None);
                kind.push(None);
                valid.push(false);
            }
            Some(bytes) => {
                let wf = validate::well_formed(bytes);
                ok.push(Some(wf.ok));
                error.push(wf.error);
                kind.push(wf.kind);
                valid.push(true);
            }
        }
    }
    Ok(arrow_io::well_formed_array(&ok, &error, &kind, valid))
}

fn build_keys(rows: &[Option<&[u8]>]) -> Result<ArrayRef> {
    let col: Vec<Option<Vec<String>>> = rows
        .iter()
        .map(|b| b.and_then(|bytes| typeinfo::keys(bytes).ok()))
        .collect();
    Ok(arrow_io::list_string_array(&col))
}

// --- macro-generated scalars ----------------------------------------------

blob_scalar! {
    struct IsValid,
    sql_name = "is_valid",
    ret = DataType::Boolean,
    arg_doc = "A BLOB to test for BSON well-formedness.",
    description = "Return true iff the blob is exactly one well-formed BSON document",
    title = "BSON Is-Valid",
    doc_llm = "Return TRUE iff the blob is exactly one well-formed BSON document per the BSON spec \
        (declared length matches the bytes, types known, cstrings NUL-terminated, valid UTF-8, no \
        trailing bytes). A cheap structural pass — it never allocates a value tree. Never errors \
        or panics: a malformed/hostile blob simply returns FALSE; a NULL input returns NULL. See \
        `well_formed` for the failure reason.",
    doc_md = "TRUE iff the blob is one well-formed BSON document. Total (never throws). See \
        `well_formed` for the reason on failure.",
    keywords = "bson, valid, is_valid, well-formed, validate, structural, check, triage",
    examples = "[{\"description\":\"Validate a tiny empty BSON document (05 00 00 00 00).\",\"sql\":\"SELECT bson.main.is_valid(from_hex('0500000000')) AS ok\"}]",
    build = build_is_valid,
}

blob_scalar! {
    struct WellFormed,
    sql_name = "well_formed",
    ret = arrow_io::well_formed_type(),
    arg_doc = "A BLOB to diagnose for BSON well-formedness.",
    description = "Diagnose BSON well-formedness: STRUCT(ok BOOL, error VARCHAR, kind VARCHAR)",
    title = "BSON Well-Formed Diagnosis",
    doc_llm = "Diagnose a BSON blob and return STRUCT(ok BOOL, error VARCHAR, kind VARCHAR). \
        `kind` is one of truncated, length-mismatch, trailing-bytes, invalid-type, bad-cstring, \
        bad-utf8, duplicate-key, nesting-limit, bad-decimal128, bad-subtype (NULL when ok). \
        Stricter than `is_valid`: it also flags duplicate document keys and trailing bytes. Never \
        errors or panics on hostile input — a lying gigabyte length prefix or a 5000-deep document \
        returns ok=false with the classified reason, so a bad row never crashes the scan.",
    doc_md = "Diagnose well-formedness → `STRUCT(ok, error, kind)`. `kind` ∈ {truncated, \
        length-mismatch, trailing-bytes, invalid-type, bad-cstring, bad-utf8, duplicate-key, \
        nesting-limit, bad-decimal128, bad-subtype}.",
    keywords = "bson, well_formed, validate, error, kind, truncated, length-mismatch, \
        duplicate-key, robustness, triage",
    examples = "[{\"description\":\"Diagnose a truncated document.\",\"sql\":\"SELECT (bson.main.well_formed(from_hex('0500000001'))).kind AS kind\"}]",
    build = build_well_formed,
}

blob_scalar! {
    struct Keys,
    sql_name = "keys",
    ret = DataType::List(std::sync::Arc::new(arrow_schema::Field::new("item", DataType::Utf8, true))),
    arg_doc = "A BSON-encoded BLOB whose top-level field names to list.",
    description = "List the top-level field names of a BSON document in document order",
    title = "BSON Keys",
    doc_llm = "Return the top-level field names of a BSON document as a LIST<VARCHAR>, in document \
        order (BSON preserves insertion order). Use it to discover the shape of a collection's \
        documents or to drive dynamic projection. Returns NULL for a malformed blob.",
    doc_md = "Top-level field names → `LIST<VARCHAR>` in document order.",
    keywords = "bson, keys, field names, columns, shape, schema, discovery",
    examples = "[{\"description\":\"List the keys of a {a:1,b:2} document.\",\"sql\":\"SELECT bson.main.keys(bson.main.from_json('{\\\"a\\\":1,\\\"b\\\":2}')) AS k\"}]",
    build = build_keys,
}

// --- to_json (canonical / relaxed Extended JSON v2) -----------------------

/// `to_json(doc, mode := 'canonical')` — Extended JSON v2.
pub struct ToJson {
    /// Whether this overload accepts the optional positional `mode` argument.
    pub with_mode: bool,
}

impl ScalarFunction for ToJson {
    fn name(&self) -> &str {
        "to_json"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON → Extended JSON v2",
            "Render a BSON document as MongoDB Extended JSON v2. `mode` is 'canonical' (default — \
             the type-preserving, lossless form that wraps every typed value: {\"$oid\":…}, \
             {\"$numberDecimal\":…}, {\"$date\":{\"$numberLong\":…}}, {\"$timestamp\":{t,i}}, \
             {\"$binary\":{base64,subType}}, {\"$minKey\":1}) or 'relaxed' (human-readable — \
             in-range numbers and dates render natively, only out-of-range/typed values keep \
             wrappers). Canonical is the migration/diff surface and round-trips byte-identically \
             through from_json. to_json always succeeds on a well-formed document; returns NULL \
             for a malformed blob.",
            "Render BSON as Extended JSON v2. `mode` ∈ {canonical (lossless, default), relaxed \
             (readable)}.",
            "bson, to_json, extended json, extjson, canonical, relaxed, mongodb, oid, decimal128, \
             date, timestamp, binary, lossless, migration",
        );
        let example = if self.with_mode {
            "[{\"description\":\"Relaxed Extended JSON of {a:1}.\",\"sql\":\"SELECT bson.main.to_json(bson.main.from_json('{\\\"a\\\":1}'), 'relaxed') AS j\"}]"
        } else {
            "[{\"description\":\"Canonical Extended JSON of {a:1}.\",\"sql\":\"SELECT bson.main.to_json(bson.main.from_json('{\\\"a\\\":1}')) AS j\"}]"
        };
        tags.push(("vgi.example_queries".into(), example.into()));
        FunctionMetadata {
            description: if self.with_mode {
                "Render a BSON document as Extended JSON v2 with an explicit mode (canonical / relaxed)"
            } else {
                "Render a BSON document as canonical Extended JSON v2"
            }
            .into(),
            return_type: Some(DataType::Utf8),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut specs = vec![ArgSpec::any_column(
            "doc",
            0,
            "A BSON-encoded BLOB to render as Extended JSON.",
        )];
        if self.with_mode {
            specs.push(ArgSpec::const_arg(
                "mode",
                1,
                "varchar",
                "Extended JSON mode: 'canonical' (default, lossless) or 'relaxed' (readable).",
            ));
        }
        specs
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        if let Some(mode) = params.arguments.const_str(1) {
            JsonMode::parse(&mode).map_err(ve)?;
        }
        Ok(BindResponse::result(DataType::Utf8))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let mode = params
            .arguments
            .const_str(1)
            .map(|m| JsonMode::parse(&m).map_err(ve))
            .transpose()?
            .unwrap_or(JsonMode::Canonical);
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(blob_bytes(col, i)?.and_then(|b| extjson::to_json(b, mode).ok()));
        }
        let arr = arrow_io::string_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// --- decode (auto/struct/map/json → Extended JSON in v1) ------------------

/// `decode(doc, mode := 'auto')` — decode to the richest form. Because a DuckDB
/// scalar fixes its output column type at bind with no data sample, every mode
/// currently returns canonical Extended JSON text (the lossless, stable column).
pub struct Decode {
    /// Whether this overload accepts the optional positional `mode` argument.
    pub with_mode: bool,
}

impl ScalarFunction for Decode {
    fn name(&self) -> &str {
        "decode"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Decode",
            "Decode a BSON document to its richest self-describing form. The optional `mode` is \
             'auto' (default), 'struct', 'map', or 'json'. NOTE: a DuckDB scalar fixes its output \
             column type at bind time with no data sample available, so this worker returns \
             canonical Extended JSON text for every mode (the lossless, stable column type that \
             preserves ObjectId / Decimal128 / UUID / Timestamp-vs-DateTime). For a typed \
             projection of a known shape, cast the JSON or use `field(doc, path, as)` per leaf. \
             Returns NULL for a malformed blob.",
            "Decode BSON to canonical Extended JSON (the stable lossless column). `mode` ∈ {auto, \
             struct, map, json} is accepted; all currently return Extended JSON text.",
            "bson, decode, struct, map, json, extended json, deserialize, objectid, decimal128",
        );
        let example = if self.with_mode {
            "[{\"description\":\"Decode forcing JSON mode.\",\"sql\":\"SELECT bson.main.decode(bson.main.from_json('{\\\"a\\\":1}'), 'json') AS d\"}]"
        } else {
            "[{\"description\":\"Decode a document to Extended JSON.\",\"sql\":\"SELECT bson.main.decode(bson.main.from_json('{\\\"a\\\":1}')) AS d\"}]"
        };
        tags.push(("vgi.example_queries".into(), example.into()));
        FunctionMetadata {
            description: if self.with_mode {
                "Decode a BSON document to its richest form (Extended JSON in v1), with an explicit mode"
            } else {
                "Decode a BSON document to its richest form (Extended JSON in v1)"
            }
            .into(),
            return_type: Some(DataType::Utf8),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut specs = vec![ArgSpec::any_column(
            "doc",
            0,
            "A BSON-encoded BLOB to decode.",
        )];
        if self.with_mode {
            specs.push(ArgSpec::const_arg(
                "mode",
                1,
                "varchar",
                "Decode mode: 'auto' (default), 'struct', 'map', or 'json'. All currently produce \
                 Extended JSON text (see the function note).",
            ));
        }
        specs
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        if let Some(mode) = params.arguments.const_str(1) {
            validate_decode_mode(&mode)?;
        }
        Ok(BindResponse::result(DataType::Utf8))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(
                blob_bytes(col, i)?.and_then(|b| extjson::to_json(b, JsonMode::Canonical).ok()),
            );
        }
        let arr = arrow_io::string_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

fn validate_decode_mode(mode: &str) -> Result<()> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "auto" | "struct" | "map" | "json" => Ok(()),
        other => Err(ve(format!(
            "decode: unknown mode '{other}' (expected auto | struct | map | json)"
        ))),
    }
}

// --- from_json (Extended JSON → BSON bytes) -------------------------------

pub struct FromJson;

impl ScalarFunction for FromJson {
    fn name(&self) -> &str {
        "from_json"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "Extended JSON → BSON",
            "Parse a MongoDB Extended JSON v2 string (canonical OR relaxed) into BSON document \
             bytes. Interprets the typed wrappers ({\"$oid\":…}, {\"$numberDecimal\":…}, \
             {\"$date\":…}, {\"$binary\":…}, {\"$timestamp\":…}, …). Round-trips \
             decode∘encode∘to_json('canonical') to byte-identity on canonical inputs. Returns NULL \
             on invalid JSON or a non-document top-level value.",
            "Encode Extended JSON v2 text as BSON document bytes. Inverse of `to_json`.",
            "bson, from_json, encode, extended json, extjson, oid, decimal128, serialize, mongodb",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Encode a JSON document to BSON and hex it.\",\"sql\":\"SELECT to_hex(bson.main.from_json('{\\\"a\\\":1}')) AS h\"}]".into(),
        ));
        FunctionMetadata {
            description: "Encode an Extended JSON v2 string as BSON document bytes".into(),
            return_type: Some(DataType::Binary),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column(
            "extjson",
            0,
            "An Extended JSON v2 string (VARCHAR) to encode as a BSON document.",
        )]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Binary))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let v = blob_bytes(col, i)?
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| extjson::from_json(s).ok());
            out.push(v);
        }
        let arr = arrow_io::binary_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// --- encode (DuckDB value → BSON bytes) -----------------------------------

pub struct Encode;

impl ScalarFunction for Encode {
    fn name(&self) -> &str {
        "encode"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Encode",
            "Encode a DuckDB value as a BSON document. STRUCT → embedded document; MAP → document; \
             LIST → array; TIMESTAMP/TIMESTAMPTZ → UTCDateTime; UUID → Binary subtype 0x04; BLOB → \
             Binary subtype 0x00; integers → Int32/Int64 by range; DOUBLE → Double; DECIMAL → \
             Decimal128. The top-level value MUST be a document (a STRUCT or MAP) — BSON has no \
             top-level scalar, so a bare scalar raises an error. Returns NULL for a NULL input.",
            "Encode a DuckDB STRUCT/MAP as BSON document bytes. The inverse of `decode` / the typed \
             companion of `from_json`.",
            "bson, encode, serialize, struct, map, list, timestamp, uuid, decimal, document",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Encode a struct to BSON and hex it.\",\"sql\":\"SELECT to_hex(bson.main.encode({'a': 1, 'b': 'x'})) AS h\"}]".into(),
        ));
        FunctionMetadata {
            description: "Encode a DuckDB STRUCT/MAP value as a BSON document".into(),
            return_type: Some(DataType::Binary),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column(
            "value",
            0,
            "The DuckDB value to encode as a BSON document. Must be a STRUCT or MAP at the top \
             level (BSON documents have no top-level scalar).",
        )]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Binary))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(encode_document(col, i)?);
        }
        let arr = arrow_io::binary_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// --- field (dotted-path extract + typed coercion) -------------------------

/// `field(doc, path, as := NULL)` — extract one field by dotted path, coercing
/// the leaf to a cast-ready VARCHAR per the optional `as` mode.
pub struct Field {
    /// Whether this overload accepts the optional positional `as` argument.
    pub with_as: bool,
}

impl ScalarFunction for Field {
    fn name(&self) -> &str {
        "field"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Field Extract",
            "Extract one field from a BSON document by dotted path ('o._id', 'items.0.sku') \
             without materializing the whole document, returning a VARCHAR. The optional `as` \
             argument coerces the leaf to a cast-ready string: 'objectid' → 24-hex, 'decimal' → \
             the canonical decimal literal (cast with ::DECIMAL(38,4)), 'uuid' → canonical UUID \
             (cast with ::UUID), 'datetime'/'timestamp' → RFC 3339 (cast with ::TIMESTAMPTZ), \
             'blob' → lowercase hex, 'json' → canonical Extended JSON. With no `as` (or NULL), the \
             leaf is inferred (scalars render bare). A missing path → NULL. Lossless typed \
             projection a naive JSON path would corrupt.",
            "Extract a field by dotted path → VARCHAR. `as` ∈ {objectid, decimal, uuid, timestamp, \
             datetime, blob, json} coerces the leaf to a cast-ready string; NULL infers.",
            "bson, field, path, extract, project, objectid, decimal, uuid, datetime, timestamp, \
             dotted path, typed",
        );
        let example = if self.with_as {
            "[{\"description\":\"Extract _id as a hex ObjectId.\",\"sql\":\"SELECT bson.main.field(bson.main.from_json('{\\\"_id\\\":{\\\"$oid\\\":\\\"54759eb3c090d83494e2d804\\\"}}'), '_id', 'objectid') AS id\"}]"
        } else {
            "[{\"description\":\"Extract a nested field by path.\",\"sql\":\"SELECT bson.main.field(bson.main.from_json('{\\\"o\\\":{\\\"sku\\\":\\\"abc\\\"}}'), 'o.sku') AS sku\"}]"
        };
        tags.push(("vgi.example_queries".into(), example.into()));
        FunctionMetadata {
            description: if self.with_as {
                "Extract a BSON field by dotted path, coercing the leaf to a typed string via `as`"
            } else {
                "Extract a BSON field by dotted path (inferred leaf rendering)"
            }
            .into(),
            return_type: Some(DataType::Utf8),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut specs = vec![
            ArgSpec::any_column("doc", 0, "A BSON-encoded BLOB to extract from."),
            ArgSpec::const_arg(
                "path",
                1,
                "varchar",
                "A dotted field path with numeric array indices, e.g. 'o._id' or 'items.0.sku'.",
            ),
        ];
        if self.with_as {
            specs.push(ArgSpec::const_arg(
                "as",
                2,
                "varchar",
                "Leaf coercion: 'objectid', 'decimal', 'uuid', 'timestamp', 'datetime', 'blob', \
                 or 'json'. Omit (NULL) to infer.",
            ));
        }
        specs
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        if let Some(as_mode) = params.arguments.const_str(2) {
            Coerce::parse(&as_mode).map_err(ve)?;
        }
        Ok(BindResponse::result(DataType::Utf8))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let path = params
            .arguments
            .const_str(1)
            .ok_or_else(|| ve("field: a constant path argument is required"))?;
        let mode = params
            .arguments
            .const_str(2)
            .map(|m| Coerce::parse(&m).map_err(ve))
            .transpose()?
            .unwrap_or(Coerce::Infer);
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let v = match blob_bytes(col, i)? {
                Some(b) => field::field(b, &path, mode).unwrap_or(None),
                None => None,
            };
            out.push(v);
        }
        let arr = arrow_io::string_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// --- type_of (BSON type name at a path) -----------------------------------

/// `type_of(doc, path := NULL)` — the BSON type name at `path`.
pub struct TypeOf {
    /// Whether this overload accepts the optional positional `path` argument.
    pub with_path: bool,
}

impl ScalarFunction for TypeOf {
    fn name(&self) -> &str {
        "type_of"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Type Of",
            "Return the BSON type name at a dotted path: 'objectId', 'decimal128', 'timestamp', \
             'date', 'binData:uuid', 'binData:generic', 'binData:encrypted', 'minKey', 'int', \
             'long', 'double', 'string', 'object', 'array', … . With no `path` (or NULL) it \
             returns the top-level type ('object'). The binData subtype is surfaced after the \
             colon, so you can tell a UUID, an encrypted (FLE) blob, or a generic blob apart. A \
             missing path → NULL. Returns NULL for a malformed blob.",
            "BSON type name at a path, e.g. 'objectId', 'decimal128', 'binData:uuid'. NULL path → \
             top-level 'object'.",
            "bson, type_of, type, binData, subtype, objectId, decimal128, timestamp, date, peek",
        );
        let example = if self.with_path {
            "[{\"description\":\"Type of a nested ObjectId field.\",\"sql\":\"SELECT bson.main.type_of(bson.main.from_json('{\\\"_id\\\":{\\\"$oid\\\":\\\"54759eb3c090d83494e2d804\\\"}}'), '_id') AS t\"}]"
        } else {
            "[{\"description\":\"Top-level type of a document.\",\"sql\":\"SELECT bson.main.type_of(bson.main.from_json('{\\\"a\\\":1}')) AS t\"}]"
        };
        tags.push(("vgi.example_queries".into(), example.into()));
        FunctionMetadata {
            description: if self.with_path {
                "Return the BSON type name at a dotted path"
            } else {
                "Return the top-level BSON type name of a document"
            }
            .into(),
            return_type: Some(DataType::Utf8),
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut specs = vec![ArgSpec::any_column(
            "doc",
            0,
            "A BSON-encoded BLOB to inspect.",
        )];
        if self.with_path {
            specs.push(ArgSpec::const_arg(
                "path",
                1,
                "varchar",
                "A dotted field path (e.g. 'o._id'); omit for the top-level document type.",
            ));
        }
        specs
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Utf8))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let path = params.arguments.const_str(1);
        let col = batch.column(0);
        let rows = batch.num_rows();
        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let v = match blob_bytes(col, i)? {
                Some(b) => typeinfo::type_of(b, path.as_deref()).unwrap_or(None),
                None => None,
            };
            out.push(v);
        }
        let arr = arrow_io::string_array(&out);
        RecordBatch::try_new(params.output_schema.clone(), vec![arr])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}
