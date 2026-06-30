//! `bson_version()` — return the worker's version string.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
    ScalarFunction,
};
use vgi_rpc::{Result, RpcError};

pub struct BsonVersion;

impl ScalarFunction for BsonVersion {
    fn name(&self) -> &str {
        "bson_version"
    }

    fn metadata(&self) -> FunctionMetadata {
        let mut tags = crate::meta::object_tags(
            "BSON Worker Version",
            "Return the semantic version string of the running bson worker binary (the worker's \
             own build version, not the SDK/protocol version). The string is MAJOR.MINOR.PATCH \
             (e.g. '0.1.0'). Takes no arguments and is deterministic — it always returns the same \
             single VARCHAR value (never NULL) for a given build. Useful for diagnostics and \
             confirming which build is attached.",
            "Return the bson worker version string, e.g. `bson_version()` → '0.1.0'. \
             Argument-free, deterministic, single semver VARCHAR.",
            "version, build version, bson_version, diagnostics, worker version, semver",
        );
        tags.push((
            "vgi.example_queries".into(),
            "[{\"description\":\"Return the worker version string.\",\"sql\":\"SELECT bson.main.bson_version() AS version\"}]".into(),
        ));
        tags.push((
            "vgi.executable_examples".into(),
            r#"[
  {
    "description": "Return the worker version string.",
    "sql": "SELECT bson.main.bson_version() AS version"
  }
]"#
            .into(),
        ));
        FunctionMetadata {
            description: "Returns the bson worker version string".into(),
            return_type: Some(DataType::Utf8),
            examples: vec![FunctionExample {
                sql: "SELECT bson.main.bson_version();".into(),
                description: "Return the bson worker version string.".into(),
                expected_output: None,
            }],
            tags,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Utf8))
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let out: ArrayRef = Arc::new(StringArray::from(vec![bson_core::version(); rows]));
        RecordBatch::try_new(params.output_schema.clone(), vec![out])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}
