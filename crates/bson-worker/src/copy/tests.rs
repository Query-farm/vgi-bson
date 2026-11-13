use super::*;
use arrow_array::{BinaryArray, StringArray};
use arrow_schema::Field;
use std::sync::Arc;
use vgi::storage::{FsStorage, MemoryStorage};

fn batch(docs: &[Option<&[u8]>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            true,
        )])),
        vec![Arc::new(BinaryArray::from(docs.to_vec()))],
    )
    .unwrap()
}

#[test]
fn export_preserves_bytes_and_order_across_storage_handles_and_shards() {
    let dir = tempfile::tempdir().unwrap();
    let writer_store = FsStorage::new();
    // FsStorage handles share a process-independent temp root. Use a unique
    // execution scope so concurrent test runs cannot overwrite each other.
    let scope_name = dir.path().to_string_lossy();
    let scope = scope_name.as_bytes();
    let document =
        bson_core::extjson::from_json(&format!("{{\"s\":\"{}\"}}", "x".repeat(1024 * 1024)))
            .unwrap();
    let small = bson_core::extjson::from_json(
        r#"{"_id":{"$oid":"54759eb3c090d83494e2d804"},"n":{"$numberLong":"42"}}"#,
    )
    .unwrap();
    stage_batch(
        &writer_store,
        scope,
        &batch(&[Some(document.as_slice()); 9]),
        DEFAULT_MAX_DOCUMENT_SIZE,
    )
    .unwrap();
    stage_batch(
        &writer_store,
        scope,
        &batch(&[Some(&small)]),
        DEFAULT_MAX_DOCUMENT_SIZE,
    )
    .unwrap();
    assert!(Manifest::load(&writer_store, scope).unwrap().shards >= 3);
    drop(writer_store);
    let coordinator_store = FsStorage::new();
    let dest = dir.path().join("out.bson");
    assert_eq!(
        finish(&coordinator_store, scope, dest.to_str().unwrap()).unwrap(),
        10
    );
    let mut expected = document.repeat(9);
    expected.extend_from_slice(&small);
    assert_eq!(std::fs::read(dest).unwrap(), expected);
    coordinator_store.clear(scope);
}

#[test]
fn invalid_exports_do_not_touch_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bson");
    std::fs::write(&dest, b"existing content").unwrap();
    let store = MemoryStorage::new();
    let empty = [5, 0, 0, 0, 0];
    for docs in [
        vec![Some(empty.as_slice()), None],
        vec![Some(&[5, 0, 0, 0, 1][..])],
        vec![Some(&[5, 0, 0, 0, 0, 0][..])],
    ] {
        assert!(stage_batch(&store, b"invalid", &batch(&docs), DEFAULT_MAX_DOCUMENT_SIZE).is_err());
        assert_eq!(std::fs::read(&dest).unwrap(), b"existing content");
    }
    // Missing staged data must also fail before publication and remove the temp file.
    stage_batch(
        &store,
        b"missing",
        &batch(&[Some(&empty)]),
        DEFAULT_MAX_DOCUMENT_SIZE,
    )
    .unwrap();
    store.kv_del(b"missing", &shard_key(0));
    assert!(finish(&store, b"missing", dest.to_str().unwrap()).is_err());
    assert_eq!(std::fs::read(&dest).unwrap(), b"existing content");
    stage_batch(
        &store,
        b"manifest",
        &batch(&[Some(&empty)]),
        DEFAULT_MAX_DOCUMENT_SIZE,
    )
    .unwrap();
    store.kv_del(b"manifest", MANIFEST);
    assert!(finish(&store, b"manifest", dest.to_str().unwrap()).is_err());
    assert_eq!(std::fs::read(&dest).unwrap(), b"existing content");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn export_preserves_nan_payload_bits() {
    // Re-encoding through Extended JSON would erase this NaN's payload.
    let mut doc = vec![16, 0, 0, 0, 1, b'x', 0];
    doc.extend_from_slice(&0x7ff8_0000_0000_0001_u64.to_le_bytes());
    doc.push(0);
    let store = MemoryStorage::new();
    stage_batch(
        &store,
        b"nan",
        &batch(&[Some(&doc)]),
        DEFAULT_MAX_DOCUMENT_SIZE,
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nan.bson");
    assert_eq!(finish(&store, b"nan", path.to_str().unwrap()).unwrap(), 1);
    assert_eq!(std::fs::read(path).unwrap(), doc);
}

#[test]
fn empty_export_replaces_destination_with_empty_file() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bson");
    std::fs::write(&dest, b"old bytes").unwrap();
    assert_eq!(
        finish(&MemoryStorage::new(), b"empty", dest.to_str().unwrap()).unwrap(),
        0
    );
    assert!(std::fs::read(dest).unwrap().is_empty());
}

#[test]
fn schemas_options_and_document_limits_are_enforced() {
    let store = MemoryStorage::new();
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["x"]))],
    )
    .unwrap();
    assert!(stage_batch(&store, b"schema", &wrong, DEFAULT_MAX_DOCUMENT_SIZE).is_err());
    let bytes = bson_core::extjson::from_json(r#"{"x":1}"#).unwrap();
    assert!(stage_batch(&store, b"limit", &batch(&[Some(&bytes)]), 5).is_err());
    let mut args = Arguments::default();
    args.named
        .insert("mode".into(), Arc::new(StringArray::from(vec!["typed"])));
    assert!(options(&args).is_err());
    assert!(raw_schema(&Schema::empty()).is_err());
    assert!(raw_schema(&Schema::new(vec![
        Field::new("a", DataType::Binary, true),
        Field::new("b", DataType::Binary, true)
    ]))
    .is_err());
}
