//! Untrusted-input hardening: the BSON decoders must NEVER panic on arbitrary or
//! truncated bytes. This is the per-row error-capture contract — a hostile blob
//! fails its own row, it never crashes the scan. Property-based with proptest.
//!
//! The full-decode path hands bytes to the `bson` crate only after the bounded
//! structural [`validate::precheck`] succeeds, so a hostile deeply-nested or
//! length-lying blob is rejected before it can allocate a recursive value tree.
//! These tests run on an explicit large-stack thread so the debug build's deep
//! recursion has headroom while exercising the exact production code paths.

use bson_core::field::Coerce;
use bson_core::{extjson, field, objectid, seq, typeinfo, validate};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

/// Run every decoder on `bytes`. None may panic; all must return Result/Option.
fn exercise_all(bytes: &[u8]) {
    let _ = validate::is_valid(bytes);
    let _ = validate::well_formed(bytes);
    let _ = validate::precheck(bytes);
    let _ = bson_core::value::decode_document(bytes);
    let _ = extjson::to_json(bytes, extjson::JsonMode::Canonical);
    let _ = extjson::to_json(bytes, extjson::JsonMode::Relaxed);
    let _ = typeinfo::type_of(bytes, None);
    let _ = typeinfo::type_of(bytes, Some("a.b.0"));
    let _ = typeinfo::keys(bytes);
    let _ = field::field(bytes, "_id", Coerce::ObjectId);
    let _ = field::field(bytes, "amount", Coerce::Decimal);
    let _ = field::field(bytes, "a.b.0", Coerce::Infer);
    let _ = objectid::objectid_hex(bytes);
    let _ = seq::split(bytes);
    if let Ok(s) = std::str::from_utf8(bytes) {
        let _ = extjson::from_json(s);
        let _ = objectid::objectid_from_hex(s);
    }
}

/// Run `f` on a thread with a generous stack and propagate any panic.
fn on_big_stack<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn arbitrary_bytes_never_panic() {
    on_big_stack(|| {
        let mut runner = TestRunner::new(Config::with_cases(4000));
        runner
            .run(&proptest::collection::vec(any::<u8>(), 0..512), |bytes| {
                exercise_all(&bytes);
                Ok(())
            })
            .unwrap();
    });
}

#[test]
fn truncations_never_panic() {
    on_big_stack(|| {
        // Start from a real document, then truncate at every length.
        let mut base = Vec::new();
        bson::doc! {
            "_id": bson::oid::ObjectId::new(),
            "amount": "1.5",
            "a": { "b": [1i32, 2i32, 3i32] },
            "s": "hello",
        }
        .to_writer(&mut base)
        .unwrap();
        for n in 0..base.len() {
            exercise_all(&base[..n]);
        }

        let mut runner = TestRunner::new(Config::with_cases(2000));
        runner
            .run(&proptest::collection::vec(any::<u8>(), 1..256), |bytes| {
                for n in 0..bytes.len() {
                    exercise_all(&bytes[..n]);
                }
                Ok(())
            })
            .unwrap();
    });
}

/// A hostile blob with extreme declared nesting must be rejected
/// (`nesting-limit` or `truncated`), not stack-overflow.
#[test]
fn deeply_nested_is_rejected_not_overflowing() {
    on_big_stack(|| {
        // 5000 array-open type bytes — each would recurse one level; the bounded
        // validator must reject without overflowing.
        let bytes = vec![0x04u8; 5000];
        let wf = validate::well_formed(&bytes);
        assert!(!wf.ok);
        exercise_all(&bytes);
    });
}

/// A length prefix declaring gigabytes over a tiny buffer must not OOM.
#[test]
fn lying_length_prefix_is_truncated() {
    on_big_stack(|| {
        let mut bytes = vec![0u8; 8];
        bytes[..4].copy_from_slice(&0x7fff_ffffi32.to_le_bytes());
        let wf = validate::well_formed(&bytes);
        assert_eq!(wf.kind.as_deref(), Some("truncated"));
        exercise_all(&bytes);
    });
}
