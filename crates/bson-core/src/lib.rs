//! `bson-core` — pure-compute BSON structural decoder, Extended-JSON v2 codec,
//! and BSON type helpers for the `bson` VGI worker.
//!
//! This crate carries **no** Arrow or VGI dependency: it operates on `&[u8]` in
//! and Rust values / JSON strings out, so it is independently testable and
//! reusable. The worker crate (`bson-worker`) maps these results onto DuckDB's
//! Arrow type system.
//!
//! # Design discipline (untrusted input)
//!
//! Every decode path funnels through the bounded structural [`validate`] walker
//! (a hard [`validate::MAX_NESTING`] recursion limit, fully bounds-checked reads)
//! **before** the `bson` crate is allowed to allocate a recursive value tree. A
//! hostile blob — a lying multi-gigabyte length prefix, a NUL-less cstring, a
//! 5000-level-deep document — fails its own row with a classified
//! [`validate::DecodeError`]; it never OOMs or stack-overflows the worker. This
//! is the per-row error-capture contract.
//!
//! # Non-goals
//!
//! No live MongoDB connection, no FLE / Queryable-Encryption decryption
//! (encrypted Binary subtypes stay opaque `BLOB`), no `$jsonSchema` validation.

pub mod extjson;
pub mod field;
pub mod objectid;
pub mod seq;
pub mod typeinfo;
pub mod validate;
pub mod value;

/// The crate (and worker) semantic version string.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
