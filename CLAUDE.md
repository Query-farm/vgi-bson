# CLAUDE.md — vgi-bson

Guidance for working in this repo. It is a **VGI worker**: a standalone Rust
binary DuckDB launches and talks to over Apache Arrow IPC. It decodes/encodes
**BSON** under the catalog `bson`, schema `main`.

## Layout

- `crates/bson-core/` — **pure compute, no Arrow/VGI deps.** `&[u8]` in, Rust
  values / JSON strings out. Independently testable. Modules:
  - `validate.rs` — the hand-rolled, allocation-free structural BSON validator
    (the robustness centerpiece: bounded recursion, fully bounds-checked reads,
    classified `DecodeError` kinds). Behind `is_valid` / `well_formed`, and the
    framing oracle for `seq::split`. **Never panics.**
  - `value.rs` — safe full decode to `bson::Document` (runs `validate::precheck`
    first, so the `bson` crate never sees a stack-overflowing/length-lying blob).
  - `extjson.rs` — `to_json` (canonical/relaxed Extended JSON v2) + `from_json`.
  - `field.rs` — dotted-path extract + typed `as` coercion (cast-ready strings).
  - `objectid.rs` — hex↔bytes, embedded-timestamp, dependency-free RFC 3339.
  - `typeinfo.rs` — `type_of` (incl. `binData:<subtype>`), `keys`.
  - `seq.rs` — concatenated length-prefixed document splitter and incremental
    `DocumentReader<R: Read>` with size bounds and byte-offset errors.
- `crates/bson-worker/` — the Arrow/VGI adapter (the actual binary).
  - `main.rs` — catalog metadata + registration.
  - `arrow_io.rs` — Arrow cell readers + nullable column builders + shared STRUCT
    types (`well_formed`, `timestamp_parts`).
  - `value_in.rs` — Arrow cell → `bson::Bson` for the `encode` path.
  - `scalar/` — `common` (the `blob_scalar!` macro), `codec`
    (decode/to_json/from_json/encode/is_valid/well_formed/keys/field/type_of),
    `objectid`, `timestamp`.
  - `table/` — `seq` (`bson_seq`), `dump` (`mongodump_read`).
  - `file_scan.rs` — bounded Arrow batches over local files, shared by COPY FROM
    (strict) and `mongodump_read` (stop at corruption per file). HTTP continuation
    records the original file list and next unconsumed document position.
  - `copy.rs` — bidirectional `FORMAT '<catalog>.bson'`, one raw BLOB column.
    COPY TO uses an ordered sink, execution-scoped shared storage, and atomic
    destination publication; never keep sink state on the handler instance.
    vgi 0.51's COPY adapter does not forward continuation state; a resumable
    factory is registered first under the hidden COPY handler name. Keep HTTP
    multi-batch coverage when changing registration or upgrading the SDK.

## Hard rules

- **stdout is the Arrow-IPC channel.** All logs go to **stderr** (`env_logger`).
- **Per-row error capture, never panic.** A malformed/hostile blob yields a NULL
  output (or `well_formed(ok=false)`), never a crash that aborts the scan. New
  decode paths MUST funnel through `validate` (or `decode_document`) and be
  covered by the `proptest` zero-panic gate in `bson-core/tests/fuzz.rs`.
- **COPY is strict.** Malformed documents, duplicate keys, NULL export rows, and
  truncated input abort COPY. Errors identify a row or file/document offset.
  Raw COPY preserves bytes; typed COPY and implicit lossy conversions are out of
  scope. Validate both nonempty and empty-input schemas/options.
- **A DuckDB scalar's output type is fixed at BIND time** (no data sample). A
  generic "decode any BSON" scalar therefore CANNOT return a dynamic STRUCT — it
  returns canonical Extended JSON (VARCHAR). Reserve typed STRUCT/MAP output for
  fixed-schema results (`well_formed`, `timestamp_parts`).
- **Optional-arg scalars ship two arity overloads** (`with_mode`/`with_as`/
  `with_path`), because DuckDB binds a const argument as required.
- **Published deps only** — `vgi = "0.51.0"` (protocol 3.3, with `vgi-rpc = "0.32.4"`), arrow 59, `bson = "3.1"` (features
  `compat-3-0-0`, `serde`, `serde_json-1`). The `serde_json` `preserve_order`
  feature is load-bearing: it keeps document field order through the
  Extended-JSON parse so `from_json ∘ to_json('canonical')` is byte-identical
  (the migration guarantee). No `mongodb` driver crate (it would pull a
  network/async stack).
- **MIT license**, fleet-standard metadata (`vgi.title`/`doc_llm`/`doc_md`/
  `keywords` per object; catalog `source_url`; per-arg docs). `vgi-lint` gates at
  `--fail-on info`.

## Gates (all must be green)

```sh
cargo build --release
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace
./run_tests.sh        # haybarn SQLLogic E2E (LOAD vgi; ATTACH; assert examples)
```

`vgi-lint` runs in CI via `Query-farm/vgi-lint-check@v2` against the release
binary.

## E2E

`test/sql/basic.test` is a haybarn SQLLogic suite: `LOAD vgi;` (NOT
`require vgi`), `require-env VGI_BSON_WORKER`, `ATTACH`, then asserts the catalog
examples — including `bson_seq` and `mongodump_read` over the committed
`test/data/users.bson` fixture. `run_tests.sh` builds the worker and points the
runner at it.

`test/sql/copy.test` covers bidirectional format discovery, byte-identical file
round trips, multi-batch ordering with worker process rotation, empty files,
import rollback, failed-export destination preservation, and invalid schemas /
options. The malformed fixtures are deterministic: `truncated.bson` is 2,049
empty documents followed by two bytes of a length prefix; `malformed.bson` has
one empty document then a bad terminator; `oversized-prefix.bson` declares an
`i32::MAX`-byte document with no body.

## Releasing

Bump `[workspace.package] version` in `Cargo.toml`, sync `Cargo.lock`, tag
`vX.Y.Z`. `release.yml` gates on CI then calls the shared
`Query-farm/vgi-actions/.github/workflows/rust-release.yml@v1` (asset prefix
`vgi-bson`, bin `bson-worker`). The catalog `implementation_version` (built from
the workspace version) must equal the tag (`ci/check-version.sh`).
