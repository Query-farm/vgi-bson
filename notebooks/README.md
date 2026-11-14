# BSON notebooks

[01 · What survives a BSON–JSON round trip?](01_types_through_a_json_round_trip.ipynb)
constructs twelve typed documents, renders canonical and relaxed Extended JSON,
and restores BSON through the real worker. Visible SQL compares leaf-value text,
leaf types and complete document bytes. Two embedded charts show the selected
mode's fidelity and the exact UTF-8 sizes of both JSON representations.

## Run locally or in Colab

[Open in Colab](https://colab.research.google.com/github/Query-farm/vgi-bson/blob/main/notebooks/01_types_through_a_json_round_trip.ipynb)
and run all. The bootstrap installs only the base
[shared package v0.3.9](https://github.com/Query-farm/vgi-example-notebooks/tree/v0.3.9),
which supplies the published catalog title/subtitle, canonical branding and SVG
presentation. Local publication adds `[runner]`; no local helper, fixture files,
MongoDB server or worker source is required. One notebook file runs independently.

Install Git and [uv](https://docs.astral.sh/uv/). From this directory:

```sh
uv sync --locked
uv run --locked vgi-notebooks 01_types_through_a_json_round_trip.ipynb --timeout 300
```

In an editor select `notebooks/.venv/bin/python`, restart and run all. The runner
creates its own temporary kernel specification. Parameters and analytical SQL stay
visible; Python setup, plots and cleanup are folded. GitHub renders linked SVG
assets; Jupyter and Colab reruns show native SVG.

The visible connection uses the public native release:

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'bson' (TYPE vgi,
 LOCATION 'github-auto://Query-farm/vgi-bson@v0.4.0');
```

VGI selects the platform archive, verifies its SHA256 sidecar and caches it. Native
verification used macOS arm64. The notebook lock pins Haybarn 2.0.0rc11; no engine
build is needed. Initial installation needs network access, then analysis uses
synthetic bytes in memory. The SQL connection starts with an empty temporary secret
directory and persistent secrets disabled. Dates are interpreted in UTC.

## One-parameter experiment

```sh
uv run --locked vgi-notebooks 01_types_through_a_json_round_trip.ipynb \
  --parameters '{"JSON_MODE":"relaxed"}' --output-dir artifacts/relaxed --timeout 300
```

Accepted modes are `canonical` (default) and `relaxed`. Both are calculated as fixed
reference paths. The parameter selects the detailed comparison, fidelity matrix and
outlined size bars. Inputs, type inventory, reference totals and JSON byte lengths
remain fixed. Canonical output preserves all twelve fixture documents byte-for-byte;
relaxed output preserves all twelve leaf-value strings but only ten leaf types and
complete byte sequences. The two small Int64 fields narrow to Int32. Reset to
`canonical` after comparing results.

Overrides require a separate destination, unknown names and source overwrites fail,
and unsuccessful runs preserve the saved default. Review PNGs, logs and experiment
copies are ignored; successful default outputs and linked SVG assets are committed.

## Provenance and interpretation

All input documents are original teaching examples embedded in SQL, with explicit
Extended JSON type wrappers. They cover Int32, small and large Int64, Double,
NaN, Decimal128, millisecond dates, ObjectId, generic binary, internal Timestamp,
explicit null and a nested integer. No database connection, real dump, personal
data or credentials are involved.

`from_json` constructs the original BSON bytes. A separate typed SQL BIGINT/STRUCT
control uses `encode` to produce the same Int64 bytes without starting from JSON;
`decode` returns canonical Extended JSON VARCHAR, rather than a dynamic SQL STRUCT.
The `bson_types` registry joins observed types to their byte/class/wrapper metadata,
which explains why the audit distinguishes numeric widths. `to_json` renders each mode, and
`from_json` restores the result. Mode and path arguments bind as constants in the
released worker; UNION branches and nested-path CASE branches supply literal
arguments. Leaf extraction returns text, while `type_of` reports the BSON type.
Complete BLOB equality additionally covers type tags, field order and all fields.

A small Int64 can become an Int32 after relaxed rendering because its plain JSON
number no longer carries an explicit width. Its displayed value can still match.
The canonical byte-identity checks apply to these constructed fixtures; they are
not an exhaustive guarantee for every BSON encoding, duplicate-key document or
noncanonical floating-point bit pattern.

Canonical Extended JSON does not sort document keys for universal content
hashing. The separate field-order control displays ordered `keys` lists and retains different
byte representations for differently ordered documents. A null/string/missing-field control also shows
why text alone is ambiguous: explicit BSON null and the string `"null"` both
render as text `null`, while their type names differ. An absent field returns SQL
NULL. `is_valid` and `well_formed` distinguish invalid bytes (false plus a failure
kind) from a missing document (SQL NULL) and an absent field in valid BSON (true).
They test byte structure, not application-schema validity.

JSON size means UTF-8 bytes of the rendered strings, including wrappers and
punctuation. It does not measure compressed storage, migration throughput or
codec latency. Date and internal Timestamp retain their distinct BSON types;
ObjectId is treated as a type example, without inferring real creation history.

## Coverage and validation

[coverage.json](coverage.json) verifies meaningful visible SQL for **10/17** named
public surfaces in both released/current interfaces, exceeding the required nine.
The covered names are `from_json`, `to_json`, `type_of`, `field`, `well_formed`,
`is_valid`, `bson_types`, `keys`, `encode` and `decode`. The two file/stream readers,
three ObjectId helpers and two Timestamp helpers remain explicitly uncovered.
This is one focused fidelity story rather than a claim to demonstrate every BSON task.

Complete actual function/table/view exports are retained without capability-based
filters; overloads and relation aliases count once. Current public baseline
`e1ae97c923a1c39cd54119eb9621101f7fac0fbe` advertises the same seventeen names.
Its separate raw COPY format support adds no named SQL function/table/view to those
exports. COPY is not demonstrated here, and the draft v0.5.0 release is not used.

The selected scalar adapters and underlying codec, validation, field and type logic
are unchanged between v0.4.0 and the current baseline; sequence integration differs.
Current catalog discovery used the existing validated native binary. Reviewed current
interface coverage is distinct from exact-current notebook execution: the analytical
notebook runs released v0.4.0. Worker source and Cargo dependencies remain unchanged.

Default and detached relaxed runs pass seven SQL checks, the displayed native encoding
control and all observed type joins. Four figures, default-preservation guards and
notebook lint were reviewed. A fresh Python 3.12 kernel also ran the full notebook
with pandas 2.2.3, Matplotlib 3.10.0, IPython 8.30.0, ipykernel 6.17.1 and
jupyter-client 7.4.9 preloaded. Base-only installation preserves those versions,
`pip check` passes and the runner stays out of that kernel. This is local compatibility
testing, not a manual hosted-Colab claim.

The green [source CI baseline](https://github.com/Query-farm/vgi-bson/actions/runs/43561487244)
is reused for this notebook-only change. No source rebuild, repeated worker test
suites, draft release publication or container deployment is needed.

The worker is MIT licensed. Official unchanged branding links to
[Query.Farm](https://query.farm/) and [VGI](https://query.farm/vgi/).
