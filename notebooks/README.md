# BSON notebooks

[01 · What survives a BSON–JSON round trip?](01_types_through_a_json_round_trip.ipynb)
constructs twelve typed documents, renders canonical and relaxed Extended JSON,
and restores BSON through the real worker. Visible SQL compares leaf-value text,
leaf types and complete document bytes. Two embedded charts show the selected
mode's fidelity and the exact UTF-8 sizes of both JSON representations.

## Run from this directory

Install [uv](https://docs.astral.sh/uv/), then:

```sh
cd notebooks
uv sync --locked
uv run --locked python execute.py
```

In a notebook editor select this directory's `.venv/bin/python` kernel and run
all cells. The full notebook directory also runs outside the worker checkout.
Python 3.13–3.14 is supported; `uv` can provision the interpreter. No Rust compiler,
local worker build, MongoDB server or root virtual environment is needed.

The visible connection uses the public native release:

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'bson' (TYPE vgi,
  LOCATION 'github-auto://Query-farm/vgi-bson@v0.4.0');
```

VGI selects the platform archive, verifies its SHA256 sidecar and caches it.
The notebook environment locks Haybarn 2.0.0rc11 and its Python dependencies.
Initial setup needs network access; the analysis operates only on synthetic
bytes in memory. This notebook uses scalar codecs from public v0.4.0; it does
not require the COPY functionality in the v0.5.0 draft release available when
it was authored. Change the literal tag and rerun both parameter settings when
adopting a newer public release.

Parameters, connection SQL, analytical SQL and useful outputs remain visible.
Imports, styling, plotting, prose-generation source and cleanup are folded.

## One-parameter experiment

```sh
uv run --locked python execute.py --parameters '{"JSON_MODE":"relaxed"}' \
  --output-dir validation/relaxed
```

Accepted modes are `canonical` (default) and `relaxed` (experiment). Both modes
are calculated as fixed reference paths. The parameter selects the detailed
comparison, fidelity matrix and outlined size bars. The source documents,
reference coverage table and JSON byte lengths remain fixed. Reset to
`canonical` after comparing outputs.

The runner uses fresh kernels, requires a separate destination for overrides,
rejects unknown parameters and source-directory overwrites, and replaces saved
notebooks only after successful execution. Copies include this guide, helpers,
branding and the locked environment. Extracted charts, logs and experiments in
`validation/` are ignored; the main notebook retains its outputs and charts.

## Provenance and interpretation

All input documents are original teaching examples embedded in SQL, with explicit
Extended JSON type wrappers. They cover Int32, small and large Int64, Double,
NaN, Decimal128, millisecond dates, ObjectId, generic binary, internal Timestamp,
explicit null and a nested integer. No database connection, real dump, personal
data or credentials are involved.

`from_json` constructs the original BSON bytes. `to_json` renders each mode, and
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
hashing. The separate field-order control retains different byte representations
for differently ordered documents. A null/string/missing-field control also shows
why text alone is ambiguous: explicit BSON null and the string `"null"` both
render as text `null`, while their type names differ. An absent field returns SQL
NULL; malformed input requires a separate document-quality check.

JSON size means UTF-8 bytes of the rendered strings, including wrappers and
punctuation. It does not measure compressed storage, migration throughput or
codec latency. Date and internal Timestamp retain their distinct BSON types;
ObjectId is treated as a type example, without inferring real creation history.

The official Strata Sun and VGI media-kit assets are unchanged and stored locally.
Linked Strata Sun branding appears at the start and final end of the notebook.

References: [worker documentation](https://github.com/Query-farm/vgi-bson),
[pinned release](https://github.com/Query-farm/vgi-bson/releases/tag/v0.4.0).
