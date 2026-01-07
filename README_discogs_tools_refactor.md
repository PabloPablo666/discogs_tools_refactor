# Discogs Data Lake · Pipelines & Tests

This repository contains **production-grade ingestion pipelines**, validation tests,
and SQL utilities used to build a **typed, reproducible Discogs data lake** stored as
**Parquet** and queried with **DuckDB** and **Trino**.

The focus is **correctness, reproducibility, and schema stability**, not quick hacks.

This repository is the **pipeline and validation layer** of the Discogs lakehouse.
Infrastructure and query serving live in a separate repository.

---

## 📦 What this repo is

A **refactored, testable pipeline layer** for ingesting Discogs dumps into a
**typed lakehouse layout**:

- Streaming XML → Parquet
- Canonical, typed schemas
- Deterministic outputs
- Explicit sanity tests
- Legacy scripts preserved for auditability

This repo does **not** ship data.  
It ships **code that produces data deterministically**.

---

## 🗂 Repository structure

discogs_tools_refactor/
├── pipelines/ # Maintained ingestion pipelines (typed outputs)
│ ├── extract_artists_v1.py
│ ├── extract_artist_relations_v1.py
│ ├── extract_masters_v1.py
│ ├── extract_releases_v6.py
│ ├── parse_labels_v10.py
│ └── rebuild_artist_name_map_v1.py
│
├── tests/ # Runnable sanity tests (DuckDB-based)
│ ├── run_test_artists_v1.sh
│ ├── run_test_artist_relations.sh
│ ├── run_test_masters_v1.sh
│ ├── run_test_labels_v10.sh
│ └── run_test_releases_v6.sh
│
├── sql/ # Analytical & sanity SQL (Trino / DuckDB)
│ ├── sanity_checks_trino.sql
│ └── 90_joined_showcase/ # Portfolio-style analytical queries
│
├── legacy/ # Known-good historical scripts (immutable)
│ ├── README.md
│ ├── RESTORE_GUIDE.md
│ └── *.py
│
└── README.md

yaml
Copy code

---

## 🧠 Design principles

- **Streaming only**  
  XML dumps are parsed incrementally. No full-file loads.

- **Typed-first schemas**  
  Canonical IDs are written as numeric types (`BIGINT`) where possible.
  Legacy string-based layouts are not used for new ingestion.

- **Schema stability > convenience**  
  Outputs are Trino/DuckDB friendly by design.
  No implicit type inference, no surprises.

- **Deterministic outputs**  
  Same input → same Parquet layout → same row counts.

- **Tests before trust**  
  Every pipeline has a runnable test producing:
  - row counts
  - null checks
  - referential sanity checks
  - small human-readable samples

---

## 🏗 Output datasets

All pipelines write to a **data lake root**, typically defined by:

DISCOGS_DATA_LAKE=/absolute/path/to/discogs_data_lake/hive-data

graphql
Copy code

### Canonical physical datasets (typed)

$DISCOGS_DATA_LAKE/
├── artists_v1_typed/
├── artist_aliases_v1_typed/
├── artist_memberships_v1_typed/
├── masters_v1_typed/
├── releases_v6/
├── labels_v10/
├── collection/
└── warehouse_discogs/
├── artist_name_map_v1/
├── release_artists_v1/
└── release_label_xref_v1/

kotlin
Copy code

These datasets are consumed by **Trino** as external tables and exposed via
logical views (`*_v1`) in the lakehouse layer.

### Test outputs

During tests, pipelines write to an isolated location:

$DISCOGS_DATA_LAKE/_tmp_test/

yaml
Copy code

Nothing touches production paths unless explicitly moved.

---

## Known upstream inconsistencies

Discogs data contains structural inconsistencies by design, including:

- artist aliases referencing missing artist IDs
- group memberships with partial metadata
- labels with parent references not resolvable in the same dump

These are upstream data characteristics, not pipeline errors.

Sanity tests are designed to:
- detect unexpected regressions
- quantify known anomalies
- prevent silent data corruption

---

## ▶️ Running a pipeline test

Example: **artists**

```bash
export DISCOGS_DATA_LAKE=/Users/you/discogs_data_lake/hive-data

./tests/run_test_artists_v1.sh \
  /Users/you/discogs_store/raw/artists/discogs_YYYYMMDD_artists.xml.gz
Each test script will:

run the pipeline into _tmp_test/

validate output using DuckDB

print row counts and sample rows

exit with PASS ✅ or fail hard

Same pattern applies to:

run_test_labels_v10.sh

run_test_masters_v1.sh

run_test_releases_v6.sh

Yes, releases is slow. That’s reality, not a bug.

🔍 Sanity checks (Trino)
High-level integrity checks live in:

pgsql
Copy code
sql/sanity_checks_trino.sql
They validate:

primary key expectations

null ratios

referential integrity (artists ↔ aliases, masters ↔ releases)

known Discogs inconsistencies (documented, not hidden)

Run them after loading Parquet into Hive/Trino.

🧪 Why DuckDB + Trino
DuckDB

fast

local

ideal for pipeline validation and tests

Trino

distributed SQL engine

validates schemas at scale

runs heavy analytical and showcase queries

models realistic data-engineering workloads

🧓 Legacy directory (important)
legacy/ contains known-good historical scripts.

They are:

kept unchanged

documented

used as a reference baseline

If a refactor diverges, legacy scripts exist to prove what used to work.

🚫 What this repo is NOT
not a Discogs scraper

not a web app

not a demo toy

not shipping data

It is pipeline and validation infrastructure.

👤 Author
Paolo Olivieri
Sound engineer → data engineering pipelines
Focus: correctness, reproducibility, and real-world data pain

📜 Notes
Discogs data is subject to Discogs licensing.
This repository focuses on pipelines, tests, and tooling, not redistribution.
