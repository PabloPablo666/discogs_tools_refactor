Discogs Data Lake · Pipelines, Runs & Validation

This repository contains the pipeline, validation and orchestration layer
of a local Discogs lakehouse, designed with run-based execution, immutable snapshots
and verifiable data promotion.

The goal is not simply to parse Discogs dumps, but to build a reproducible,
auditable data production system.

Infrastructure (Trino + Hive Metastore) lives in a separate repository.

================================================================================


What this repository is

A production-style data pipeline system that:
	•	ingests Discogs XML dumps via streaming parsers
	•	produces typed Parquet datasets
	•	validates outputs with automated tests
	•	builds analytical warehouse tables
	•	publishes data atomically via an active pointer
	•	generates permanent sanity reports

This repository does not ship data.

It ships deterministic code that produces versioned datasets.

================================================================================


High-level architecture

Discogs XML dumps
        ↓
Streaming ingestion pipelines
        ↓
Typed Parquet datasets
        ↓
Warehouse transformations
        ↓
Run-level validation
        ↓
Promotion (atomic pointer switch)
        ↓
Post-promotion Trino sanity report

Each execution is isolated in its own run directory.

================================================================================


Run-based design

Every pipeline execution creates an immutable snapshot:

hive-data/
└── _runs/
    └── YYYYMMDD_HHMMSS/
        ├── artists_v1_typed/
        ├── masters_v1_typed/
        ├── releases_v6/
        ├── labels_v10/
        ├── warehouse_discogs/
        └── _reports/

Nothing is overwritten.

Old runs are never modified.

================================================================================


Active dataset pointer

Consumers (Trino, SQL, analytics) never query _runs directly.

Instead, a single symbolic link is used:

hive-data/active -> _runs/20260117_192144

Promotion switches this pointer atomically.

Benefits:
	•	zero-downtime publishing
	•	instant rollback
	•	stable table locations
	•	reproducible historical runs

================================================================================


Repository structure

discogs_tools_refactor/
├── pipelines/          # Streaming ingestion & transforms
│   ├── extract_artists_v1.py
│   ├── extract_artist_relations_v1.py
│   ├── extract_masters_v1.py
│   ├── extract_releases_v6.py
│   ├── parse_labels_v10.py
│   └── rebuild_artist_name_map_v1.py
│
├── tests/              # DuckDB-based validation tests
│   ├── run_test_artists_v1.sh
│   ├── run_test_artist_relations.sh
│   ├── run_test_masters_v1.sh
│   ├── run_test_labels_v10.sh
│   └── run_test_releases_v6.sh
│
├── digdag/             # Orchestration workflows
│   ├── main.dig
│   ├── ingest.dig
│   ├── build.dig
│   ├── promote.dig
│   └── tests_*.dig
│
├── sql/                # Trino / DuckDB SQL
│   ├── sanity_report_active_v1.sql
│   └── showcase_queries/
│
├── legacy/             # Historical reference scripts
│   └── (immutable)
│
└── README.md

================================================================================


Design principles


1) Streaming only

XML dumps are processed incrementally.
No full-file memory loading.

2) Typed-first schemas
	•	numeric IDs where possible
	•	explicit column types
	•	Trino-safe schemas
	•	no implicit inference


3) Deterministic outputs

Same input dump → same parquet layout → same results.


4) Immutable runs

Data is never overwritten.
Only new runs are created.


5)Promotion, not overwrite

Publishing is explicit and reversible.


6) Tests before trust

Every run must pass:
	•	parquet-level sanity checks
	•	schema validation
	•	referential integrity checks

7) Reports after promotion


After promotion, Trino runs full SQL sanity checks and produces CSV reports.

These reports live alongside the run forever.

================================================================================


Output datasets

All data is written under a lake root:

DISCOGS_DATA_LAKE=/absolute/path/to/discogs_data_lake/hive-data


Canonical typed datasets

artists_v1_typed/
artist_aliases_v1_typed/
artist_memberships_v1_typed/
masters_v1_typed/
releases_v6/
labels_v10/
collection/


Warehouse datasets

warehouse_discogs/
├── artist_name_map_v1/
├── release_artists_v1/
├── release_label_xref_v1/
├── label_release_counts_v1/
├── release_style_xref_v1/
└── release_genre_xref_v1/

================================================================================


Validation strategy

DuckDB tests (run-level)

Used for:
	•	pipeline correctness
	•	regression detection
	•	fast feedback during development

Runs on isolated _tmp_test/ paths.


Trino sanity reports (active-level)

Executed after promotion:
	•	validates real query behavior
	•	checks cross-table integrity
	•	produces CSV audit reports

================================================================================


Known Discogs inconsistencies

Discogs data is not clean by design.

Examples:
	•	alias IDs not resolvable to artists
	•	partial group memberships
	•	label parent references missing

Tests distinguish between:
	•	expected upstream anomalies
	•	unexpected pipeline regressions

Nothing is silently ignored.

================================================================================


What this repository is NOT
	•	not a scraper
	•	not a downloader only
	•	not an overwrite-based ETL
	•	not a demo toy

It is a versioned data production system.

================================================================================


Notes

Discogs data is subject to Discogs licensing terms.

This repository contains code only, not datasets.
