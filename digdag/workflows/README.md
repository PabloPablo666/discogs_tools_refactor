# Discogs Main Pipeline (Digdag)



## Run-based ingestion and active dataset publishing

This directory contains the main Discogs data pipeline, orchestrated with Digdag, responsible for producing the active Discogs dataset used by Trino consumers.

The pipeline follows a run-based lakehouse architecture with immutable snapshots and atomic publishing.

It behaves like a real production data platform, while remaining fully reproducible on a local machine.



## Overview

Each execution of the main pipeline:
	1.	Creates a new immutable run
	2.	Ingests and transforms Discogs dumps
	3.	Performs validation checks
	4.	Atomically publishes validated data
	5.	Produces permanent audit reports

No data is ever overwritten.

**Only a symbolic pointer is moved**.



## High-level architecture
```text
discogs_tools_refactor/
└── digdag/
    └── workflows/
        ├── main/      ← ingestion & publishing
        └── history/   ← historical analysis & KPIs
```
This README documents the main pipeline only.



## Core concepts

Run-based architecture

Every pipeline execution creates a unique run:
```text
$DISCOGS_DATA_LAKE/
└── _runs/
    └── <run_id>/
```
Example:
*2026-01__20260120_205549*

Each run is:
	•	immutable
	•	reproducible
	•	queryable later
	•	never modified after creation



## Active pointer (publish layer)

Consumers never read directly from _runs.

Instead, a symbolic link defines the active dataset:
*hive-data/active -> _runs/<run_id>*

This provides:
	•	zero-downtime publishing
	•	instant rollback
	•	stable Trino table locations

Only the pointer moves.
The data never does.



## Physical data layout

Each run snapshot contains Parquet-only datasets:
```text
_runs/<run_id>/
├── artists_v1_typed/
├── artist_aliases_v1_typed/
├── artist_memberships_v1_typed/
├── masters_v1_typed/
├── releases_v6/
├── labels_v10/
├── release_artists_v1/
├── release_label_xref_v1/
├── label_release_counts_v1/
├── genre/style xref tables
└── _reports/
```
All directories contain Parquet files only.



## Logical access (Trino)

Trino external tables always point to:
*file:/data/hive-data/active/...*

As a result:
	•	SQL never changes
	•	dashboards never change
	•	notebooks never change

Only the active pointer updates.

This mirrors how production lakehouses operate.



## Pipeline lifecycle

The Digdag workflow follows a strict sequence.

### 1. Preflight
	•	validate environment variables
	•	verify dump availability
	•	compute run_id

The run ID is generated once and propagated to all tasks.


### 2. Download (optional)
	•	downloads Discogs dumps by month
	•	idempotent
	•	skips existing files

**Safe to re-run.**


### 3. Ingest
	•	streaming XML parsing
	•	no full-file loading
	•	constant memory usage

Typed canonical datasets are written
	•	artists
	•	labels
	•	masters
	•	releases
	•	relationships

Each entity is written independently.


### 4. Warehouse build
Derived analytical tables are generated
	•	artist_name_map_v1
	•	release_artists_v1
	•	release_label_xref_v1
	•	label_release_counts_v1
	•	genre/style normalization tables

These tables are optimized for analytics, not raw storage.


### 5. Run-level sanity checks

Before promotion, filesystem-level checks are executed
	•	required datasets exist
	•	directories are not empty
	•	structural integrity

If any check fails, the run is aborted.

Nothing is published.


### 6. Promotion

If all checks pass:
*active -> _runs/<run_id>*

The previous pointer is preserved automatically:
*active__prev_<timestamp>*

### 7. Promotion guardrails

Promotion guardrails

Only runs executed with run_mode=prod and allow_promote=true
are allowed to modify the active dataset pointer.

All other executions produce fully valid run snapshots
but never affect published data.


### 8. Post-promotion Trino sanity report

After publishing, Trino-based validations are executed on the active dataset:
	•	row counts
	•	null ratios
	•	orphan keys
	•	duplicate identifiers
	•	cross-table consistency

Results are exported as CSV:
*_runs/<run_id>/_reports/trino_sanity_active_<timestamp>.csv*

This provides a permanent audit trail.



### Running the main pipeline

From the main/ workflows directory:

*SESSION="$(date -u '+%Y-%m-%d %H:%M:%S')"
digdag run main.dig \
  --session "$SESSION"*

  Notes:
  	•	each session produces a new run
  	•	re-running does not overwrite data
  	•	previous runs remain queryable



## Design guarantees

This pipeline provides:
	•	✅ immutable historical snapshots
	•	✅ atomic publishing
	•	✅ reproducibility
	•	✅ safe experimentation
	•	✅ instant rollback
	•	✅ auditability
	•	✅ separation of compute and storage

Infrastructure (Trino + Hive) can be destroyed and rebuilt at any time without touching the data.



## What this pipeline is not
	•	not overwrite-based ETL
	•	not “latest-only” ingestion
	•	not fragile filesystem scripting
	•	not a toy parser

It behaves like a real lakehouse ingestion system.



## Relationship with the history pipeline

The main pipeline produces data.

The history pipeline consumes completed runs to:
	•	register historical schemas
	•	compute KPIs
	•	track dataset growth
	•	generate longitudinal reports

Main never depends on history.
History depends on main.

That separation is intentional.



## Legal note

Discogs data is subject to Discogs licensing terms.

This project:
	•	does not distribute datasets
	•	does not ship dumps
	•	focuses purely on infrastructure and data engineering design patterns
