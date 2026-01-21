Discogs History Pipeline (Digdag)

======================================

Overview

This pipeline manages historical Discogs dumps already ingested into the data lake.

Its purpose is to:

• register historical runs

• validate Trino schemas

• maintain an append-only registry

• compute comparable KPIs over time

• export consolidated CSV reports

This pipeline does not ingest data.

It operates only on existing runs located under:
$DISCOGS_DATA_LAKE/_runs/<run_id>

with one active run pointed by:
$DISCOGS_DATA_LAKE/active -> _runs/<run_id>

=====================================

Core Concepts

1)Run

A run represents a full Discogs dump and follows the naming convention:
YYYY-MM__YYYYMMDD_HHMMSS

Example:
2025-12__20260120_194923

2)Active Run

The active dataset is selected via symlink:
active -> _runs/<run_id>

The history pipeline never modifies the active run.

3)Trino schemas

Each historical run has its own dedicated schema:
discogs_r_<run_id with '-' replaced by '_'>

Example:
discogs_r_2025_12__20260120_194923

The schema discogs (without suffix) is reserved for the active run only.

=========================================

Pipeline Structure

history/
├── register_run_schema.dig
├── reconcile_register.dig
├── update_run_registry.dig
├── compute_kpis.dig
├── export_history_csv.dig
└── README.md

=========================================

Registry Objects

Schema:
hive.discogs_history

run_registry_events (append-only)

Event log describing the lifecycle of each run.

Tracks:
	•	detection of runs
	•	schema completeness
	•	active exclusion
	•	validation failures

This table is append-only and never updated.

run_registry_latest (VIEW)

Derived view exposing the current state of each run.

This is the authoritative source used by:
	•	KPI computation
	•	reporting
	•	historical comparisons

KPI Objects

kpi_snapshot_events (append-only)

Append-only log of KPI computation results.

Each row represents:
	•	one KPI
	•	for one run
	•	computed at a specific time


kpi_snapshot_latest (VIEW)

View selecting the latest valid KPI value per run and KPI name.

This view is used for reporting and CSV export.

===============================================

Logical Flow

_runs/
   ↓
register_run_schema
   ↓
reconcile_register
   ↓
update_run_registry
   ↓
compute_kpis
   ↓
export_history_csv

All steps are idempotent and can be safely re-executed.

===================================================

Workflow Details

1. register_run_schema

Purpose

Registers a single run in Trino by creating:
	•	dedicated schema
	•	external tables
	•	compatibility views

When to use
	•	immediately after parsing a new dump

Example:

SESSION="$(date -u '+%Y-%m-%d %H:%M:%S')"

digdag run register_run_schema.dig \
  -p run_id=2025-12__20260120_194923 \
  --session "$SESSION"

  2. reconcile_register

Purpose

Automatically scans all runs under _runs/ and:
	•	excludes active
	•	checks required datasets
	•	verifies sentinel table (releases_ref_v6)
	•	records run state

No tables are created.
This step is observational only.

Outputs
	•	appends events to run_registry_events
	•	updates run_registry_latest view

Example:

SESSION="$(date -u '+%Y-%m-%d %H:%M:%S')"

digdag run reconcile_register.dig \
  --session "$SESSION"

  3. update_run_registry

  (Integrated in reconcile logic)

  Maintains a full append-only audit trail of run validation events.

  Used for:
  	•	debugging
  	•	traceability
  	•	historical reconstruction

  4. compute_kpis

  Purpose

  Computes quantitative KPIs for each valid historical run.

  Examples:
  	•	number of releases
  	•	number of artists
  	•	number of labels
  	•	table row counts
  	•	coverage percentages
  	•	top label dominance
  	•	xref-derived metrics

  KPIs are:
  	•	computed via Trino
  	•	stored as events
  	•	consolidated through kpi_snapshot_latest

  Example

  SESSION="$(date -u '+%Y-%m-%d %H:%M:%S')"

digdag run compute_kpis.dig \
  --session "$SESSION"

  Safe mode for a single run:

  digdag run compute_kpis.dig \
  -p only_run_id=2025-12__20260120_194923 \
  --session "$SESSION"

  5. export_history_csv

  Purpose

  Exports consolidated KPI data into CSV files.

  Two outputs are generated:
  	•	long format (one row per KPI)
  	•	wide format (one row per run)

  Default output directory:

  _meta/discogs_history/reports/

  Files:
	•	history_kpis_long_latest.csv
	•	history_kpis_wide_latest.csv

Example

SESSION="$(date -u '+%Y-%m-%d %H:%M:%S')"

digdag run export_history_csv.dig \
  --session "$SESSION"

With timestamped filenames:

digdag run export_history_csv.dig \
  -p with_timestamp=true \
  --session "$SESSION"

===================================================

Design Principles
	•	append-only architecture
	•	no UPDATE statements
	•	no destructive operations
	•	Trino used strictly as compute engine
	•	Digdag used strictly as orchestrator
	•	Python used strictly for logic

Every workflow can be safely rerun.

====================================================

What This Pipeline Does NOT Do
	•	no dump download
	•	no XML parsing
	•	no ingestion
	•	no mutation of active
	•	no data deletion

It exists purely for history, validation, comparison, and analytics.

===================================================

Current Status

✔ stable registry
✔ validated schemas
✔ KPI v1 complete
✔ CSV reporting working
✔ ready for KPI v2 (style / genre / label analytics)

This pipeline provides a clean historical backbone for Discogs data evolution analysis.
