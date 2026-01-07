# Discogs Lakehouse – Query Pack (Trino)

This folder contains **Trino SQL queries** designed as a **portfolio** for a Data Engineering project based on the Discogs data lake / lakehouse.  
Queries are organised by domain (artists, masters, releases, labels, memberships) and include a “showcase” section with end-to-end joins.

## Assumptions

- Catalog / schema: `hive.discogs`
- Canonical tables `*_v1` are **VIEWs** pointing to physical datasets `*_v1_typed` (typed and consistent IDs).
- Development environment: single-node Trino (limited memory).  
  For this reason, showcase queries **filter and aggregate early** to avoid combinatorial explosions.

---

## Folder structure

queries/
00_overview/
10_artists/
20_masters/
30_releases/
40_labels/
50_memberships/
90_joined_showcase/

---

## 00_overview

### `00_overview/01_dataset_overview.sql`
**Purpose:** quick overview of table volumes (row counts) to validate that the metastore points to the expected datasets.  
**Demonstrates:** reproducible smoke test and quantitative health check.  
**Tables:** `artists_v1_typed`, `masters_v1_typed`, `artist_aliases_v1_typed`, `artist_memberships_v1_typed`, `releases_ref_v6`, `labels_ref_v10`, `collection`.

---

## 10_artists

### `10_artists/01_artist_alias_coverage.sql`
**Purpose:** analyse alias coverage per artist (p50 / p90 / p99 alias counts) and total number of artists with aliases.  
**Demonstrates:** aggregations, approximate percentiles, data profiling.  
**Tables:** `artist_aliases_v1`.

---

## 20_masters

### `20_masters/01_masters_by_year.sql`
**Purpose:** distribution of masters by release year (filtered to a plausible range).  
**Demonstrates:** basic data cleaning, temporal analytics, grouping.  
**Tables:** `masters_v1`.

---

## 30_releases

### `30_releases/01_release_country_top.sql`
**Purpose:** top countries by number of releases.  
**Demonstrates:** aggregations and ranking on a clean dimensional column.  
**Tables:** `releases_ref_v6`.

### `30_releases/02_releases_styles_top.sql`
**Purpose:** most frequent musical styles, exploding the `styles` CSV-like column.  
**Demonstrates:** handling denormalised fields using `split` + `unnest`, aggregation.  
**Tables:** `releases_ref_v6`.

---

## 40_labels

### `40_labels/01_label_parent_rollup.sql`
**Purpose:** hierarchical roll-up of labels (parent → children).  
**Demonstrates:** analytics on hierarchical relationships, ranking.  
**Tables:** `labels_ref_v10`.

---

## 50_memberships

### `50_memberships/01_groups_with_most_members.sql`
**Purpose:** identify groups with the highest number of distinct members.  
**Demonstrates:** deduplication (`count(distinct ...)`), metrics on many-to-many relationships.  
**Tables:** `artist_memberships_v1`.

---

## 90_joined_showcase

> **Note:** queries in this folder are designed to demonstrate integration across Discogs entities  
> (release → artist → label, etc.).  
> On a single-node Trino setup, early filtering and aggregation are required to avoid memory errors.

### `90_joined_showcase/02_release_fact_rollup.sql`
**Purpose:** build a “fact-like” view at release level with aggregated artist and label dimensions.

**Demonstrates:**
- joins via bridge tables (`release_artists_v1`, `release_label_xref_v1`)
- name normalisation → ID resolution (`artist_name_map_v1`)
- early aggregation to avoid combinatorial explosion
- lakehouse pattern (fact + dimensions)

**Tables:**
- `releases_ref_v6` (base releases)
- `release_artists_v1` + `artist_name_map_v1` + `artists_v1`
- `release_label_xref_v1`

**Output:** one row per release with `artists[]` and `labels[]`.

---

### (Optional) `90_joined_showcase/03_release_fact_rollup_counts.sql`
**Purpose:** same as `02`, but with additional metrics (`n_artists`, `n_labels`) for improved readability and BI-friendliness.  
**Demonstrates:** metric enrichment, arrays plus distinct counts.  
**Tables:** same as `02`.

### (Optional) `90_joined_showcase/05_release_fact_metrics.sql`
**Purpose:** “materialisable” version (metrics only, no arrays), suitable for creating a real fact table (e.g. `release_fact_v1`).  
**Demonstrates:** metric-centric modelling, compact and stable output.  
**Tables:** `releases_ref_v6`, `release_artists_v1`, `artist_name_map_v1`, `release_label_xref_v1`.

---

## Conventions

- Showcase queries limit the base dataset (`LIMIT` or sampling) to remain executable in development.
- Where Discogs data is denormalised (artists, styles, labels), the approach is explicitly *best-effort*.
- Names `*_v1` are treated as a stable logical API; physical datasets are `*_v1_typed`.
