# Tests

This directory contains **lightweight pipeline validation scripts**.

These tests are **not unit tests** in the classical sense.  
They are **execution + sanity tests** designed to answer one question:

> *“If I run this pipeline on real Discogs data, does it still produce sane, queryable output?”*

---

## Why these tests exist

Discogs dumps are:
- Huge (millions of rows)
- Semi-structured
- Not guaranteed to be clean or stable across releases

Because of that, classic unit tests are mostly useless here.

What *does* matter is:
- The script runs end-to-end
- Parquet output is produced
- Schemas remain stable
- No critical nulls or broken relations are introduced

These tests provide **fast feedback** without pretending to be something they are not.

---

## What these tests do

Each `run_test_*.sh` script follows the same pattern:

1. **Run the corresponding pipeline script**  
   Using a real Discogs dump (XML or JSON)

2. **Write output to a temporary test directory**
   ```
   $DISCOGS_DATA_LAKE/_tmp_test/
   ```

3. **Perform basic sanity checks with DuckDB**
   - Row counts
   - Presence of key columns
   - Sample rows for manual inspection

4. **Print PASS / FAIL**
   - No mutation of production datasets
   - Safe to re-run at any time

---

## What these tests do NOT do

These tests intentionally do **not**:
- Assert exact row counts across Discogs versions
- Enforce referential integrity where Discogs itself does not
- Mock inputs or fabricate datasets
- Guarantee semantic correctness of Discogs metadata

They validate **pipeline stability**, not Discogs correctness.

---

## Available test scripts

| Script | Purpose |
|------|--------|
| `run_test_artists_v1.sh` | Validate artists extraction |
| `run_test_artist_relations.sh` | Validate aliases + group memberships |
| `run_test_labels_v10.sh` | Validate labels extraction |
| `run_test_masters_v1.sh` | Validate masters extraction |
| `run_test_releases_v6.sh` | Validate releases extraction |

---

## How to run a test

```bash
export DISCOGS_DATA_LAKE=/path/to/hive-data
./tests/run_test_artists_v1.sh /path/to/discogs_artists.xml.gz
```

Each script accepts the source dump path as its first argument.

---

## Philosophy

These tests exist to support:
- Refactoring
- Schema evolution
- Environment changes (local / Docker)
- Confidence before publishing or querying data

They are **engineering guardrails**, not academic exercises.

If a test fails, it means:
> “Something materially changed. Investigate before proceeding.”

That is exactly the signal we want.
