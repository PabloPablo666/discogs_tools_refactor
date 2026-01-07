#!/usr/bin/env python3
"""
Build release_label_xref_v1 (+ optional label_release_counts_v1) from releases_v6 Parquet using DuckDB.

Why:
- Avoid Trino UNNEST over 18.6M rows (memory blow-ups).
- Pre-explode (release_id, label) once, store Parquet in warehouse_discogs/.

Input:
- <DISCOGS_DATA_LAKE>/releases_v6/*.parquet
  columns used: release_id, labels, label_catnos

Outputs:
- <DISCOGS_DATA_LAKE>/warehouse_discogs/release_label_xref_v1/data.parquet
  columns:
    release_id BIGINT
    label_name VARCHAR
    label_norm VARCHAR
    label_catno VARCHAR

- <DISCOGS_DATA_LAKE>/warehouse_discogs/label_release_counts_v1/data.parquet
  columns:
    label_norm VARCHAR
    label_name_sample VARCHAR
    n_total_releases BIGINT
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build release_label_xref_v1 (DuckDB → Parquet).")
    p.add_argument(
        "--data-lake",
        default=os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data"),
        help="Root hive-data directory. Default: $DISCOGS_DATA_LAKE or /data/hive-data",
    )
    p.add_argument(
        "--releases-glob",
        default=None,
        help="Override releases parquet glob. Default: <data-lake>/releases_v6/*.parquet",
    )
    p.add_argument(
        "--warehouse-subdir",
        default="warehouse_discogs",
        help="Warehouse subdir under data-lake. Default: warehouse_discogs",
    )
    p.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing output dirs before writing.",
    )
    p.add_argument(
        "--threads",
        type=int,
        default=4,
        help="DuckDB threads. Default: 4",
    )
    p.add_argument(
        "--memory",
        default=None,
        help="Optional DuckDB memory_limit, e.g. '6GB'. Default: unset",
    )
    return p.parse_args()


def rm_dir(p: Path) -> None:
    if p.exists():
        shutil.rmtree(p)


def main() -> int:
    args = parse_args()

    data_lake = Path(args.data_lake).expanduser().resolve()
    releases_glob = args.releases_glob or str(data_lake / "releases_v6" / "*.parquet")
    warehouse = data_lake / args.warehouse_subdir

    out_xref_dir = warehouse / "release_label_xref_v1"
    out_cnt_dir = warehouse / "label_release_counts_v1"
    out_xref_file = out_xref_dir / "data.parquet"
    out_cnt_file = out_cnt_dir / "data.parquet"

    warehouse.mkdir(parents=True, exist_ok=True)

    if args.clean:
        rm_dir(out_xref_dir)
        rm_dir(out_cnt_dir)

    out_xref_dir.mkdir(parents=True, exist_ok=True)
    out_cnt_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)};")
    if args.memory:
        con.execute(f"PRAGMA memory_limit='{args.memory}';")

    print("📥 Input releases glob:")
    print("   ", releases_glob)
    print("📦 Output:")
    print("   xref  ->", out_xref_file)
    print("   count ->", out_cnt_file)
    print()

    # NOTE:
    # releases_ref_v6 stores:
    # - labels: "Label A, Label B, ..."
    # - label_catnosnos: "CAT1, CAT2, ..."  (same order, best-effort)
    #
    # We explode both arrays, keep ordinality, then join by index.
    #
    # We also normalize label to label_norm = lower(trim(label_name))

    print("🧱 Building release_label_xref_v1 ...")

    con.execute(
        f"""
        COPY (
            WITH src AS (
                SELECT
                    release_id::BIGINT AS release_id,
                    coalesce(labels, '') AS labels_csv,
                    coalesce(label_catnos, '') AS catnos_csv
                FROM read_parquet('{releases_glob}')
            ),
            lbl AS (
                SELECT
                    release_id,
                    i AS pos,
                    trim(tok) AS label_name
                FROM src
                CROSS JOIN UNNEST(regexp_split_to_array(labels_csv, ',')) WITH ORDINALITY AS t(tok, i)
                WHERE trim(tok) <> ''
            ),
            cat AS (
                SELECT
                    release_id,
                    i AS pos,
                    trim(tok) AS label_catno
                FROM src
                CROSS JOIN UNNEST(regexp_split_to_array(catnos_csv, ',')) WITH ORDINALITY AS t(tok, i)
                WHERE trim(tok) <> ''
            )
            SELECT DISTINCT
                l.release_id,
                l.label_name,
                lower(l.label_name) AS label_norm,
                c.label_catno
            FROM lbl l
            LEFT JOIN cat c
              ON l.release_id = c.release_id
             AND l.pos = c.pos
        )
        TO '{out_xref_file.as_posix()}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
    )

    xref_rows = con.execute(f"SELECT count(*) FROM read_parquet('{out_xref_file.as_posix()}')").fetchone()[0]
    distinct_pairs = con.execute(
        f"SELECT count(*) FROM (SELECT DISTINCT release_id, label_norm FROM read_parquet('{out_xref_file.as_posix()}'))"
    ).fetchone()[0]
    print(f"   ✅ xref rows: {xref_rows:,}")
    print(f"   ✅ distinct (release_id,label_norm): {distinct_pairs:,}")

    print()
    print("🧱 Building label_release_counts_v1 ...")

    con.execute(
        f"""
        COPY (
            SELECT
                label_norm,
                min(label_name) AS label_name_sample,
                count(DISTINCT release_id) AS n_total_releases
            FROM read_parquet('{out_xref_file.as_posix()}')
            GROUP BY 1
        )
        TO '{out_cnt_file.as_posix()}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
    )

    cnt_rows = con.execute(f"SELECT count(*) FROM read_parquet('{out_cnt_file.as_posix()}')").fetchone()[0]
    print(f"   ✅ label counts rows: {cnt_rows:,}")

    print()
    print("✅ Done.")
    print("Next: create Trino external tables pointing to:")
    print("  file:/data/hive-data/warehouse_discogs/release_label_xref_v1")
    print("  file:/data/hive-data/warehouse_discogs/label_release_counts_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
