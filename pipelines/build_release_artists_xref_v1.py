#!/usr/bin/env python3
"""
Build release_artists_v1 from releases_ref_v6 (Parquet) using DuckDB.

Goal:
- Pre-explode releases_ref_v6.artists once into a compact xref.
- Avoid expensive Trino regex_split + UNNEST on 18.6M rows.

Input (Parquet dataset):
- <data-lake>/releases_v6/*.parquet
  columns used: release_id, artists

Output (Parquet dataset dir):
- <data-lake>/<warehouse-subdir>/release_artists_v1/data.parquet
  columns:
    - release_id   BIGINT
    - artist_norm  VARCHAR   (lower(trim(token)))

Rules:
- Split by comma (same as current Trino logic)
- trim tokens
- drop empty
- normalize: lower(token)
- DISTINCT to deduplicate duplicates within the same release
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build release_artists_v1 (DuckDB → Parquet).")
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
        "--out-subdir",
        default="release_artists_v1",
        help="Output dataset subdir under warehouse. Default: release_artists_v1",
    )
    p.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing output dir before writing.",
    )
    p.add_argument(
        "--threads",
        type=int,
        default=4,
        help="DuckDB threads. Default: 4",
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

    out_dir = warehouse / args.out_subdir
    out_file = out_dir / "data.parquet"

    warehouse.mkdir(parents=True, exist_ok=True)

    if args.clean:
        rm_dir(out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)};")

    print("📥 Input releases glob:")
    print("   ", releases_glob)
    print("📦 Output:")
    print("   ", out_file)
    print()

    # Build xref
    con.execute(
        f"""
        COPY (
            WITH src AS (
                SELECT
                    release_id::BIGINT AS release_id,
                    artists
                FROM read_parquet('{releases_glob}')
            ),
            exploded AS (
                SELECT
                    release_id,
                    trim(tok) AS artist_raw
                FROM src
                CROSS JOIN UNNEST(string_split(coalesce(artists, ''), ',')) AS t(tok)
                WHERE trim(tok) <> ''
            )
            SELECT DISTINCT
                release_id,
                lower(artist_raw) AS artist_norm
            FROM exploded
        )
        TO '{out_file.as_posix()}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
    )

    rows = con.execute(f"SELECT count(*) FROM read_parquet('{out_file.as_posix()}')").fetchone()[0]
    distinct_releases = con.execute(
        f"SELECT count(DISTINCT release_id) FROM read_parquet('{out_file.as_posix()}')"
    ).fetchone()[0]

    print(f"✅ release_artists_v1 rows: {rows:,}")
    print(f"✅ distinct release_id:     {distinct_releases:,}")
    print()
    print("Next step (if not already in Trino):")
    print(f"  CREATE TABLE hive.discogs.release_artists_v1 (...)")
    print(f"  WITH (external_location='file:/data/hive-data/{args.warehouse_subdir}/{args.out_subdir}', format='PARQUET');")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
