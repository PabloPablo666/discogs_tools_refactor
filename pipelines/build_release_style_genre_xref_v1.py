#!/usr/bin/env python3
"""
Build release_style_xref_v1 + release_genre_xref_v1 from releases_ref_v6 (Parquet).

Goal:
- Pre-explode styles and genres once (DuckDB), write Parquet datasets.
- Then Trino queries become cheap joins instead of huge UNNEST + regex on 18.6M rows.

Input (Parquet):
- releases_v6/*.parquet
  columns used: release_id, genres, styles

Output (Parquet dataset dirs):
- warehouse_discogs/release_style_xref_v1/data.parquet
  columns: release_id BIGINT, style VARCHAR, style_norm VARCHAR
- warehouse_discogs/release_genre_xref_v1/data.parquet
  columns: release_id BIGINT, genre VARCHAR, genre_norm VARCHAR

Normalization:
- token split by regex: [,;/]+
- trim
- drop empty
- *_norm = lower(trim(token))
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build release_style_xref_v1 and release_genre_xref_v1 (DuckDB → Parquet).")
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
    return p.parse_args()


def rm_dir(p: Path) -> None:
    if p.exists():
        shutil.rmtree(p)


def main() -> int:
    args = parse_args()

    data_lake = Path(args.data_lake).expanduser().resolve()
    releases_glob = args.releases_glob or str(data_lake / "releases_v6" / "*.parquet")
    warehouse = data_lake / args.warehouse_subdir

    out_styles_dir = warehouse / "release_style_xref_v1"
    out_genres_dir = warehouse / "release_genre_xref_v1"

    warehouse.mkdir(parents=True, exist_ok=True)

    if args.clean:
        rm_dir(out_styles_dir)
        rm_dir(out_genres_dir)

    out_styles_dir.mkdir(parents=True, exist_ok=True)
    out_genres_dir.mkdir(parents=True, exist_ok=True)

    out_styles_file = out_styles_dir / "data.parquet"
    out_genres_file = out_genres_dir / "data.parquet"

    con = duckdb.connect(database=":memory:")
    # DuckDB performance knobs (safe-ish defaults for laptop)
    con.execute("PRAGMA threads=4;")
    # If you want: con.execute("PRAGMA memory_limit='6GB';")  # optional, if you have RAM

    print("📥 Input releases glob:")
    print("   ", releases_glob)
    print("📦 Output:")
    print("   styles ->", out_styles_file)
    print("   genres ->", out_genres_file)
    print()

    # ---- Styles xref ----
    # Notes:
    # - regexp_split_to_array exists in DuckDB
    # - UNNEST explodes array to rows
    # - We keep both raw token (trimmed) and normalized token (lower)
    # - DISTINCT avoids duplicates like "House, House"
    print("🧱 Building release_style_xref_v1 ...")

    con.execute(
        f"""
        COPY (
            WITH src AS (
                SELECT
                    release_id::BIGINT AS release_id,
                    styles
                FROM read_parquet('{releases_glob}')
            ),
            exploded AS (
                SELECT
                    release_id,
                    trim(tok) AS style
                FROM src
                CROSS JOIN UNNEST(regexp_split_to_array(coalesce(styles, ''), '[,;/]+')) AS t(tok)
                WHERE trim(tok) <> ''
            )
            SELECT DISTINCT
                release_id,
                style,
                lower(style) AS style_norm
            FROM exploded
        )
        TO '{out_styles_file.as_posix()}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
    )

    styles_rows = con.execute(f"SELECT count(*) FROM read_parquet('{out_styles_file.as_posix()}')").fetchone()[0]
    print(f"   ✅ styles rows: {styles_rows:,}")

    # ---- Genres xref ----
    print("🧱 Building release_genre_xref_v1 ...")

    con.execute(
        f"""
        COPY (
            WITH src AS (
                SELECT
                    release_id::BIGINT AS release_id,
                    genres
                FROM read_parquet('{releases_glob}')
            ),
            exploded AS (
                SELECT
                    release_id,
                    trim(tok) AS genre
                FROM src
                CROSS JOIN UNNEST(regexp_split_to_array(coalesce(genres, ''), '[,;/]+')) AS t(tok)
                WHERE trim(tok) <> ''
            )
            SELECT DISTINCT
                release_id,
                genre,
                lower(genre) AS genre_norm
            FROM exploded
        )
        TO '{out_genres_file.as_posix()}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
    )

    genres_rows = con.execute(f"SELECT count(*) FROM read_parquet('{out_genres_file.as_posix()}')").fetchone()[0]
    print(f"   ✅ genres rows: {genres_rows:,}")

    print()
    print("✅ Done.")
    print("Next step: CREATE TABLE in Trino pointing to:")
    print("  file:/data/hive-data/warehouse_discogs/release_style_xref_v1")
    print("  file:/data/hive-data/warehouse_discogs/release_genre_xref_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
