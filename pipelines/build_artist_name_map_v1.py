#!/usr/bin/env python3
"""
Build artist_name_map_v1 (TYPED, FK-enforced)

Derived dataset for fast artist lookup and name disambiguation.

Inputs (typed):
- artists_v1_typed (artist_id BIGINT, name, realname)
- artist_aliases_v1_typed (artist_id BIGINT, alias_name)
- artist_memberships_v1_typed (member_id BIGINT, group_name)

Output (Parquet dataset directory):
- $DISCOGS_DATA_LAKE/warehouse_discogs/artist_name_map_v1/*.parquet

Schema:
- norm_name VARCHAR
- artist_id BIGINT

Guarantees:
- artist_id is FK-valid (exists in artists_v1_typed)
- norm_name is non-null / non-empty
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build artist_name_map_v1 (typed, FK-enforced).")
    p.add_argument("--clean", action="store_true", help="Delete existing output directory before writing.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    data_lake = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()

    artists_glob = str(data_lake / "artists_v1_typed" / "*.parquet")
    aliases_glob = str(data_lake / "artist_aliases_v1_typed" / "*.parquet")
    members_glob = str(data_lake / "artist_memberships_v1_typed" / "*.parquet")

    out_dir = data_lake / "warehouse_discogs" / "artist_name_map_v1"

    print("🔧 Building artist_name_map_v1 (typed, FK-enforced)")
    print(f"📂 Data lake: {data_lake}")
    print(f"📥 artists:   {artists_glob}")
    print(f"📥 aliases:   {aliases_glob}")
    print(f"📥 members:   {members_glob}")
    print(f"📦 output:    {out_dir}")

    if args.clean and out_dir.exists():
        print("🧹 Cleaning output directory …")
        shutil.rmtree(out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # Normalization:
    # - lower + trim
    # - remove Discogs numeric suffix at end: "Artist (2)" -> "artist"
    # - collapse whitespace
    norm_expr = r"""
    regexp_replace(
      regexp_replace(
        trim(lower(name)),
        '\(\d+\)$',
        ''
      ),
      '\s+',
      ' '
    )
    """

    # Build mapping rows (may include orphan artist_id at this stage), then enforce FK against artists_v1_typed.
    # Also track stats for drop reasons.
    sql = f"""
    WITH
    artists_src AS (
      SELECT artist_id, name FROM read_parquet('{artists_glob}')
      UNION ALL
      SELECT artist_id, realname AS name FROM read_parquet('{artists_glob}')
    ),
    aliases_src AS (
      SELECT artist_id, alias_name AS name FROM read_parquet('{aliases_glob}')
    ),
    members_src AS (
      SELECT member_id AS artist_id, group_name AS name FROM read_parquet('{members_glob}')
    ),
    all_names AS (
      SELECT artist_id, name FROM artists_src
      UNION ALL
      SELECT artist_id, name FROM aliases_src
      UNION ALL
      SELECT artist_id, name FROM members_src
    ),
    normalized AS (
      SELECT
        CAST(artist_id AS BIGINT) AS artist_id,
        NULLIF({norm_expr}, '') AS norm_name
      FROM all_names
      WHERE name IS NOT NULL
    ),
    fk_artists AS (
      SELECT DISTINCT artist_id
      FROM read_parquet('{artists_glob}')
      WHERE artist_id IS NOT NULL
    ),
    fk_filtered AS (
      SELECT n.norm_name, n.artist_id
      FROM normalized n
      JOIN fk_artists a
        ON n.artist_id = a.artist_id
      WHERE n.norm_name IS NOT NULL
    )
    SELECT DISTINCT
      norm_name,
      artist_id
    FROM fk_filtered
    """

    # Write as dataset (multiple parquet parts) inside out_dir
    # DuckDB will create files in the directory when using a path with a wildcard pattern.
    out_pattern = str(out_dir / "part-*.parquet")

    print("💾 Writing Parquet dataset …")
    con.execute(
        f"""
        COPY ({sql})
        TO '{out_pattern}'
        (FORMAT PARQUET, COMPRESSION 'snappy');
        """
    )

    # Stats: written
    written = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_dir}/*.parquet')"
    ).fetchone()[0]

    # Stats: dropped because norm_name empty/null (before FK)
    dropped_empty_norm = con.execute(
        f"""
        WITH all_names AS (
          SELECT artist_id, name FROM read_parquet('{artists_glob}')
          UNION ALL SELECT artist_id, realname AS name FROM read_parquet('{artists_glob}')
          UNION ALL SELECT artist_id, alias_name AS name FROM read_parquet('{aliases_glob}')
          UNION ALL SELECT member_id AS artist_id, group_name AS name FROM read_parquet('{members_glob}')
        ),
        normalized AS (
          SELECT
            CAST(artist_id AS BIGINT) AS artist_id,
            NULLIF({norm_expr}, '') AS norm_name
          FROM all_names
          WHERE name IS NOT NULL
        )
        SELECT COUNT(*)
        FROM normalized
        WHERE norm_name IS NULL
        """
    ).fetchone()[0]

    # Stats: dropped by FK (orphans)
    dropped_fk = con.execute(
        f"""
        WITH
        all_names AS (
          SELECT artist_id, name FROM read_parquet('{artists_glob}')
          UNION ALL SELECT artist_id, realname AS name FROM read_parquet('{artists_glob}')
          UNION ALL SELECT artist_id, alias_name AS name FROM read_parquet('{aliases_glob}')
          UNION ALL SELECT member_id AS artist_id, group_name AS name FROM read_parquet('{members_glob}')
        ),
        normalized AS (
          SELECT
            CAST(artist_id AS BIGINT) AS artist_id,
            NULLIF({norm_expr}, '') AS norm_name
          FROM all_names
          WHERE name IS NOT NULL
        ),
        fk_artists AS (
          SELECT DISTINCT artist_id
          FROM read_parquet('{artists_glob}')
          WHERE artist_id IS NOT NULL
        )
        SELECT COUNT(*)
        FROM normalized n
        LEFT JOIN fk_artists a
          ON n.artist_id = a.artist_id
        WHERE n.norm_name IS NOT NULL
          AND a.artist_id IS NULL
        """
    ).fetchone()[0]

    print(f"📊 artist_name_map_v1: written={written:,}")
    print(f"📉 dropped_empty_norm={dropped_empty_norm:,}")
    print(f"🧷 dropped_fk_orphans={dropped_fk:,}")
    print("✅ Done. artist_name_map_v1 rebuilt successfully.")


if __name__ == "__main__":
    main()
