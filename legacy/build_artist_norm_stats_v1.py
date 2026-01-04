#!/usr/bin/env python3
import duckdb
import os
import re

RELEASES_PATH = "/tmp/hive-data/releases_v6/*.parquet"
OUTPUT_DIR = "/tmp/hive-data/warehouse_discogs/artist_norm_stats_v1"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "data.parquet")

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.strip().lower()
    n = re.sub(r'\(\d+\)$', '', n).strip()   # rimuove (123) finale
    n = re.sub(r'\s+', ' ', n)               # collassa spazi
    return n

def main():
    con = duckdb.connect()

    if os.path.exists(OUTPUT_DIR):
        print("Clearing old artist_norm_stats_v1 dir…")
        os.system(f"rm -rf {OUTPUT_DIR}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    con.create_function("py_norm", normalize_name)

    print("Building artist_norm_stats_v1 from releases_v6…")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW releases AS
        SELECT
            release_id,
            title,
            released,
            artists
        FROM read_parquet('{RELEASES_PATH}');
    """)

    con.execute(f"""
        COPY (
            WITH exploded AS (
                SELECT
                    release_id,
                    lower(trim(title)) AS title_norm,
                    CASE
                        WHEN released IS NOT NULL
                             AND length(released) >= 4
                             AND substring(released,1,4) ~ '^[0-9]+$'
                        THEN CAST(substring(released,1,4) AS INTEGER)
                        ELSE NULL
                    END AS year_norm,
                    py_norm(unnested) AS artist_norm
                FROM releases r,
                UNNEST(string_split(COALESCE(r.artists, ''), ',')) AS t(unnested)
            ),
            valid AS (
                SELECT *
                FROM exploded
                WHERE artist_norm <> ''
            ),
            agg AS (
                SELECT
                    artist_norm,
                    COUNT(DISTINCT release_id) AS n_releases_total,
                    COUNT(DISTINCT title_norm) AS n_titles_total,
                    MIN(year_norm)            AS first_year,
                    MAX(year_norm)            AS last_year
                FROM valid
                GROUP BY artist_norm
            )
            SELECT * FROM agg
        ) TO '{OUTPUT_FILE}'
        (FORMAT PARQUET, COMPRESSION 'snappy');
    """)

    print(f"Saved artist_norm_stats_v1 → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
