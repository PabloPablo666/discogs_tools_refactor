#!/usr/bin/env python3
import duckdb
import os

RELEASES_PATH = "/tmp/hive-data/releases_v6/*.parquet"
ARTIST_MAP_FILE = "/tmp/hive-data/warehouse_discogs/artist_name_map_v1/data.parquet"

OUTPUT_DIR = "/tmp/hive-data/warehouse_discogs/artist_release_stats_v1"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "data.parquet")

def main():
    con = duckdb.connect()

    # mappa canonica (norm_name già normalizzato in Python)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW artist_map AS
        SELECT
            norm_name,
            CAST(artist_id AS VARCHAR) AS artist_id
        FROM read_parquet('{ARTIST_MAP_FILE}');
    """)

    # releases base
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW releases AS
        SELECT
            release_id,
            title,
            released,
            artists
        FROM read_parquet('{RELEASES_PATH}');
    """)

    if os.path.exists(OUTPUT_DIR):
        print("Clearing old artist_release_stats_v1 directory…")
        os.system(f"rm -rf {OUTPUT_DIR}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Computing artist_release_stats_v1…")

    con.execute(f"""
        COPY (
            WITH exploded AS (
                SELECT
                    r.release_id,
                    lower(trim(r.title)) AS title_norm,
                    r.title,
                    r.released,
                    -- normalizzazione ARTIST identica alla Python:
                    lower(
                      regexp_replace(
                        regexp_replace(trim(unnested), '\\\\(\\\\d+\\\\)$', ''),
                        '\\\\s+',
                        ' '
                      )
                    ) AS norm_name
                FROM releases r,
                UNNEST(string_split(COALESCE(r.artists, ''), ',')) AS t(unnested)
            ),
            joined AS (
                SELECT
                    am.artist_id,
                    e.release_id,
                    e.title_norm,
                    e.released
                FROM exploded e
                JOIN artist_map am
                  ON e.norm_name = am.norm_name
            ),
            normed AS (
                SELECT
                    artist_id,
                    release_id,
                    title_norm,
                    CASE
                        WHEN released IS NOT NULL
                             AND length(released) >= 4
                             AND substring(released, 1, 4) ~ '^[0-9]+$'
                        THEN CAST(substring(released, 1, 4) AS INTEGER)
                        ELSE NULL
                    END AS year_norm
                FROM joined
            ),
            agg AS (
                SELECT
                    artist_id,
                    COUNT(DISTINCT release_id)    AS n_releases_total,
                    COUNT(DISTINCT title_norm)    AS n_titles_total,
                    MIN(year_norm)                AS first_year,
                    MAX(year_norm)                AS last_year
                FROM normed
                GROUP BY artist_id
            )
            SELECT * FROM agg
        ) TO '{OUTPUT_FILE}'
        (FORMAT PARQUET, COMPRESSION 'snappy');
    """)

    print(f"Saved artist_release_stats_v1 → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
