#!/usr/bin/env python3
import duckdb
import os
import pandas as pd
import re

# ------------------------------------------------------------
# Input parquet tables (from hive-data)
# ------------------------------------------------------------

ARTISTS_PATH = "/tmp/hive-data/artists_v1/*.parquet"
ALIASES_PATH = "/tmp/hive-data/artist_aliases_v1/*.parquet"
MEMBERS_PATH = "/tmp/hive-data/artist_memberships_v1/*.parquet"

# ------------------------------------------------------------
# Output
# ------------------------------------------------------------

DEST_DIR = "/tmp/hive-data/warehouse_discogs/artist_name_map_v1"
DEST_FILE = os.path.join(DEST_DIR, "data.parquet")

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.strip().lower()
    # rimuovi "(123)" finale stile Discogs
    n = re.sub(r'\(\d+\)$', '', n).strip()
    # collassa spazi multipli
    n = re.sub(r'\s+', ' ', n)
    return n

def main():
    con = duckdb.connect()

    print("Loading artists_v1…")
    artists = con.execute(
        f"SELECT artist_id, name, realname FROM '{ARTISTS_PATH}'"
    ).df()

    print("Loading aliases (artist_aliases_v1)…")
    aliases = con.execute(
        f"SELECT artist_id AS target_id, alias_name AS name FROM '{ALIASES_PATH}'"
    ).df()

    print("Loading memberships (artist_memberships_v1)…")
    members = con.execute(
        f"SELECT member_id AS target_id, group_name AS name FROM '{MEMBERS_PATH}'"
    ).df()

    print("Building mapping rows…")
    rows = []

    # canonical + realname
    for _, row in artists.iterrows():
        aid = str(row.artist_id)

        for field in ("name", "realname"):
            raw = row[field]
            norm = normalize_name(raw)
            if norm:
                rows.append({
                    "norm_name": norm,
                    "artist_id": aid,
                    "name_type": field
                })

    # aliases
    for _, row in aliases.iterrows():
        norm = normalize_name(row.name)
        if norm:
            rows.append({
                "norm_name": norm,
                "artist_id": str(row.target_id),
                "name_type": "alias"
            })

    # group names attached to members
    for _, row in members.iterrows():
        norm = normalize_name(row.name)
        if norm:
            rows.append({
                "norm_name": norm,
                "artist_id": str(row.target_id),
                "name_type": "group"
            })

    print(f"Total name rows: {len(rows):,}")
    df_map = pd.DataFrame(rows)

    if os.path.exists(DEST_DIR):
        print("Clearing old directory…")
        os.system(f"rm -rf {DEST_DIR}")
    os.makedirs(DEST_DIR, exist_ok=True)

    con.register("tmp_map", df_map)

    print(f"Writing Parquet to {DEST_FILE} …")
    con.execute(f"""
        COPY tmp_map TO '{DEST_FILE}'
        (FORMAT PARQUET, COMPRESSION 'snappy');
    """)

    print("Done. artist_name_map_v1 rebuilt successfully.")

if __name__ == "__main__":
    main()
