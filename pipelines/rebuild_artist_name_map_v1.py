#!/usr/bin/env python3
"""
Build artist_name_map_v1

Derived dataset for fast artist lookup and name disambiguation.
Combines:
- artists_v1 (name, realname)
- artist_aliases_v1 (aliases)
- artist_memberships_v1 (group names)

Output table (Parquet):
hive.discogs.artist_name_map_v1
- norm_name  VARCHAR
- artist_id  VARCHAR

Portability:
- No hardcoded /tmp paths
- Uses DISCOGS_DATA_LAKE

ENV:
- DISCOGS_DATA_LAKE   default: /data/hive-data
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Optional, List, Dict

import duckdb
import pandas as pd


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

DATA_LAKE = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()

ARTISTS_PATH = DATA_LAKE / "artists_v1" / "*.parquet"
ALIASES_PATH = DATA_LAKE / "artist_aliases_v1" / "*.parquet"
MEMBERS_PATH = DATA_LAKE / "artist_memberships_v1" / "*.parquet"

DEST_DIR = DATA_LAKE / "warehouse_discogs" / "artist_name_map_v1"
DEST_FILE = DEST_DIR / "data.parquet"


# ------------------------------------------------------------
# Normalization
# ------------------------------------------------------------

_discogs_suffix_re = re.compile(r"\(\d+\)$")
_space_re = re.compile(r"\s+")


def normalize_name(name: Optional[str]) -> Optional[str]:
    if not isinstance(name, str):
        return None

    n = name.strip().lower()
    if not n:
        return None

    # remove Discogs numeric suffix: "Artist (2)" → "artist"
    n = _discogs_suffix_re.sub("", n).strip()

    # collapse whitespace
    n = _space_re.sub(" ", n)

    return n or None


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main() -> None:
    print("🔧 Building artist_name_map_v1")
    print(f"📂 Data lake: {DATA_LAKE}")

    con = duckdb.connect()

    print("📥 Loading artists_v1 …")
    artists = con.execute(
        f"""
        SELECT
            CAST(artist_id AS VARCHAR) AS artist_id,
            name,
            realname
        FROM '{ARTISTS_PATH}'
        """
    ).df()

    print("📥 Loading artist_aliases_v1 …")
    aliases = con.execute(
        f"""
        SELECT
            CAST(artist_id AS VARCHAR) AS artist_id,
            alias_name AS name
        FROM '{ALIASES_PATH}'
        """
    ).df()

    print("📥 Loading artist_memberships_v1 …")
    members = con.execute(
        f"""
        SELECT
            CAST(member_id AS VARCHAR) AS artist_id,
            group_name AS name
        FROM '{MEMBERS_PATH}'
        """
    ).df()

    print("🔨 Building mapping rows …")
    rows: List[Dict[str, str]] = []

    # canonical + realname
    for _, r in artists.iterrows():
        aid = r["artist_id"]
        for field in ("name", "realname"):
            norm = normalize_name(r[field])
            if norm:
                rows.append(
                    {
                        "norm_name": norm,
                        "artist_id": aid,
                    }
                )

    # aliases
    for _, r in aliases.iterrows():
        norm = normalize_name(r["name"])
        if norm:
            rows.append(
                {
                    "norm_name": norm,
                    "artist_id": r["artist_id"],
                }
            )

    # group names → member artist_id
    for _, r in members.iterrows():
        norm = normalize_name(r["name"])
        if norm:
            rows.append(
                {
                    "norm_name": norm,
                    "artist_id": r["artist_id"],
                }
            )

    print(f"📦 Total mapping rows: {len(rows):,}")

    df_map = pd.DataFrame(rows, columns=["norm_name", "artist_id"])

    # ------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------

    if DEST_DIR.exists():
        print("🧹 Clearing old output directory …")
        shutil.rmtree(DEST_DIR)

    DEST_DIR.mkdir(parents=True, exist_ok=True)

    con.register("tmp_map", df_map)

    print(f"💾 Writing Parquet → {DEST_FILE}")
    con.execute(
        f"""
        COPY tmp_map
        TO '{DEST_FILE}'
        (FORMAT PARQUET, COMPRESSION 'snappy');
        """
    )

    print("✅ Done. artist_name_map_v1 rebuilt successfully.")


if __name__ == "__main__":
    main()
