#!/usr/bin/env python3
"""
Extract artist relations from Discogs artists.xml.gz

Outputs two Parquet datasets aligned with Trino tables:

1) artist_aliases_v1
   - artist_id   VARCHAR
   - alias_id    VARCHAR
   - alias_name  VARCHAR

2) artist_memberships_v1
   - group_id    VARCHAR
   - group_name  VARCHAR
   - member_id   VARCHAR
   - member_name VARCHAR

Portability:
- No hardcoded paths.
- Uses ENV vars with sane defaults for Docker.

ENV vars:
- DISCOGS_DATA_LAKE   default: /data/hive-data
- DISCOGS_RAW_ROOT    default: /data/raw
- DISCOGS_ARTISTS_XML default: <RAW_ROOT>/artists/discogs_artists.xml.gz

CLI overrides:
- --src, --out-aliases, --out-members, --batch, --engine
"""

from __future__ import annotations

import argparse
import gzip
import os
import sys
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd


def text_or_none(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def parquet_engine_preferred() -> str:
    # Prefer pyarrow if installed, fallback to fastparquet
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except Exception:
        return "fastparquet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract Discogs artist aliases and memberships to Parquet.")
    p.add_argument("--src", help="Path to discogs artists.xml.gz")
    p.add_argument("--out-aliases", help="Output dir for artist_aliases_v1")
    p.add_argument("--out-members", help="Output dir for artist_memberships_v1")
    p.add_argument("--batch", type=int, default=50_000, help="Rows per Parquet part (default: 50000)")
    p.add_argument("--engine", choices=["pyarrow", "fastparquet"], default=None, help="Parquet engine override")
    p.add_argument("--clean", action="store_true", help="Delete output dirs before writing")
    return p.parse_args()


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    data_lake = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()
    raw_root = Path(os.environ.get("DISCOGS_RAW_ROOT", "/data/raw")).expanduser()

    default_src = os.environ.get(
        "DISCOGS_ARTISTS_XML",
        str(raw_root / "artists" / "discogs_artists.xml.gz"),
    )

    src = Path(args.src).expanduser() if args.src else Path(default_src).expanduser()
    out_aliases = Path(args.out_aliases).expanduser() if args.out_aliases else (data_lake / "artist_aliases_v1")
    out_members = Path(args.out_members).expanduser() if args.out_members else (data_lake / "artist_memberships_v1")

    return src, out_aliases, out_members


def write_parquet_part(rows: List[Dict[str, Any]], out_dir: Path, part: int, engine: str, prefix: str) -> None:
    df = pd.DataFrame(rows)
    out_file = out_dir / f"part-{part:05d}.parquet"
    df.to_parquet(out_file, engine=engine, index=False)
    print(f"💾 Written {len(df):,} {prefix} rows → {out_file}")


def main() -> int:
    args = parse_args()
    src, out_aliases, out_members = resolve_paths(args)
    engine = args.engine or parquet_engine_preferred()
    batch = int(args.batch)

    if not src.exists():
        print(f"ERROR: source not found: {src}", file=sys.stderr)
        return 2

    if args.clean:
        if out_aliases.exists():
            shutil.rmtree(out_aliases)
        if out_members.exists():
            shutil.rmtree(out_members)

    out_aliases.mkdir(parents=True, exist_ok=True)
    out_members.mkdir(parents=True, exist_ok=True)

    print("🔧 Extracting artist relations")
    print(f"📥 SRC: {src}")
    print(f"📦 OUT aliases : {out_aliases}")
    print(f"📦 OUT members : {out_members}")
    print(f"⚙️  batch={batch} engine={engine}")

    rows_aliases: List[Dict[str, Any]] = []
    rows_members: List[Dict[str, Any]] = []
    part_aliases = 0
    part_members = 0

    def flush_aliases() -> None:
        nonlocal rows_aliases, part_aliases
        if not rows_aliases:
            return
        write_parquet_part(rows_aliases, out_aliases, part_aliases, engine, "alias")
        rows_aliases = []
        part_aliases += 1

    def flush_members() -> None:
        nonlocal rows_members, part_members
        if not rows_members:
            return
        write_parquet_part(rows_members, out_members, part_members, engine, "membership")
        rows_members = []
        part_members += 1

    # Stream parse: low memory
    with gzip.open(src, "rb") as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "artist":
                continue

            artist_id = text_or_none(elem.findtext("id"))
            artist_name = text_or_none(elem.findtext("name"))

            # --- ALIASES: artist_id -> alias_id / alias_name ---
            aliases_elem = elem.find("aliases")
            if aliases_elem is not None and artist_id is not None:
                for a in aliases_elem.findall("name"):
                    alias_id = text_or_none(a.get("id"))
                    alias_name = text_or_none(a.text)
                    if alias_name:
                        rows_aliases.append(
                            {
                                "artist_id": artist_id,
                                "alias_id": alias_id,
                                "alias_name": alias_name,
                            }
                        )

            # --- MEMBERSHIPS ---
            # Case 1: this artist is a GROUP with <members>
            members_elem = elem.find("members")
            if members_elem is not None and artist_id is not None and artist_name is not None:
                for m in members_elem.findall("name"):
                    member_id = text_or_none(m.get("id"))
                    member_name = text_or_none(m.text)
                    if member_name:
                        rows_members.append(
                            {
                                "group_id": artist_id,
                                "group_name": artist_name,
                                "member_id": member_id,
                                "member_name": member_name,
                            }
                        )

            # Case 2: this artist is a PERSON with <groups>
            groups_elem = elem.find("groups")
            if groups_elem is not None and artist_id is not None and artist_name is not None:
                for g in groups_elem.findall("name"):
                    group_id = text_or_none(g.get("id"))
                    group_name = text_or_none(g.text)
                    if group_name:
                        rows_members.append(
                            {
                                "group_id": group_id,
                                "group_name": group_name,
                                "member_id": artist_id,
                                "member_name": artist_name,
                            }
                        )

            if len(rows_aliases) >= batch:
                flush_aliases()
            if len(rows_members) >= batch:
                flush_members()

            elem.clear()

    flush_aliases()
    flush_members()

    print("✅ Done.")
    print(f"   aliases     → {out_aliases} (parts: {part_aliases})")
    print(f"   memberships → {out_members} (parts: {part_members})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
