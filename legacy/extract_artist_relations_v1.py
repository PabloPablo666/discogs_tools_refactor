#!/usr/bin/env python3
import gzip
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd


def text_or_none(x):
    if x is None:
        return None
    x = str(x).strip()
    return x or None


SRC = Path("/Users/paoloolivieri/discogs_store/raw/artists/discogs_20251101_artists.xml.gz")

OUT_ALIASES = Path("/Users/paoloolivieri/discogs_store/hive-data/artist_aliases_v1")
OUT_MEMBERS = Path("/Users/paoloolivieri/discogs_store/hive-data/artist_memberships_v1")

OUT_ALIASES.mkdir(parents=True, exist_ok=True)
OUT_MEMBERS.mkdir(parents=True, exist_ok=True)

BATCH = 50000

rows_aliases = []
rows_members = []

part_aliases = 0
part_members = 0


def flush_aliases():
    global rows_aliases, part_aliases
    if not rows_aliases:
        return
    df = pd.DataFrame(rows_aliases)
    out_file = OUT_ALIASES / f"part-{part_aliases:05d}.parquet"
    df.to_parquet(out_file, index=False)
    print(f"💾 Written {len(df)} alias rows to {out_file}")
    rows_aliases = []
    part_aliases += 1


def flush_members():
    global rows_members, part_members
    if not rows_members:
        return
    df = pd.DataFrame(rows_members)
    out_file = OUT_MEMBERS / f"part-{part_members:05d}.parquet"
    df.to_parquet(out_file, index=False)
    print(f"💾 Written {len(df)} membership rows to {out_file}")
    rows_members = []
    part_members += 1


with gzip.open(SRC, "rb") as f:
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

        # --- MEMBERSHIPS: gruppi e persone ---
        # Caso 1: questo artist è un GRUPPO con <members>
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

        # Caso 2: questo artist è una PERSONA con <groups>
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

        if len(rows_aliases) >= BATCH:
            flush_aliases()
        if len(rows_members) >= BATCH:
            flush_members()

        elem.clear()

flush_aliases()
flush_members()
print("✅ Done. Outputs:")
print("   aliases    ->", OUT_ALIASES)
print("   memberships->", OUT_MEMBERS)
