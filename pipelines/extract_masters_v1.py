#!/usr/bin/env python3
"""
Discogs → masters_v1 (Parquet)

Streaming parse of the Discogs masters XML dump (gzipped) to Parquet.

Modes:
- legacy (default): writes masters_v1 with master_id/main_release_id as VARCHAR (as before)
- typed (--typed):  writes masters_v1_typed with master_id/main_release_id as BIGINT,
                    drops rows with non-numeric master_id and reports drops.

ENV vars:
- DISCOGS_DATA_LAKE      default: /data/hive-data
- DISCOGS_RAW            default: /data/raw
- DISCOGS_MASTERS_DUMP   optional explicit dump path
"""

from __future__ import annotations

import argparse
import gzip
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd


def text_or_none(x) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def parquet_engine_preferred() -> str:
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except Exception:
        return "fastparquet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract Discogs masters dump to Parquet.")
    p.add_argument("--src", help="Path to masters XML dump (*.xml.gz). Overrides DISCOGS_MASTERS_DUMP.")
    p.add_argument("--out", help="Output dir override.")
    p.add_argument("--batch", type=int, default=50_000, help="Rows per Parquet part (default: 50000).")
    p.add_argument("--engine", choices=["pyarrow", "fastparquet"], default=None, help="Parquet engine override.")
    p.add_argument("--max-parts", type=int, default=None, help="Stop after writing N parts (debug).")
    p.add_argument("--clean", action="store_true", help="Delete existing *.parquet in output dir before writing.")
    p.add_argument("--typed", action="store_true", help="Write typed IDs (master_id/main_release_id BIGINT) to masters_v1_typed.")
    return p.parse_args()


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    data_lake_root = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()
    raw_root = Path(os.environ.get("DISCOGS_RAW", "/data/raw")).expanduser()

    src_env = os.environ.get("DISCOGS_MASTERS_DUMP")
    if args.src:
        src = Path(args.src).expanduser()
    elif src_env:
        src = Path(src_env).expanduser()
    else:
        src = raw_root / "masters" / "discogs_masters.xml.gz"

    if args.out:
        out = Path(args.out).expanduser()
    else:
        out = data_lake_root / ("masters_v1_typed" if args.typed else "masters_v1")

    return src, out


def join_csv(values: List[str], sep: str = ", ") -> Optional[str]:
    return sep.join(values) if values else None


def safe_int(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except ValueError:
        return None


def main() -> int:
    args = parse_args()
    src, out_dir = resolve_paths(args)

    if not src.exists():
        print(f"❌ ERROR: source file not found: {src}", file=sys.stderr)
        print("   Tip: pass --src /path/to/discogs_YYYYMMDD_masters.xml.gz", file=sys.stderr)
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        old = list(out_dir.glob("*.parquet"))
        for fp in old:
            fp.unlink()
        if old:
            print(f"🧹 Cleaned {len(old)} parquet files in {out_dir}")

    engine = args.engine or parquet_engine_preferred()
    batch = int(args.batch)

    print(f"📥 Source:  {src}")
    print(f"📦 Output:  {out_dir}")
    print(f"⚙️  batch={batch} engine={engine} clean={bool(args.clean)} typed={bool(args.typed)}")

    rows: List[Dict[str, Any]] = []
    part = 0
    dropped_total = 0
    written_total = 0

    def flush() -> None:
        nonlocal rows, part, dropped_total, written_total
        if not rows:
            return

        df = pd.DataFrame(
            rows,
            columns=[
                "master_id",
                "main_release_id",
                "title",
                "year",
                "master_artists",
                "master_artist_ids",
                "genres",
                "styles",
                "data_quality",
            ],
        )

        # year stays numeric
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

        dropped_batch = 0
        if args.typed:
            df["master_id"] = pd.to_numeric(df["master_id"], errors="coerce").astype("Int64")
            df["main_release_id"] = pd.to_numeric(df["main_release_id"], errors="coerce").astype("Int64")

            dropped_batch = int(df["master_id"].isna().sum())
            if dropped_batch:
                print(f"⚠️  masters_v1_typed: dropping {dropped_batch:,} rows with non-numeric master_id (batch)")

            # master_id is mandatory for typed dataset
            df = df[df["master_id"].notna()]

        out_file = out_dir / f"part-{part:05d}.parquet"
        df.to_parquet(out_file, engine=engine, index=False)

        written = len(df)
        written_total += written
        dropped_total += dropped_batch

        print(f"💾 Written {written:,} rows → {out_file.name}")

        rows = []
        part += 1

    with gzip.open(src, "rb") as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "master":
                continue

            master_id = text_or_none(elem.get("id"))
            main_release_id = text_or_none(elem.findtext("main_release"))
            title = text_or_none(elem.findtext("title"))
            year = safe_int(text_or_none(elem.findtext("year")))
            data_quality = text_or_none(elem.findtext("data_quality"))

            master_artists_list: List[str] = []
            master_artist_ids_list: List[str] = []
            artists_elem = elem.find("artists")
            if artists_elem is not None:
                for a in artists_elem.findall("artist"):
                    name = text_or_none(a.findtext("name"))
                    aid = text_or_none(a.findtext("id"))
                    if name:
                        master_artists_list.append(name)
                    if aid:
                        master_artist_ids_list.append(aid)

            master_artists = join_csv(master_artists_list, sep=", ")
            master_artist_ids = join_csv(master_artist_ids_list, sep=",")

            genres_list: List[str] = []
            genres_elem = elem.find("genres")
            if genres_elem is not None:
                for g in genres_elem.findall("genre"):
                    gtxt = text_or_none(g.text)
                    if gtxt:
                        genres_list.append(gtxt)
            genres = join_csv(genres_list, sep=", ")

            styles_list: List[str] = []
            styles_elem = elem.find("styles")
            if styles_elem is not None:
                for s in styles_elem.findall("style"):
                    stxt = text_or_none(s.text)
                    if stxt:
                        styles_list.append(stxt)
            styles = join_csv(styles_list, sep=", ")

            rows.append(
                {
                    "master_id": master_id,
                    "main_release_id": main_release_id,
                    "title": title,
                    "year": year,
                    "master_artists": master_artists,
                    "master_artist_ids": master_artist_ids,
                    "genres": genres,
                    "styles": styles,
                    "data_quality": data_quality,
                }
            )

            if len(rows) >= batch:
                flush()
                if args.max_parts is not None and part >= args.max_parts:
                    print(f"🧪 max-parts reached ({args.max_parts}), stopping early.")
                    elem.clear()
                    break

            elem.clear()

    flush()

    if args.typed:
        print(f"✅ Done. Parts written: {part}  Output: {out_dir}")
        print(f"📊 masters_v1_typed: written={written_total:,} dropped_non_numeric_master_id={dropped_total:,}")
    else:
        print(f"✅ Done. Parts written: {part}  Output: {out_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
