#!/usr/bin/env python3
"""
Discogs → artists_v1 (Parquet)

Streaming parse of the Discogs artists XML dump (gzipped) to Parquet, aligned with:
hive.discogs.artists_v1

Output columns (strings):
- artist_id
- name
- realname
- profile
- data_quality
- urls            (comma+space separated)
- namevariations  (comma+space separated)
- aliases         (comma+space separated)

No hardcoded paths:
- Default input/output can be driven by ENV vars (Docker-friendly),
  but you can always pass --src / --out.

ENV vars:
- DISCOGS_DATA_LAKE      default: /data/hive-data
- DISCOGS_RAW            default: /data/raw
- DISCOGS_ARTISTS_DUMP   optional explicit dump path
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


# ----------------------------
# helpers
# ----------------------------

def text_or_none(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def join_csv(values: List[str]) -> Optional[str]:
    return ", ".join(values) if values else None


def preferred_parquet_engine() -> str:
    # Prefer pyarrow if present, fallback to fastparquet
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except Exception:
        return "fastparquet"


def resolve_paths(src_arg: Optional[str], out_arg: Optional[str]) -> tuple[Path, Path]:
    data_lake_root = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()
    raw_root = Path(os.environ.get("DISCOGS_RAW", "/data/raw")).expanduser()

    src_env = os.environ.get("DISCOGS_ARTISTS_DUMP")

    if src_arg:
        src = Path(src_arg).expanduser()
    elif src_env:
        src = Path(src_env).expanduser()
    else:
        # default guess (prefer passing --src because Discogs filenames are timestamped)
        src = raw_root / "artists" / "discogs_artists.xml.gz"

    out = Path(out_arg).expanduser() if out_arg else (data_lake_root / "artists_v1")
    return src, out


# ----------------------------
# args
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract Discogs artists dump to Parquet (artists_v1).")
    p.add_argument("--src", help="Path to artists XML dump (*.xml.gz). Overrides DISCOGS_ARTISTS_DUMP.")
    p.add_argument("--out", help="Output directory. Default: $DISCOGS_DATA_LAKE/artists_v1")
    p.add_argument("--batch", type=int, default=50_000, help="Rows per Parquet part (default: 50000).")
    p.add_argument("--engine", choices=["pyarrow", "fastparquet"], default=None, help="Parquet engine override.")
    p.add_argument("--clean", action="store_true", help="Delete existing *.parquet in output dir before writing.")
    p.add_argument("--max-parts", type=int, default=None, help="Stop after writing N parts (debug).")
    return p.parse_args()


# ----------------------------
# main
# ----------------------------

def main() -> int:
    args = parse_args()
    src, out_dir = resolve_paths(args.src, args.out)

    if not src.exists():
        print(f"❌ ERROR: source file not found: {src}", file=sys.stderr)
        print("   Tip: pass --src /path/to/discogs_YYYYMMDD_artists.xml.gz", file=sys.stderr)
        return 2

    if args.batch <= 0:
        print("❌ ERROR: --batch must be > 0", file=sys.stderr)
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)

    # clean only parquet files (safe)
    if args.clean:
        old = list(out_dir.glob("*.parquet"))
        for fp in old:
            fp.unlink()
        if old:
            print(f"🧹 Cleaned {len(old)} parquet files in {out_dir}")

    engine = args.engine or preferred_parquet_engine()

    print(f"📥 Source: {src}")
    print(f"📦 Output: {out_dir}")
    print(f"⚙️  batch={args.batch} engine={engine} clean={bool(args.clean)} max_parts={args.max_parts}")

    rows: List[Dict[str, Any]] = []
    part = 0

    def flush() -> None:
        nonlocal rows, part
        if not rows:
            return

        df = pd.DataFrame(
            rows,
            columns=[
                "artist_id",
                "name",
                "realname",
                "profile",
                "data_quality",
                "urls",
                "namevariations",
                "aliases",
            ],
        )

        out_file = out_dir / f"part-{part:05d}.parquet"
        df.to_parquet(out_file, engine=engine, index=False)
        print(f"💾 Written {len(df):,} rows → {out_file.name}")

        rows = []
        part += 1

    with gzip.open(src, "rb") as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "artist":
                continue

            # Discogs artists dump usually has <id> inside <artist>; some variants have id attribute
            artist_id = text_or_none(elem.findtext("id")) or text_or_none(elem.get("id"))
            name = text_or_none(elem.findtext("name"))
            realname = text_or_none(elem.findtext("realname"))
            profile = text_or_none(elem.findtext("profile"))
            data_quality = text_or_none(elem.findtext("data_quality"))

            urls_list: List[str] = []
            urls_elem = elem.find("urls")
            if urls_elem is not None:
                for u in urls_elem.findall("url"):
                    utxt = text_or_none(u.text)
                    if utxt:
                        urls_list.append(utxt)
            urls = join_csv(urls_list)

            nv_list: List[str] = []
            nv_elem = elem.find("namevariations")
            if nv_elem is not None:
                for n in nv_elem.findall("name"):
                    ntxt = text_or_none(n.text)
                    if ntxt:
                        nv_list.append(ntxt)
            namevariations = join_csv(nv_list)

            aliases_list: List[str] = []
            aliases_elem = elem.find("aliases")
            if aliases_elem is not None:
                for a in aliases_elem.findall("name"):
                    atxt = text_or_none(a.text)
                    if atxt:
                        aliases_list.append(atxt)
            aliases = join_csv(aliases_list)

            rows.append(
                {
                    "artist_id": artist_id,
                    "name": name,
                    "realname": realname,
                    "profile": profile,
                    "data_quality": data_quality,
                    "urls": urls,
                    "namevariations": namevariations,
                    "aliases": aliases,
                }
            )

            if len(rows) >= args.batch:
                flush()
                if args.max_parts is not None and part >= args.max_parts:
                    print(f"🧪 max-parts reached ({args.max_parts}), stopping early.")
                    elem.clear()
                    break

            elem.clear()

    flush()
    print(f"✅ Done. Parts written: {part}  Output: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
