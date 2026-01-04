#!/usr/bin/env python3
"""
Discogs Collection → collection (Parquet)

Reads paginated Discogs collection exports in JSON form (discogs_page_*.json)
and writes a query-ready Parquet dataset aligned with:

hive.discogs.collection

Output columns:
- instance_id (BIGINT)
- release_id  (BIGINT)
- title       (VARCHAR)
- artists     (VARCHAR)   comma-separated
- labels      (VARCHAR)   comma-separated
- year        (BIGINT)
- formats     (VARCHAR)   comma-separated
- genres      (VARCHAR)   comma-separated
- styles      (VARCHAR)   comma-separated
- date_added  (VARCHAR)
- rating      (BIGINT)

Portability:
- No hardcoded paths.
- Uses ENV vars with Docker-friendly defaults.

ENV vars:
- DISCOGS_DATA_LAKE          default: /data/hive-data
- DISCOGS_COLLECTION_JSON    default: /data/raw/collection_json
- DISCOGS_COLLECTION_PATTERN default: discogs_page_*.json
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def text_or_none(x: Any) -> Optional[str]:
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
    p = argparse.ArgumentParser(description="Parse Discogs collection JSON pages to Parquet dataset (collection).")
    p.add_argument("--src-dir", help="Directory containing JSON pages. Overrides DISCOGS_COLLECTION_JSON.")
    p.add_argument("--pattern", default=None, help="Glob pattern for JSON files (default from env or discogs_page_*.json).")
    p.add_argument("--out", help="Output directory (default: $DISCOGS_DATA_LAKE/collection).")
    p.add_argument("--chunk", type=int, default=20_000, help="Rows per Parquet part (default: 20000).")
    p.add_argument("--engine", choices=["pyarrow", "fastparquet"], default=None, help="Parquet engine override.")
    p.add_argument("--max-parts", type=int, default=None, help="Stop after writing N parts (debug).")
    return p.parse_args()


def resolve_paths(src_dir_arg: Optional[str], pattern_arg: Optional[str], out_arg: Optional[str]) -> tuple[Path, str, Path]:
    data_lake_root = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()

    src_dir_env = os.environ.get("DISCOGS_COLLECTION_JSON", "/data/raw/collection_json")
    pattern_env = os.environ.get("DISCOGS_COLLECTION_PATTERN", "discogs_page_*.json")

    src_dir = Path(src_dir_arg).expanduser() if src_dir_arg else Path(src_dir_env).expanduser()
    pattern = pattern_arg or pattern_env

    out_dir = Path(out_arg).expanduser() if out_arg else (data_lake_root / "collection")
    return src_dir, pattern, out_dir


def load_items(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    if isinstance(data, list):
        return data
    return []


def join_names(arr: Any, key: str = "name") -> Optional[str]:
    if not isinstance(arr, list):
        return None
    out: List[str] = []
    for x in arr:
        if isinstance(x, dict):
            v = text_or_none(x.get(key))
            if v:
                out.append(v)
    return ", ".join(out) if out else None


def join_values(arr: Any) -> Optional[str]:
    if not isinstance(arr, list):
        return None
    out: List[str] = []
    for x in arr:
        v = text_or_none(x)
        if v:
            out.append(v)
    return ", ".join(out) if out else None


def to_int_or_none(x: Any) -> Optional[int]:
    if x is None:
        return None
    # Allow ints, numeric strings, etc.
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if not s:
        return None
    if s.isdigit():
        try:
            return int(s)
        except ValueError:
            return None
    return None


def main() -> int:
    args = parse_args()
    src_dir, pattern, out_dir = resolve_paths(args.src_dir, args.pattern, args.out)

    if not src_dir.exists() or not src_dir.is_dir():
        print(f"ERROR: src-dir not found or not a directory: {src_dir}", file=sys.stderr)
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    engine = args.engine or parquet_engine_preferred()

    files = sorted(glob.glob(str(src_dir / pattern)))
    print(f"📥 Found {len(files)} JSON files in {src_dir} (pattern: {pattern})")
    print(f"📦 Output: {out_dir}")
    print(f"⚙️  chunk={args.chunk} engine={engine}")

    rows: List[Dict[str, Any]] = []
    part = 0

    def flush() -> None:
        nonlocal rows, part
        if not rows:
            return

        df = pd.DataFrame(
            rows,
            columns=[
                "instance_id",
                "release_id",
                "title",
                "artists",
                "labels",
                "year",
                "formats",
                "genres",
                "styles",
                "date_added",
                "rating",
            ],
        )

        # Enforce numeric where expected by Trino
        df["instance_id"] = pd.to_numeric(df["instance_id"], errors="coerce").astype("Int64")
        df["release_id"] = pd.to_numeric(df["release_id"], errors="coerce").astype("Int64")
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce").astype("Int64")

        out_file = out_dir / f"collection_part{part:03d}.parquet"
        df.to_parquet(out_file, engine=engine, index=False)
        print(f"🧱 written {out_file.name}  ({len(df):,} rows)")

        rows.clear()
        part += 1

    for fp in files:
        p = Path(fp)
        items = load_items(p)

        for it in items:
            if not isinstance(it, dict):
                continue

            d = it.get("basic_information", {})
            if not isinstance(d, dict):
                d = {}

            instance_id = it.get("id") or it.get("instance_id") or d.get("instance_id")
            release_id = d.get("id") or it.get("release_id")

            title = d.get("title")
            year = d.get("year")
            genres = d.get("genres")
            styles = d.get("styles")
            labels = d.get("labels")
            artists = d.get("artists")
            formats = d.get("formats")
            date_added = it.get("date_added") or d.get("date_added")

            rating = (
                it.get("rating")
                or it.get("rating_value")
                or d.get("rating")
                or (it.get("notes", {}).get("rating") if isinstance(it.get("notes"), dict) else None)
            )

            rows.append(
                {
                    "instance_id": to_int_or_none(instance_id),
                    "release_id": to_int_or_none(release_id),
                    "title": text_or_none(title),
                    "artists": join_names(artists, "name"),
                    "labels": join_names(labels, "name"),
                    "year": to_int_or_none(year),
                    "formats": join_names(formats, "name"),
                    "genres": join_values(genres),
                    "styles": join_values(styles),
                    "date_added": text_or_none(date_added),
                    "rating": to_int_or_none(rating),
                }
            )

            if len(rows) >= int(args.chunk):
                flush()
                if args.max_parts is not None and part >= args.max_parts:
                    print(f"🧪 max-parts reached ({args.max_parts}), stopping early.")
                    break

        if args.max_parts is not None and part >= args.max_parts:
            break

    flush()
    print(f"✅ Done. Parts written: {part}  Output: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
