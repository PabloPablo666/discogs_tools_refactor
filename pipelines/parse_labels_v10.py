#!/usr/bin/env python3
"""
Discogs → labels_v10 (Parquet)

Streaming parse del dump Discogs labels (xml.gz) verso Parquet, compatibile con:
hive.discogs.labels_ref_v10

Colonne output:
- label_id (BIGINT)
- name (VARCHAR)
- profile (VARCHAR)
- contact_info (VARCHAR)
- data_quality (VARCHAR)
- parent_label_id (BIGINT)
- parent_label_name (VARCHAR)
- urls_csv (VARCHAR)
- sublabel_ids_csv (VARCHAR)        comma+space separated
- sublabel_names_csv (VARCHAR)      comma+space separated

Obiettivi:
- Stessa logica del tuo vecchio script (che funzionava)
- Portabile (no path hardcoded)
- Zero righe con label_id NULL
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


def join_csv(values: List[str]) -> Optional[str]:
    return ", ".join(values) if values else None


def parquet_engine_preferred() -> str:
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except Exception:
        return "fastparquet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parse Discogs labels dump to Parquet (labels_v10).")
    p.add_argument("--src", required=False, help="Path to labels XML dump (*.xml.gz).")
    p.add_argument("--out", required=False, help="Output dir. Default: $DISCOGS_DATA_LAKE/labels_v10")
    p.add_argument("--batch", type=int, default=50_000, help="Rows per part (default: 50000).")
    p.add_argument("--engine", choices=["pyarrow", "fastparquet"], default=None, help="Parquet engine override.")
    p.add_argument("--clean", action="store_true", help="Delete existing *.parquet in output dir before writing.")
    p.add_argument("--max-parts", type=int, default=None, help="Stop after writing N parts (debug).")
    return p.parse_args()


def resolve_paths(src_arg: Optional[str], out_arg: Optional[str]) -> tuple[Path, Path]:
    data_lake_root = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()
    raw_root = Path(os.environ.get("DISCOGS_RAW", "/data/raw")).expanduser()

    src_env = os.environ.get("DISCOGS_LABELS_DUMP")
    if src_arg:
        src = Path(src_arg).expanduser()
    elif src_env:
        src = Path(src_env).expanduser()
    else:
        # default “docker-friendly guess”
        src = raw_root / "labels" / "discogs_labels.xml.gz"

    out = Path(out_arg).expanduser() if out_arg else (data_lake_root / "labels_v10")
    return src, out


def main() -> int:
    args = parse_args()
    src, out_dir = resolve_paths(args.src, args.out)

    if not src.exists():
        print(f"❌ ERROR: source file not found: {src}", file=sys.stderr)
        print("   Tip: pass --src /path/to/discogs_YYYYMMDD_labels.xml.gz", file=sys.stderr)
        return 2

    if args.batch <= 0:
        print("❌ ERROR: --batch must be > 0", file=sys.stderr)
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

    print(f"📥 Source: {src}")
    print(f"📦 Output: {out_dir}")
    print(f"⚙️  batch={batch} engine={engine} clean={bool(args.clean)} max_parts={args.max_parts}")

    rows: List[Dict[str, Any]] = []
    part = 0
    skipped_no_id = 0

    def flush() -> None:
        nonlocal rows, part
        if not rows:
            return

        df = pd.DataFrame(
            rows,
            columns=[
                "label_id",
                "name",
                "profile",
                "contact_info",
                "data_quality",
                "parent_label_id",
                "parent_label_name",
                "urls_csv",
                "sublabel_ids_csv",
                "sublabel_names_csv",
            ],
        )

        # Force numeric IDs (like your old script) and guarantee no NULL label_id
        df["label_id"] = pd.to_numeric(df["label_id"], errors="coerce").astype("Int64")
        df["parent_label_id"] = pd.to_numeric(df["parent_label_id"], errors="coerce").astype("Int64")

        # Drop any row that somehow lost label_id (paranoia mode ON)
        before = len(df)
        df = df[df["label_id"].notna()]
        dropped = before - len(df)
        if dropped:
            print(f"⚠️ Dropped {dropped} rows with NULL label_id during flush()")

        # Force text cols to string (prevents BLOB surprises)
        text_cols = [
            "name",
            "profile",
            "contact_info",
            "data_quality",
            "parent_label_name",
            "urls_csv",
            "sublabel_ids_csv",
            "sublabel_names_csv",
        ]
        for c in text_cols:
            df[c] = df[c].astype("string")

        out_file = out_dir / f"labels_part{part:04d}.parquet"
        df.to_parquet(out_file, engine=engine, index=False)
        print(f"🧱 written {out_file.name} ({len(df):,} rows)")

        rows.clear()
        part += 1

    with gzip.open(src, "rb") as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "label":
                continue

            # ID: prefer attribute, fallback to <id> if present
            lid_raw = text_or_none(elem.get("id")) or text_or_none(elem.findtext("id"))

            # If ID missing, skip. No NULL rows. Ever.
            if not lid_raw or not lid_raw.isdigit():
                skipped_no_id += 1
                elem.clear()
                continue

            name = text_or_none(elem.findtext("name"))
            profile = text_or_none(elem.findtext("profile"))
            contact = text_or_none(elem.findtext("contactinfo"))
            dq = text_or_none(elem.findtext("data_quality"))

            parent = elem.find("parent_label")
            parent_id = text_or_none(parent.get("id")) if parent is not None else None
            parent_name = None
            if parent is not None and parent.text:
                parent_name = text_or_none(parent.text)

            urls_list: List[str] = []
            for u in elem.findall("urls/url"):
                utxt = text_or_none(u.text)
                if utxt:
                    urls_list.append(utxt)
            urls_csv = join_csv(urls_list)

            s_ids: List[str] = []
            s_names: List[str] = []
            for s in elem.findall("sublabels/label"):
                sid = text_or_none(s.get("id"))
                sname = text_or_none(s.text)
                if sid:
                    s_ids.append(sid)
                if sname:
                    s_names.append(sname)

            rows.append(
                {
                    "label_id": lid_raw,
                    "name": name,
                    "profile": profile,
                    "contact_info": contact,
                    "data_quality": dq,
                    "parent_label_id": parent_id,
                    "parent_label_name": parent_name,
                    "urls_csv": urls_csv,
                    "sublabel_ids_csv": join_csv(s_ids),
                    "sublabel_names_csv": join_csv(s_names),
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
    print(f"✅ Done. Parts written: {part}  Output: {out_dir}")
    if skipped_no_id:
        print(f"ℹ️ Skipped labels without numeric id: {skipped_no_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
