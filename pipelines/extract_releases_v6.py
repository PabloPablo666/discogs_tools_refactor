#!/usr/bin/env python3
"""
Discogs → releases_v6 (Parquet)

Streaming parse of Discogs releases XML dump (gzipped) → Parquet dataset aligned with:
hive.discogs.releases_ref_v6

Key guarantees:
- Text fields are always str or None (never bytes), so schema stays VARCHAR in DuckDB/Trino.
- Output schema is declared ONCE at the top (scalable + maintainable).
"""

from __future__ import annotations

import argparse
import gzip
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd


# ============================================================
# Output schema (DECLARE ONCE)
# ============================================================

OUTPUT_COLUMNS: List[str] = [
    "release_id",
    "master_id",
    "title",
    "artists",
    "labels",
    "label_catnos",
    "country",
    "formats",
    "genres",
    "styles",
    "credits_flat",
    "status",
    "released",
    "data_quality",
    "format_qtys",
    "format_texts",
    "format_descriptions",
    "identifiers_flat",
]

ID_COLUMNS: List[str] = ["release_id", "master_id"]

TEXT_COLUMNS: List[str] = [
    c for c in OUTPUT_COLUMNS if c not in ID_COLUMNS
]


# ============================================================
# Helpers
# ============================================================

def safe_text(x) -> Optional[str]:
    """
    Return clean UTF-8 text or None.

    - bytes are decoded as UTF-8 with replacement
    - everything else is str()'d
    """
    if x is None:
        return None
    if isinstance(x, bytes):
        try:
            s = x.decode("utf-8", errors="replace")
        except Exception:
            return None
    else:
        s = str(x)
    s = s.strip()
    return s or None


def join_csv(items: List[Any]) -> Optional[str]:
    out: List[str] = []
    for it in items:
        s = safe_text(it)
        if s:
            out.append(s)
    return ", ".join(out) if out else None


def parquet_engine_preferred() -> str:
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except Exception:
        return "fastparquet"


def resolve_paths(src_arg: Optional[str], out_arg: Optional[str]) -> tuple[Path, Path]:
    data_lake_root = Path(os.environ.get("DISCOGS_DATA_LAKE", "/data/hive-data")).expanduser()
    raw_root = Path(os.environ.get("DISCOGS_RAW", "/data/raw")).expanduser()

    src_env = os.environ.get("DISCOGS_RELEASES_DUMP")
    if src_arg:
        src = Path(src_arg).expanduser()
    elif src_env:
        src = Path(src_env).expanduser()
    else:
        src = raw_root / "releases" / "discogs_releases.xml.gz"

    out = Path(out_arg).expanduser() if out_arg else (data_lake_root / "releases_v6")
    return src, out


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract Discogs releases dump into Parquet dataset releases_v6.")
    p.add_argument("--src", help="Path to Discogs releases XML dump (*.xml.gz). Overrides DISCOGS_RELEASES_DUMP.")
    p.add_argument("--out", help="Output directory for Parquet parts. Default: $DISCOGS_DATA_LAKE/releases_v6")
    p.add_argument("--batch", type=int, default=50_000, help="Rows per Parquet part (default: 50000).")
    p.add_argument("--engine", choices=["pyarrow", "fastparquet"], default=None, help="Parquet engine override.")
    p.add_argument("--max-parts", type=int, default=None, help="Stop after writing N parts (debug).")
    p.add_argument("--clean", action="store_true", help="Delete existing parquet files in output dir before writing.")
    return p.parse_args()


# ============================================================
# Main
# ============================================================

def main() -> int:
    args = parse_args()
    src, out_dir = resolve_paths(args.src, args.out)

    if not src.exists():
        print(f"ERROR: source file not found: {src}", file=sys.stderr)
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        for p in out_dir.glob("*.parquet"):
            p.unlink()

    engine = args.engine or parquet_engine_preferred()

    rows: List[Dict[str, Any]] = []
    part = 0

    def flush() -> None:
        nonlocal rows, part
        if not rows:
            return

        df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

        # IDs → numeric nullable
        for c in ID_COLUMNS:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        # Text columns → force string/None (prevents DuckDB inferring BLOB)
        for c in TEXT_COLUMNS:
            df[c] = df[c].map(safe_text).astype("string")

        out_file = out_dir / f"releases_part{part:04d}.parquet"
        df.to_parquet(out_file, engine=engine, index=False)
        print(f"🧱 written {out_file.name}  ({len(df):,} rows)")

        rows.clear()
        part += 1

    print(f"📥 Source: {src}")
    print(f"📦 Output: {out_dir}")
    print(f"📐 Columns: {len(OUTPUT_COLUMNS)} (IDs={len(ID_COLUMNS)}, text={len(TEXT_COLUMNS)})")
    print(f"⚙️  batch={args.batch} engine={engine}")

    with gzip.open(src, "rb") as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "release":
                continue

            release_id = safe_text(elem.get("id"))
            master_id = safe_text(elem.findtext("master_id"))
            title = safe_text(elem.findtext("title"))
            country = safe_text(elem.findtext("country"))
            status = safe_text(elem.findtext("status"))
            released = safe_text(elem.findtext("released"))
            data_quality = safe_text(elem.findtext("data_quality"))

            # Artists
            artists_nodes = elem.findall("artists/artist")
            artists = join_csv([a.findtext("name") for a in artists_nodes])

            # Labels + catnos
            labels_nodes = elem.findall("labels/label")
            labels = join_csv([l.get("name") for l in labels_nodes])
            label_catnos = join_csv([l.get("catno") for l in labels_nodes])

            # Formats + qty/text/descriptions
            format_names: List[str] = []
            format_qtys: List[str] = []
            format_texts: List[str] = []
            format_descs: List[str] = []

            for fmt in elem.findall("formats/format"):
                nm = safe_text(fmt.get("name"))
                qt = safe_text(fmt.get("qty"))
                tx = safe_text(fmt.get("text"))

                if nm: format_names.append(nm)
                if qt: format_qtys.append(qt)
                if tx: format_texts.append(tx)

                for d in fmt.findall("descriptions/description"):
                    desc = safe_text(d.text)
                    if desc:
                        format_descs.append(desc)

            formats = join_csv(format_names)
            format_qtys_str = join_csv(format_qtys)
            format_texts_str = join_csv(format_texts)
            format_descs_str = join_csv(format_descs)

            # Genres / Styles
            genres = join_csv([g.text for g in elem.findall("genres/genre")])
            styles = join_csv([s.text for s in elem.findall("styles/style")])

            # Credits (extraartists)
            credits_pairs: List[str] = []
            for ac in elem.findall("extraartists/artist"):
                nm = safe_text(ac.findtext("name"))
                rl = safe_text(ac.findtext("role"))
                if rl and nm:
                    credits_pairs.append(f"{rl}: {nm}")
                elif nm:
                    credits_pairs.append(nm)
                elif rl:
                    credits_pairs.append(rl)
            credits_flat = "; ".join(credits_pairs) if credits_pairs else None

            # Identifiers
            identifier_chunks: List[str] = []
            for ident in elem.findall("identifiers/identifier"):
                t = safe_text(ident.get("type"))
                desc = safe_text(ident.get("description"))
                val = safe_text(ident.get("value")) or safe_text(ident.text)

                if not (t or desc or val):
                    continue

                head = t or ""
                if desc:
                    head = f"{head} [{desc}]".strip()
                chunk = f"{head} : {val}".strip() if val else head.strip()
                if chunk:
                    identifier_chunks.append(chunk)

            identifiers_flat = "; ".join(identifier_chunks) if identifier_chunks else None

            rows.append(
                {
                    "release_id": release_id,
                    "master_id": master_id,
                    "title": title,
                    "artists": artists,
                    "labels": labels,
                    "label_catnos": label_catnos,
                    "country": country,
                    "formats": formats,
                    "genres": genres,
                    "styles": styles,
                    "credits_flat": credits_flat,
                    "status": status,
                    "released": released,
                    "data_quality": data_quality,
                    "format_qtys": format_qtys_str,
                    "format_texts": format_texts_str,
                    "format_descriptions": format_descs_str,
                    "identifiers_flat": identifiers_flat,
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
