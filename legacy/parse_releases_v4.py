#!/usr/bin/env python3
import gzip
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

def text_or_none(x):
    if x is None:
        return None
    x = str(x).strip()
    return x or None

SRC = Path("/Users/paoloolivieri/discogs_store/raw/releases/discogs_20251101_releases.xml.gz")
OUT = Path("/Users/paoloolivieri/discogs_store/hive-data/releases_v4")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
BATCH = 50000
part = 0

def flush():
    global rows, part
    if not rows:
        return
    df = pd.DataFrame(rows, columns=[
        "release_id",
        "master_id",
        "title",
        "artists",
        "labels",
        "label_catnos",
        "country",
        "year",
        "formats",
        "genres",
        "styles",
        "credits_flat",
    ])
    for col in ["release_id", "master_id", "year"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df.to_parquet(
        OUT / f"releases_part{part:04d}.parquet",
        engine="fastparquet",
        index=False
    )
    print(f"🧱 written releases_part{part:04d}.parquet  ({len(df)} rows)")
    rows.clear()
    part += 1

with gzip.open(SRC, "rb") as f:
    for event, elem in ET.iterparse(f, events=("end",)):
        if elem.tag != "release":
            continue

        rid = text_or_none(elem.get("id"))
        mid = text_or_none(elem.findtext("master_id"))
        title = text_or_none(elem.findtext("title"))
        year = text_or_none(elem.findtext("year"))
        country = text_or_none(elem.findtext("country"))

        artists_nodes = elem.findall("artists/artist")
        artists = ", ".join(
            text_or_none(a.findtext("name")) for a in artists_nodes
            if text_or_none(a.findtext("name"))
        ) or None

        labels_nodes = elem.findall("labels/label")
        labels = ", ".join(
            text_or_none(l.get("name")) for l in labels_nodes
            if text_or_none(l.get("name"))
        ) or None

        catnos = ", ".join(
            text_or_none(l.get("catno")) for l in labels_nodes
            if text_or_none(l.get("catno"))
        ) or None

        formats = ", ".join(
            text_or_none(f.get("name")) for f in elem.findall("formats/format")
            if text_or_none(f.get("name"))
        ) or None

        genres = ", ".join(
            text_or_none(g.text) for g in elem.findall("genres/genre")
            if text_or_none(g.text)
        ) or None

        styles = ", ".join(
            text_or_none(s.text) for s in elem.findall("styles/style")
            if text_or_none(s.text)
        ) or None

        credits_pairs = []
        for c in elem.findall("extraartists/artist"):
            nm = text_or_none(c.findtext("name"))
            rl = text_or_none(c.findtext("role"))
            if nm and rl:
                credits_pairs.append(f"{rl}: {nm}")
            elif nm:
                credits_pairs.append(nm)
            elif rl:
                credits_pairs.append(rl)

        credits_flat = "; ".join(credits_pairs) or None

        rows.append({
            "release_id": rid,
            "master_id": mid,
            "title": title,
            "artists": artists,
            "labels": labels,
            "label_catnos": catnos,
            "country": country,
            "year": year,
            "formats": formats,
            "genres": genres,
            "styles": styles,
            "credits_flat": credits_flat,
        })

        if len(rows) >= BATCH:
            flush()
        elem.clear()

flush()
print("✅ Done. Output:", OUT)
