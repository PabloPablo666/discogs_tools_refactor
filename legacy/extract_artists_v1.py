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
OUT = Path("/Users/paoloolivieri/discogs_store/hive-data/artists_v1")

OUT.mkdir(parents=True, exist_ok=True)

BATCH = 50000
rows = []
part = 0

def flush():
    global rows, part
    if not rows:
        return
    df = pd.DataFrame(rows)
    out_file = OUT / f"part-{part:05d}.parquet"
    df.to_parquet(out_file, index=False)
    print(f"💾 Written {len(df)} rows to {out_file}")
    rows = []
    part += 1

with gzip.open(SRC, "rb") as f:
    for event, elem in ET.iterparse(f, events=("end",)):
        if elem.tag != "artist":
            continue

        artist_id = text_or_none(elem.findtext("id"))
        name = text_or_none(elem.findtext("name"))
        realname = text_or_none(elem.findtext("realname"))
        profile = text_or_none(elem.findtext("profile"))
        data_quality = text_or_none(elem.findtext("data_quality"))

        urls_list = []
        urls_elem = elem.find("urls")
        if urls_elem is not None:
            for u in urls_elem.findall("url"):
                utxt = text_or_none(u.text)
                if utxt:
                    urls_list.append(utxt)
        urls = ", ".join(urls_list) if urls_list else None

        nv_list = []
        nv_elem = elem.find("namevariations")
        if nv_elem is not None:
            for n in nv_elem.findall("name"):
                ntxt = text_or_none(n.text)
                if ntxt:
                    nv_list.append(ntxt)
        namevariations = ", ".join(nv_list) if nv_list else None

        aliases_list = []
        aliases_elem = elem.find("aliases")
        if aliases_elem is not None:
            for a in aliases_elem.findall("name"):
                atxt = text_or_none(a.text)
                if atxt:
                    aliases_list.append(atxt)
        aliases = ", ".join(aliases_list) if aliases_list else None

        rows.append({
            "artist_id": artist_id,
            "name": name,
            "realname": realname,
            "profile": profile,
            "data_quality": data_quality,
            "urls": urls,
            "namevariations": namevariations,
            "aliases": aliases
        })

        if len(rows) >= BATCH:
            flush()

        elem.clear()

flush()
print("✅ Done. Output:", OUT)
