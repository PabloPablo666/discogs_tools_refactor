#!/usr/bin/env python3
import gzip, os
import pandas as pd
from lxml import etree

SRC = "/tmp/hive-data/discogs_dump/discogs_20250101_artists.xml.gz"
DST = "/tmp/hive-data/artists_parquet"
os.makedirs(DST, exist_ok=True)

rows, part, CH = [], 0, 20000

def t(x):
    return x.strip() if x and x.strip() else None

def flush():
    global rows, part
    if not rows: return
    cols = ["artist_id","name","realname","profile","data_quality"]
    df = pd.DataFrame(rows, columns=cols)
    df["artist_id"] = pd.to_numeric(df["artist_id"], errors="coerce").astype("Int64")
    for c in cols:
        if c != "artist_id":
            df[c] = df[c].astype("string")
    df.to_parquet(f"{DST}/artists_part{part:03d}.parquet", engine="fastparquet", index=False)
    rows.clear(); part += 1

with gzip.open(SRC, "rb") as f:
    for _, e in etree.iterparse(f, events=("end",), tag="artist"):
        rows.append({
            "artist_id": e.findtext("id") or e.get("id"),
            "name": t(e.findtext("name")),
            "realname": t(e.findtext("realname")),
            "profile": t(e.findtext("profile")),
            "data_quality": t(e.findtext("data_quality")),
        })
        if len(rows) >= CH:
            flush()
        e.clear()
        while e.getprevious() is not None:
            del e.getparent()[0]
flush()
print("OK:", DST)
