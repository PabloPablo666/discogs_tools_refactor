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


SRC = Path("/Users/paoloolivieri/discogs_store/raw/masters/discogs_20251101_masters.xml.gz")
OUT = Path("/Users/paoloolivieri/discogs_store/hive-data/masters_v1")

OUT.mkdir(parents=True, exist_ok=True)

BATCH = 50_000
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
        if elem.tag != "master":
            continue

        master_id = text_or_none(elem.get("id"))
        main_release_id = text_or_none(elem.findtext("main_release"))
        title = text_or_none(elem.findtext("title"))
        year_raw = text_or_none(elem.findtext("year"))
        data_quality = text_or_none(elem.findtext("data_quality"))

        year = None
        if year_raw is not None:
            try:
                year = int(year_raw)
            except ValueError:
                year = None

        master_artists_list = []
        master_artist_ids_list = []
        artists_elem = elem.find("artists")
        if artists_elem is not None:
            for a in artists_elem.findall("artist"):
                name = text_or_none(a.findtext("name"))
                aid = text_or_none(a.findtext("id"))
                if name:
                    master_artists_list.append(name)
                if aid:
                    master_artist_ids_list.append(aid)

        master_artists = ", ".join(master_artists_list) if master_artists_list else None
        master_artist_ids = ",".join(master_artist_ids_list) if master_artist_ids_list else None

        genres_list = []
        genres_elem = elem.find("genres")
        if genres_elem is not None:
            for g in genres_elem.findall("genre"):
                gtxt = text_or_none(g.text)
                if gtxt:
                    genres_list.append(gtxt)
        genres = ", ".join(genres_list) if genres_list else None

        styles_list = []
        styles_elem = elem.find("styles")
        if styles_elem is not None:
            for s in styles_elem.findall("style"):
                stxt = text_or_none(s.text)
                if stxt:
                    styles_list.append(stxt)
        styles = ", ".join(styles_list) if styles_list else None

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

        if len(rows) >= BATCH:
            flush()

        elem.clear()

flush()
print("✅ Done. Output:", OUT)
