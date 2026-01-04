#!/usr/bin/env python3
import os, glob, json
import pandas as pd

SRC_DIR = "/Users/paoloolivieri/discogs_data"
PATTERN = "discogs_page_*.json"
DST_DIR = "/tmp/hive-data/discogs_parquet"
os.makedirs(DST_DIR, exist_ok=True)

def norm(s):
    if s is None:
        return None
    s = str(s).strip()
    return s if s else None

def load_items(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    if isinstance(data, list):
        return data
    return []

rows, part, CH = [], 0, 20000

files = sorted(glob.glob(os.path.join(SRC_DIR, PATTERN)))
print(f"Trovati {len(files)} file JSON in {SRC_DIR}")

for fp in files:
    items = load_items(fp)
    for it in items:
        d = it.get("basic_information", {}) if isinstance(it, dict) else it
        instance_id = it.get("id") or it.get("instance_id") or d.get("instance_id")
        release_id  = d.get("id") or it.get("release_id")
        title   = d.get("title")
        year    = d.get("year")
        genres  = d.get("genres")
        styles  = d.get("styles")
        labels  = d.get("labels")
        artists = d.get("artists")
        formats = d.get("formats")
        date_added = it.get("date_added") or d.get("date_added")
        rating = (it.get("rating") or it.get("rating_value") or d.get("rating") or
                  it.get("notes", {}).get("rating"))

        def join_names(arr, key="name"):
            if not isinstance(arr, list): return None
            out = [norm(x.get(key)) for x in arr if isinstance(x, dict) and norm(x.get(key))]
            return ", ".join(out) if out else None

        def join_values(arr):
            if not isinstance(arr, list): return None
            out = [norm(x) for x in arr if norm(x)]
            return ", ".join(out) if out else None

        rows.append({
            "instance_id": instance_id,
            "release_id": release_id,
            "title": norm(title),
            "artists": join_names(artists, "name"),
            "labels": join_names(labels, "name"),
            "year": year if isinstance(year, int) else (int(year) if str(year).isdigit() else None),
            "formats": join_names(formats, "name"),
            "genres": join_values(genres),
            "styles": join_values(styles),
            "date_added": norm(date_added),
            "rating": int(rating) if str(rating).isdigit() else None
        })

        if len(rows) >= CH:
            df = pd.DataFrame(rows)
            df.to_parquet(f"{DST_DIR}/collection_part{part:03d}.parquet",
                          engine="fastparquet", index=False)
            rows.clear(); part += 1

if rows:
    df = pd.DataFrame(rows)
    df.to_parquet(f"{DST_DIR}/collection_part{part:03d}.parquet",
                  engine="fastparquet", index=False)

print("OK:", DST_DIR)
