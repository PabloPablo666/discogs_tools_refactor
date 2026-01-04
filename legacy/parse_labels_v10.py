#!/usr/bin/env python3
import gzip
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

def t(x):
    if x is None:
        return None
    x = str(x).strip()
    return x or None

SRC = Path("/Users/paoloolivieri/discogs_store/raw/labels/discogs_20251101_labels.xml.gz")
OUT = Path("/Users/paoloolivieri/discogs_store/hive-data/labels_v10")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
BATCH = 50000
part = 0

def flush():
    global rows, part
    if not rows:
        return
    df = pd.DataFrame(rows, columns=[
        "label_id",
        "name",
        "profile",
        "contact_info",
        "data_quality",
        "parent_label_id",
        "parent_label_name",
        "urls_csv",
        "sublabel_ids_csv",
        "sublabel_names_csv"
    ])

    df["label_id"] = pd.to_numeric(df["label_id"], errors="coerce").astype("Int64")
    df["parent_label_id"] = pd.to_numeric(df["parent_label_id"], errors="coerce").astype("Int64")

    df.to_parquet(
        OUT / f"labels_part{part:04d}.parquet",
        engine="fastparquet",
        index=False
    )
    print(f"🧱 written labels_part{part:04d}.parquet  ({len(df)} rows)")
    rows.clear()
    part += 1

with gzip.open(SRC, "rb") as f:
    for event, elem in ET.iterparse(f, events=("end",)):
        if elem.tag != "label":
            continue

        lid = t(elem.get("id"))
        name = t(elem.findtext("name"))
        profile = t(elem.findtext("profile"))
        contact = t(elem.findtext("contactinfo"))
        dq = t(elem.findtext("data_quality"))

        parent = elem.find("parent_label")
        parent_id = parent.get("id") if parent is not None else None
        parent_name = parent.text.strip() if parent is not None and parent.text else None

        urls = [
            t(u.text)
            for u in elem.findall("urls/url")
            if t(u.text)
        ]
        urls_csv = ", ".join(urls) or None

        s_ids = []
        s_names = []
        for s in elem.findall("sublabels/label"):
            sid = t(s.get("id"))
            sname = t(s.text)
            if sid:
                s_ids.append(sid)
            if sname:
                s_names.append(sname)

        s_ids_csv = ", ".join(s_ids) or None
        s_names_csv = ", ".join(s_names) or None

        rows.append({
            "label_id": lid,
            "name": name,
            "profile": profile,
            "contact_info": contact,
            "data_quality": dq,
            "parent_label_id": parent_id,
            "parent_label_name": parent_name,
            "urls_csv": urls_csv,
            "sublabel_ids_csv": s_ids_csv,
            "sublabel_names_csv": s_names_csv
        })

        if len(rows) >= BATCH:
            flush()
        elem.clear()

flush()
print("✅ Done. Output:", OUT)
