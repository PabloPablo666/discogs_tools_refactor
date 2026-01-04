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
OUT = Path("/Users/paoloolivieri/discogs_store/hive-data/releases_v5")  # nuova cartella
OUT.mkdir(parents=True, exist_ok=True)

rows = []
BATCH = 50000
part = 0

def flush():
    global rows, part
    if not rows:
        return

    df = pd.DataFrame(rows, columns=[
        # campi che avevi già
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

        # nuovi campi di release
        "status",
        "released",
        "data_quality",
        "is_main_release",

        # nuovi campi di format
        "format_qtys",
        "format_texts",
        "format_descriptions",

        # identificatori & companies
        "identifiers_flat",
        "companies_flat",
    ])

    # tipi numerici dove ha senso
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
    # iterparse in streaming
    for event, elem in ET.iterparse(f, events=("end",)):
        if elem.tag != "release":
            continue

        # attributi / campi base
        rid = text_or_none(elem.get("id"))
        status = text_or_none(elem.get("status"))

        mid = text_or_none(elem.findtext("master_id"))
        title = text_or_none(elem.findtext("title"))
        year = text_or_none(elem.findtext("year"))
        country = text_or_none(elem.findtext("country"))

        released = text_or_none(elem.findtext("released"))
        data_quality = text_or_none(elem.findtext("data_quality"))
        is_main_release = text_or_none(elem.findtext("is_main_release"))
        # notes volendo lo puoi aggiungere qui
        # notes = text_or_none(elem.findtext("notes"))

        # ARTISTS (main)
        artists_nodes = elem.findall("artists/artist")
        artists = ", ".join(
            text_or_none(a.findtext("name")) for a in artists_nodes
            if text_or_none(a.findtext("name"))
        ) or None

        # LABELS
        labels_nodes = elem.findall("labels/label")
        labels = ", ".join(
            text_or_none(l.get("name")) for l in labels_nodes
            if text_or_none(l.get("name"))
        ) or None

        catnos = ", ".join(
            text_or_none(l.get("catno")) for l in labels_nodes
            if text_or_none(l.get("catno"))
        ) or None

        # FORMATS (esteso)
        format_names = []
        format_qtys = []
        format_texts = []
        format_descs = []

        for fmt in elem.findall("formats/format"):
            name = text_or_none(fmt.get("name"))
            qty = text_or_none(fmt.get("qty"))
            text_attr = text_or_none(fmt.get("text"))

            if name:
                format_names.append(name)
            if qty:
                format_qtys.append(qty)
            if text_attr:
                format_texts.append(text_attr)

            # descriptions/description
            for d in fmt.findall("descriptions/description"):
                desc = text_or_none(d.text)
                if desc:
                    format_descs.append(desc)

        formats = ", ".join(format_names) or None
        format_qtys_str = ", ".join(format_qtys) or None
        format_texts_str = ", ".join(format_texts) or None
        format_descs_str = ", ".join(format_descs) or None

        # GENRES / STYLES
        genres = ", ".join(
            text_or_none(g.text) for g in elem.findall("genres/genre")
            if text_or_none(g.text)
        ) or None

        styles = ", ".join(
            text_or_none(s.text) for s in elem.findall("styles/style")
            if text_or_none(s.text)
        ) or None

        # EXTRAARTISTS / CREDITS
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

        # IDENTIFIERS (barcode, matrix, etc.)
        identifier_chunks = []
        for ident in elem.findall("identifiers/identifier"):
            t = text_or_none(ident.get("type"))
            desc = text_or_none(ident.get("description"))
            val = text_or_none(ident.text)

            # costruisco stringhe tipo: "Barcode [EAN]: 1234"
            parts = []
            if t:
                parts.append(t)
            if desc:
                parts.append(f"[{desc}]")
            if val:
                parts.append(f": {val}")
            if parts:
                identifier_chunks.append(" ".join(parts))

        identifiers_flat = "; ".join(identifier_chunks) or None

        # COMPANIES (Pressed By, Recorded At, ecc.)
        company_chunks = []
        for comp in elem.findall("companies/company"):
            name = text_or_none(comp.get("name"))
            role = text_or_none(comp.get("entity_type_name"))
            catno_comp = text_or_none(comp.get("catno"))

            if name or role:
                s = ""
                if role:
                    s += f"{role}: "
                if name:
                    s += name
                if catno_comp:
                    s += f" ({catno_comp})"
                company_chunks.append(s)

        companies_flat = "; ".join(company_chunks) or None

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

            "status": status,
            "released": released,
            "data_quality": data_quality,
            "is_main_release": is_main_release,

            "format_qtys": format_qtys_str,
            "format_texts": format_texts_str,
            "format_descriptions": format_descs_str,

            "identifiers_flat": identifiers_flat,
            "companies_flat": companies_flat,
        })

        if len(rows) >= BATCH:
            flush()

        # importantissimo per non mangiarsi 64 GB di RAM
        elem.clear()

flush()
print("✅ Done. Output:", OUT)

