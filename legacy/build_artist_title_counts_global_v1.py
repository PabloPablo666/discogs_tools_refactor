#!/usr/bin/env python3
from pathlib import Path
import pandas as pd


BASE = Path("/Users/paoloolivieri/discogs_store/hive-data")

SRC_RELEASES = BASE / "releases_v6"
SRC_NAME_MAP = BASE / "artist_name_map_unamb_v1"

OUT_DIR = BASE / "artist_title_counts_global_v1"


def load_parquet_dir(path: Path, columns=None) -> pd.DataFrame:
    files = sorted(path.glob("*.parquet"))
    if not files:
        raise SystemExit(f"Nessun parquet trovato in {path}")
    dfs = []
    print(f"🔎 Trovati {len(files)} file in {path}")
    for f in files:
        print(f"   -> leggendo {f}")
        dfs.append(pd.read_parquet(f, columns=columns))
    return pd.concat(dfs, ignore_index=True)


def main():
    print("📥 Carico mappa nomi unamb (artist_name_map_unamb_v1)...")
    df_map = load_parquet_dir(SRC_NAME_MAP, columns=["norm_name", "artist_id"])
    # normalizza
    df_map["norm_name"] = df_map["norm_name"].astype(str).str.strip().str.lower()
    df_map["artist_id"] = df_map["artist_id"].astype(str)

    # per join più rapida
    df_map = df_map.drop_duplicates(subset=["norm_name"])

    print(f"📦 name_map: {len(df_map)} righe uniche su norm_name")

    print("📥 Carico releases_v6 (solo title, artists)...")
    df_rel = load_parquet_dir(
        SRC_RELEASES,
        columns=["release_id", "title", "artists"]
    )
    print(f"📦 releases_v6: {len(df_rel)} righe")

    # normalizza title
    df_rel["title_norm"] = (
        df_rel["title"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    # filtra righe senza titolo
    df_rel = df_rel[df_rel["title_norm"] != ""]

    # esplodi artisti
    print("🔁 Esplodo artisti per release...")
    df_rel = df_rel.dropna(subset=["artists"])
    df_rel["artists"] = df_rel["artists"].astype(str)

    # split alla vecchia maniera
    exploded_rows = []
    for idx, row in df_rel.iterrows():
        artists_str = row["artists"]
        parts = [a.strip() for a in artists_str.split(",") if a.strip()]
        for a in parts:
            exploded_rows.append(
                {
                    "release_id": row["release_id"],
                    "title_norm": row["title_norm"],
                    "artist_name": a,
                    "norm_name": a.strip().lower(),
                }
            )

    df_exp = pd.DataFrame(exploded_rows)
    print(f"📦 rel_artists esplosi: {len(df_exp)} righe")

    # JOIN con la mappa unamb
    print("🔗 Join con artist_name_map_unamb_v1...")
    df_join = df_exp.merge(
        df_map,
        how="inner",
        on="norm_name"
    )

    print(f"📦 dopo join: {len(df_join)} righe")

    # ci basta artist_id + title_norm
    df_join = df_join[["artist_id", "title_norm"]].drop_duplicates()

    print("🔢 Calcolo n_distinct_titles per artist_id...")
    counts = (
        df_join
        .groupby("artist_id")["title_norm"]
        .nunique()
        .reset_index()
        .rename(columns={"title_norm": "n_distinct_titles"})
    )

    print(f"📦 artist_title_counts_global_v1: {len(counts)} righe")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / "part-00000.parquet"
    counts.to_parquet(out_file, index=False)
    print(f"💾 Salvato {out_file}")


if __name__ == "__main__":
    main()
