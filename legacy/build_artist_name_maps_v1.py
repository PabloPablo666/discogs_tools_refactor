#!/usr/bin/env python3
from pathlib import Path
import pandas as pd

BASE = Path("/Users/paoloolivieri/discogs_store/hive-data")

SRC_ARTISTS = BASE / "artists_v1"
SRC_ALIASES = BASE / "artist_aliases_v1"

OUT_MAP      = BASE / "artist_name_map_v1"
OUT_MAP_UNAMB = BASE / "artist_name_map_unamb_v1"


def load_parquet_dir(path: Path) -> pd.DataFrame:
    files = sorted(path.glob("*.parquet"))
    if not files:
        raise SystemExit(f"Nessun parquet trovato in {path}")
    dfs = []
    print(f"🔎 Trovati {len(files)} file in {path}")
    for f in files:
        print(f"   -> leggendo {f}")
        dfs.append(pd.read_parquet(f))
    return pd.concat(dfs, ignore_index=True)


def main():
    # 1) Carica artists_v1 e aliases
    print("📥 Carico artists_v1...")
    df_art = load_parquet_dir(SRC_ARTISTS)[["artist_id", "name"]]

    print("📥 Carico artist_aliases_v1...")
    df_alias = load_parquet_dir(SRC_ALIASES)[["artist_id", "alias_name"]]

    # 2) Normalizza e costruisce la mappa completa
    print("🔁 Costruisco mappa nomi completa (primary + alias)...")

    df_art = df_art.dropna(subset=["name"])
    df_art["norm_name"] = df_art["name"].astype(str).str.strip().str.lower()
    df_art = df_art[df_art["norm_name"] != ""]
    df_art["name_type"] = "primary"

    df_alias = df_alias.dropna(subset=["alias_name"])
    df_alias["norm_name"] = df_alias["alias_name"].astype(str).str.strip().str.lower()
    df_alias = df_alias[df_alias["norm_name"] != ""]
    df_alias["name_type"] = "alias"
    df_alias = df_alias.rename(columns={"alias_name": "name"})

    df_map = pd.concat(
        [
            df_art[["norm_name", "artist_id", "name_type"]],
            df_alias[["norm_name", "artist_id", "name_type"]],
        ],
        ignore_index=True,
    )

    print(f"📦 artist_name_map_v1: {len(df_map)} righe")

    OUT_MAP.mkdir(parents=True, exist_ok=True)
    out_map_file = OUT_MAP / "part-00000.parquet"
    df_map.to_parquet(out_map_file, index=False)
    print(f"💾 Salvato mapping completo in {out_map_file}")

    # 3) Calcola i norm_name non ambigui (1 solo artist_id)
    print("🔍 Calcolo norm_name univoci (1 solo artist_id)...")
    df_map["artist_id"] = df_map["artist_id"].astype("string")
    df_map["norm_name"] = df_map["norm_name"].astype("string")

    counts = df_map.groupby("norm_name")["artist_id"].nunique()
    good_names = counts[counts == 1].index
    print(f"✅ norm_name univoci: {len(good_names)}")

    df_unamb = df_map[df_map["norm_name"].isin(good_names)].copy()
    print(f"📦 artist_name_map_unamb_v1: {len(df_unamb)} righe")

    OUT_MAP_UNAMB.mkdir(parents=True, exist_ok=True)
    out_unamb_file = OUT_MAP_UNAMB / "part-00000.parquet"
    df_unamb.to_parquet(out_unamb_file, index=False)
    print(f"💾 Salvato mapping unamb in {out_unamb_file}")


if __name__ == "__main__":
    main()
