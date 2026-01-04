#!/usr/bin/env python3
from pathlib import Path
import pandas as pd

def main():
    src = Path("/tmp/hive-data/warehouse_discogs/artist_name_map_v1")
    out = Path("/Users/paoloolivieri/discogs_store/hive-data/artist_name_map_unamb_v1")

    if not src.exists():
        raise SystemExit(f"Sorgente non trovata: {src}")

    files = sorted(src.glob("*.parquet"))
    if not files:
        raise SystemExit(f"Nessun parquet trovato in {src}")

    print(f"🔎 Trovati {len(files)} file parquet in {src}")
    dfs = []
    for f in files:
        print(f"   -> leggendo {f}")
        df_part = pd.read_parquet(f)
        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True)
    print(f"📦 DataFrame totale: {len(df)} righe")

    # ci assicuriamo che i campi chiave siano stringhe
    df["norm_name"] = df["norm_name"].astype("string")
    df["artist_id"] = df["artist_id"].astype("string")

    print("🔁 Calcolo numero di artist_id distinti per norm_name...")
    counts = df.groupby("norm_name")["artist_id"].nunique()
    good_names = counts[counts == 1].index
    print(f"✅ norm_name univoci: {len(good_names)}")

    print("🔍 Filtro righe per norm_name non ambiguo...")
    df_unamb = df[df["norm_name"].isin(good_names)]
    print(f"📦 DataFrame unamb: {len(df_unamb)} righe")

    out.mkdir(parents=True, exist_ok=True)
    out_file = out / "part-00000.parquet"
    df_unamb.to_parquet(out_file, index=False)
    print(f"💾 Scritto parquet pulito in: {out_file}")

if __name__ == "__main__":
    main()
