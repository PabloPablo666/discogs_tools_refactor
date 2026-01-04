#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import re


BASE = Path("/Users/paoloolivieri/discogs_store/hive-data")

SRC_RELEASES = BASE / "releases_v6"
SRC_NAME_MAP = BASE / "artist_name_map_unamb_v1"
SRC_TITLE_COUNTS = BASE / "artist_title_counts_global_v1"
SRC_ARTISTS = BASE / "artists_v1"

OUT_DIR = Path("/Users/paoloolivieri/discogs_store/results/queries")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PARQUET = OUT_DIR / "ph_electro_one_shots_1996_2009.parquet"
OUT_CSV = OUT_DIR / "ph_electro_one_shots_1996_2009.csv"


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


def norm_year_from_released(released):
    if not isinstance(released, str):
        return None
    released = released.strip()
    if len(released) < 4:
        return None
    year_str = released[:4]
    if not year_str.isdigit():
        return None
    y = int(year_str)
    if 1900 <= y <= 2100:
        return y
    return None


def split_norm_list(s: str):
    if not isinstance(s, str):
        return []
    s = s.strip().lower()
    if not s:
        return []
    parts = re.split(r"[,;/]+", s)
    return [p.strip() for p in parts if p.strip()]


def main():
    # 1) Carico mappa unamb
    print("📥 Carico artist_name_map_unamb_v1...")
    df_map = load_parquet_dir(SRC_NAME_MAP, columns=["norm_name", "artist_id"])
    df_map["norm_name"] = df_map["norm_name"].astype(str).str.strip().str.lower()
    df_map["artist_id"] = df_map["artist_id"].astype(str)
    df_map = df_map.drop_duplicates(subset=["norm_name"])
    print(f"📦 name_map_unamb: {len(df_map)} righe uniche su norm_name")

    # 2) Carico artist_title_counts_global_v1 e prendo gli one-shot globali
    print("📥 Carico artist_title_counts_global_v1...")
    df_counts = load_parquet_dir(SRC_TITLE_COUNTS, columns=["artist_id", "n_distinct_titles"])
    df_counts["artist_id"] = df_counts["artist_id"].astype(str)
    one_shot_ids = set(
        df_counts.loc[df_counts["n_distinct_titles"] == 1, "artist_id"].tolist()
    )
    print(f"✅ artist one-shot globali: {len(one_shot_ids)}")

    # filtra la mappa solo ai one-shot (riduciamo un po' il join dopo)
    df_map = df_map[df_map["artist_id"].isin(one_shot_ids)].copy()
    print(f"📦 name_map_unamb filtrata su one-shot: {len(df_map)} righe")

    # 3) Carico artists_v1 per avere il nome canonico
    print("📥 Carico artists_v1 (solo id+name)...")
    df_art = load_parquet_dir(SRC_ARTISTS, columns=["artist_id", "name"])
    df_art["artist_id"] = df_art["artist_id"].astype(str)

    # 4) Processiamo releases_v6 file per file
    files = sorted(SRC_RELEASES.glob("*.parquet"))
    if not files:
        raise SystemExit(f"Nessun parquet trovato in {SRC_RELEASES}")

    results = []

    for f in files:
        print(f"📀 Processo file releases: {f}")
        df_rel = pd.read_parquet(
            f,
            columns=[
                "release_id",
                "title",
                "artists",
                "labels",
                "label_catnos",
                "country",
                "genres",
                "styles",
                "formats",
                "format_descriptions",
                "released",
            ],
        )

        # anno normalizzato
        df_rel["year_norm"] = df_rel["released"].apply(norm_year_from_released)

        # filtro per anno
        df_rel = df_rel[
            (df_rel["year_norm"] >= 1996) & (df_rel["year_norm"] <= 2009)
        ].copy()
        if df_rel.empty:
            print("   -> nessuna release nel range anno, skip")
            continue

        # check vinile
        df_rel["formats_lc"] = df_rel["formats"].astype(str).str.lower()
        df_rel = df_rel[df_rel["formats_lc"].str.contains("vinyl", na=False)].copy()
        if df_rel.empty:
            print("   -> nessun vinile in questo file, skip")
            continue

        # genera liste normalizzate di generi e stili
        df_rel["genres_norm"] = df_rel["genres"].apply(split_norm_list)
        df_rel["styles_norm"] = df_rel["styles"].apply(split_norm_list)

        allowed_styles = {"progressive house", "electro", "house"}

        def style_filter(row):
            gs = row["genres_norm"] or []
            ss = row["styles_norm"] or []
            if "electronic" not in gs:
                return False
            sset = set(ss)
            # devono esserci progressive house E electro
            if not ({"progressive house", "electro"} <= sset):
                return False
            # nessun altro stile oltre progressive house, electro, house
            if not sset.issubset(allowed_styles):
                return False
            return True

        df_rel = df_rel[df_rel.apply(style_filter, axis=1)].copy()
        if df_rel.empty:
            print("   -> nessuna release PH+Electro in questo file, skip")
            continue

        # label_catnos non vuoto
        df_rel["label_catnos_str"] = df_rel["label_catnos"].astype(str).str.strip()
        df_rel = df_rel[df_rel["label_catnos_str"] != ""].copy()
        if df_rel.empty:
            print("   -> tutte senza catno valido, skip")
            continue

        # esplodiamo artists
        df_rel["artists_str"] = df_rel["artists"].astype(str)
        rows = []
        for idx, row in df_rel.iterrows():
            artists_str = row["artists_str"]
            parts = [a.strip() for a in artists_str.split(",") if a.strip()]
            for a in parts:
                rows.append(
                    {
                        "release_id": row["release_id"],
                        "year_norm": row["year_norm"],
                        "title": row["title"],
                        "labels": row["labels"],
                        "label_catnos": row["label_catnos"],
                        "country": row["country"],
                        "genres": row["genres"],
                        "styles": row["styles"],
                        "formats": row["formats"],
                        "format_descriptions": row["format_descriptions"],
                        "released": row["released"],
                        "artist_name_raw": a,
                        "norm_name": a.strip().lower(),
                    }
                )

        if not rows:
            print("   -> nessun artista esploso, skip")
            continue

        df_exp = pd.DataFrame(rows)
        print(f"   -> {len(df_exp)} righe release-artista dopo explode")

        # join con mappa nomi unamb + one-shot
        df_join = df_exp.merge(df_map, how="inner", on="norm_name")
        if df_join.empty:
            print("   -> nessun match su mappa nomi, skip")
            continue

        # filtro Various
        df_join = df_join[
            ~df_join["artist_name_raw"].str.lower().str.startswith("various")
        ].copy()
        if df_join.empty:
            print("   -> solo Various, skip")
            continue

        # per release teniamo solo quelle con UN solo artist_id
        grp = df_join.groupby("release_id")["artist_id"].nunique().reset_index()
        grp = grp[grp["artist_id"] == 1].rename(columns={"artist_id": "n_artists"})
        df_one = df_join.merge(grp[["release_id"]], on="release_id", how="inner")

        if df_one.empty:
            print("   -> nessuna release single-entity in questo file, skip")
            continue

        print(f"   -> {len(df_one)} righe single-entity candidate")

        results.append(df_one)

    if not results:
        print("❌ Nessuna release trovata complessivamente.")
        return

    df_all = pd.concat(results, ignore_index=True)
    print(f"✅ Totale candidate PH+Electro single-entity: {len(df_all)} righe")

    # deduplico release_id + artist_id per sicurezza
    df_all = df_all.drop_duplicates(subset=["release_id", "artist_id"])

    # join con artist_title_counts_global_v1 (per sicurezza, anche se già filtrato)
    df_all = df_all.merge(
        df_counts[df_counts["n_distinct_titles"] == 1][["artist_id", "n_distinct_titles"]],
        on="artist_id",
        how="inner",
    )

    # join con artists_v1 per il nome canonico
    df_all = df_all.merge(df_art, on="artist_id", how="left")
    df_all = df_all.rename(columns={"name": "artist_name_canonical"})

    print(f"📦 Righello finale: {len(df_all)} righe")

    # salvo parquet + csv
    df_all.to_parquet(OUT_PARQUET, index=False)
    print(f"💾 Salvato Parquet in {OUT_PARQUET}")

    df_all.to_csv(OUT_CSV, index=False)
    print(f"💾 Salvato CSV in {OUT_CSV}")


if __name__ == "__main__":
    main()
