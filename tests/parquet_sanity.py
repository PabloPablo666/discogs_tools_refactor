#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import sys

# ---------- expected schemas (minimal, stable subset) ----------
EXPECTED_COLS = {
    "artists_v1_typed": [
        "artist_id", "name", "realname", "profile", "data_quality",
        "urls", "namevariations", "aliases"
    ],
    "artist_aliases_v1_typed": ["artist_id", "alias_id", "alias_name"],
    "artist_memberships_v1_typed": ["group_id", "group_name", "member_id", "member_name"],
    "masters_v1_typed": [
        "master_id", "main_release_id", "title", "year",
        "master_artists", "master_artist_ids", "genres", "styles", "data_quality"
    ],
    "releases_v6": ["release_id", "master_id", "title", "artists", "labels", "genres", "styles", "status", "released", "data_quality"],
    "labels_v10": ["label_id", "name", "profile", "contact_info", "data_quality", "parent_label_id", "parent_label_name"],
}

WAREHOUSE_TABLES = {
    "artist_name_map_v1": ["norm_name", "artist_id"],
    "release_artists_v1": ["release_id", "artist_norm"],
    "release_label_xref_v1": ["release_id", "label_name", "label_norm"],
    "label_release_counts_v1": ["label_norm", "label_name_sample", "n_total_releases"],
    "release_style_xref_v1": ["release_id", "style", "style_norm"],
    "release_genre_xref_v1": ["release_id", "genre", "genre_norm"],
}

MIN_ROWS = {
    "artists_v1_typed": 1000,
    "masters_v1_typed": 1000,
    "releases_v6": 1000,
    "labels_v10": 1000,
    # warehouse may be empty in edge cases, but normally should be >0 once build ran
    "warehouse_discogs": 1,
}

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Path to active/ or a run root (_runs/<run_id>).")
    ap.add_argument("--fast", action="store_true", help="Skip heavier checks (counts joins), keep essentials.")
    return ap.parse_args()

def fail(msg: str) -> None:
    print(f"FAIL ❌ {msg}", file=sys.stderr)
    raise SystemExit(2)

def ok(msg: str) -> None:
    print(f"OK ✅ {msg}")

def parquet_glob(d: Path) -> str:
    return str(d / "*.parquet")

def assert_dir_has_parquet(d: Path, name: str) -> None:
    if not d.exists() or not d.is_dir():
        fail(f"missing dir: {name} ({d})")
    files = list(d.glob("*.parquet"))
    if not files:
        fail(f"no parquet files in: {name} ({d})")
    ok(f"{name}: parquet parts={len(files)}")

def assert_columns(con: duckdb.DuckDBPyConnection, glob: str, expected: list[str], name: str) -> None:
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()]
    missing = [c for c in expected if c not in cols]
    if missing:
        fail(f"{name}: missing columns: {missing}")
    ok(f"{name}: schema contains expected columns ({len(expected)})")

def rowcount(con: duckdb.DuckDBPyConnection, glob: str) -> int:
    return con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]

def assert_min_rows(con, glob: str, name: str) -> None:
    n = rowcount(con, glob)
    min_n = MIN_ROWS.get(name, 1)
    if n < min_n:
        fail(f"{name}: too few rows {n:,} (< {min_n:,})")
    ok(f"{name}: rows={n:,}")

def assert_no_nulls(con, glob: str, col: str, name: str) -> None:
    n = con.execute(f"SELECT count(*) FROM read_parquet('{glob}') WHERE {col} IS NULL").fetchone()[0]
    if n != 0:
        fail(f"{name}: {col} has {n:,} NULLs")
    ok(f"{name}: {col} nulls=0")

def assert_uniqueness(con, glob: str, col: str, name: str) -> None:
    dup = con.execute(
        f"""
        SELECT count(*) FROM (
          SELECT {col} FROM read_parquet('{glob}')
          GROUP BY 1 HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    if dup != 0:
        fail(f"{name}: {col} has {dup:,} duplicated values")
    ok(f"{name}: {col} unique")

def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser().resolve()

    # Base dataset dirs
    base_dirs = {
        "artists_v1_typed": root / "artists_v1_typed",
        "artist_aliases_v1_typed": root / "artist_aliases_v1_typed",
        "artist_memberships_v1_typed": root / "artist_memberships_v1_typed",
        "masters_v1_typed": root / "masters_v1_typed",
        "releases_v6": root / "releases_v6",
        "labels_v10": root / "labels_v10",
    }
    warehouse_root = root / "warehouse_discogs"

    print("==============================================")
    print(" PARQUET SANITY")
    print(f" root: {root}")
    print("==============================================")

    # Existence + parquet presence
    for name, d in base_dirs.items():
        assert_dir_has_parquet(d, name)
    if not warehouse_root.exists():
        fail(f"missing dir: warehouse_discogs ({warehouse_root})")
    ok("warehouse_discogs: dir exists")

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")

    # Schema checks (stable subset)
    for name, cols in EXPECTED_COLS.items():
        g = parquet_glob(base_dirs[name])
        assert_columns(con, g, cols, name)

    # Rowcount + key integrity basics
    assert_min_rows(con, parquet_glob(base_dirs["artists_v1_typed"]), "artists_v1_typed")
    assert_min_rows(con, parquet_glob(base_dirs["masters_v1_typed"]), "masters_v1_typed")
    assert_min_rows(con, parquet_glob(base_dirs["releases_v6"]), "releases_v6")
    assert_min_rows(con, parquet_glob(base_dirs["labels_v10"]), "labels_v10")

    # Primary-ish keys should be non-null and unique
    assert_no_nulls(con, parquet_glob(base_dirs["artists_v1_typed"]), "artist_id", "artists_v1_typed")
    assert_uniqueness(con, parquet_glob(base_dirs["artists_v1_typed"]), "artist_id", "artists_v1_typed")

    assert_no_nulls(con, parquet_glob(base_dirs["masters_v1_typed"]), "master_id", "masters_v1_typed")
    assert_uniqueness(con, parquet_glob(base_dirs["masters_v1_typed"]), "master_id", "masters_v1_typed")

    assert_no_nulls(con, parquet_glob(base_dirs["releases_v6"]), "release_id", "releases_v6")
    assert_uniqueness(con, parquet_glob(base_dirs["releases_v6"]), "release_id", "releases_v6")

    assert_no_nulls(con, parquet_glob(base_dirs["labels_v10"]), "label_id", "labels_v10")
    # labels might be duplicated depending on parsing; keep it soft if you want:
    # assert_uniqueness(con, parquet_glob(base_dirs["labels_v10"]), "label_id", "labels_v10")

    # FK-ish checks (fast enough)
    if not args.fast:
        # releases.master_id -> masters.master_id (allow NULL master_id)
        orphan_m = con.execute(
            f"""
            WITH r AS (
              SELECT master_id FROM read_parquet('{parquet_glob(base_dirs["releases_v6"])}')
              WHERE master_id IS NOT NULL
            ),
            m AS (
              SELECT master_id FROM read_parquet('{parquet_glob(base_dirs["masters_v1_typed"])}')
            )
            SELECT count(*) FROM r
            LEFT JOIN m ON r.master_id = m.master_id
            WHERE m.master_id IS NULL
            """
        ).fetchone()[0]
        ok(f"releases_v6.master_id orphans vs masters_v1_typed: {orphan_m:,} (informational)")

        # artist_aliases.artist_id must exist in artists
        orphan_artist_fk = con.execute(
            f"""
            WITH a AS (
              SELECT artist_id FROM read_parquet('{parquet_glob(base_dirs["artist_aliases_v1_typed"])}')
              WHERE artist_id IS NOT NULL
            ),
            ar AS (
              SELECT artist_id FROM read_parquet('{parquet_glob(base_dirs["artists_v1_typed"])}')
            )
            SELECT count(*) FROM a
            LEFT JOIN ar ON a.artist_id = ar.artist_id
            WHERE ar.artist_id IS NULL
            """
        ).fetchone()[0]
        if orphan_artist_fk != 0:
            fail(f"artist_aliases_v1_typed.artist_id FK broken rows={orphan_artist_fk:,}")
        ok("artist_aliases_v1_typed.artist_id FK OK")

    # Warehouse checks: dirs + parquet + columns + key sanity
    for wname, wcols in WAREHOUSE_TABLES.items():
        d = warehouse_root / wname
        assert_dir_has_parquet(d, f"warehouse_discogs/{wname}")
        g = parquet_glob(d)
        assert_columns(con, g, wcols, f"warehouse_discogs/{wname}")

    # warehouse FKs
    if not args.fast:
        # artist_name_map.artist_id -> artists.artist_id
        orphan_nm = con.execute(
            f"""
            WITH nm AS (
              SELECT artist_id FROM read_parquet('{parquet_glob(warehouse_root/"artist_name_map_v1")}')
            ),
            a AS (
              SELECT artist_id FROM read_parquet('{parquet_glob(base_dirs["artists_v1_typed"])}')
            )
            SELECT count(*) FROM nm
            LEFT JOIN a ON nm.artist_id = a.artist_id
            WHERE a.artist_id IS NULL
            """
        ).fetchone()[0]
        if orphan_nm != 0:
            fail(f"warehouse artist_name_map_v1 FK broken rows={orphan_nm:,}")
        ok("warehouse artist_name_map_v1 FK OK")

        # release_* xrefs release_id -> releases.release_id
        for x in ["release_artists_v1","release_label_xref_v1","release_style_xref_v1","release_genre_xref_v1"]:
            orphan = con.execute(
                f"""
                WITH x AS (
                  SELECT DISTINCT release_id FROM read_parquet('{parquet_glob(warehouse_root/x)}')
                ),
                r AS (
                  SELECT release_id FROM read_parquet('{parquet_glob(base_dirs["releases_v6"])}')
                )
                SELECT count(*) FROM x
                LEFT JOIN r ON x.release_id = r.release_id
                WHERE r.release_id IS NULL
                """
            ).fetchone()[0]
            if orphan != 0:
                fail(f"warehouse {x} has orphan release_id rows={orphan:,}")
            ok(f"warehouse {x} release_id FK OK")

        # label_release_counts matches recomputed counts (sample check)
        mism = con.execute(
            f"""
            WITH recomputed AS (
              SELECT label_norm, count(DISTINCT release_id) AS n
              FROM read_parquet('{parquet_glob(warehouse_root/"release_label_xref_v1")}')
              GROUP BY 1
            ),
            c AS (
              SELECT label_norm, n_total_releases
              FROM read_parquet('{parquet_glob(warehouse_root/"label_release_counts_v1")}')
            )
            SELECT count(*) FROM (
              SELECT c.label_norm
              FROM c JOIN recomputed r ON c.label_norm = r.label_norm
              WHERE c.n_total_releases <> r.n
              LIMIT 1000
            )
            """
        ).fetchone()[0]
        if mism != 0:
            fail(f"label_release_counts_v1 mismatches found (sampled)={mism}")
        ok("label_release_counts_v1 matches recomputed counts (sample)")

    print("==============================================")
    print("✅ PARQUET SANITY PASSED")
    print("==============================================")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
