#!/usr/bin/env bash
set -euo pipefail

DATA_LAKE="${DISCOGS_DATA_LAKE:-/Users/paoloolivieri/discogs_data_lake/hive-data}"
SRC="${1:-/Users/paoloolivieri/discogs_store/raw/artists/discogs_20251101_artists.xml.gz}"

OUT_BASE="${DISCOGS_TEST_ROOT:-$DATA_LAKE/_tmp_test}"
OUT_ALIASES="$OUT_BASE/artist_aliases_v1"
OUT_MEMBERS="$OUT_BASE/artist_memberships_v1"

echo "DATA_LAKE: $DATA_LAKE"
echo "SRC:       $SRC"
echo "OUT:       $OUT_BASE"
echo

rm -rf "$OUT_ALIASES" "$OUT_MEMBERS"
mkdir -p "$OUT_ALIASES" "$OUT_MEMBERS"

python3 "$(dirname "$0")/../pipelines/extract_artist_relations_v1.py" \
  --src "$SRC" \
  --out-aliases "$OUT_ALIASES" \
  --out-members "$OUT_MEMBERS" \
  --clean

python3 - <<PY
import duckdb
base = r"$OUT_BASE"
con = duckdb.connect()
a = f"{base}/artist_aliases_v1/*.parquet"
m = f"{base}/artist_memberships_v1/*.parquet"
aliases = con.execute(f"select count(*) from read_parquet('{a}')").fetchone()[0]
members = con.execute(f"select count(*) from read_parquet('{m}')").fetchone()[0]
print("ALIASES rows:", aliases)
print("MEMBERS  rows:", members)
print()
print("ALIASES sample:")
print(con.execute(f"select * from read_parquet('{a}') limit 3").df())
print()
print("MEMBERS sample:")
print(con.execute(f"select * from read_parquet('{m}') limit 3").df())
PY

echo
echo "PASS ✅"
