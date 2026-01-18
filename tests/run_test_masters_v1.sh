#!/usr/bin/env bash
set -euo pipefail

DATA_LAKE="${DISCOGS_DATA_LAKE:-/Users/paoloolivieri/discogs_data_lake/hive-data}"
SRC="${1:-/Users/paoloolivieri/discogs_store/raw/masters/discogs_20251101_masters.xml.gz}"

OUT_BASE="${DISCOGS_TEST_ROOT:-$DATA_LAKE/_tmp_test}"
OUT="$OUT_BASE/masters_v1"

echo "DATA_LAKE: $DATA_LAKE"
echo "SRC:       $SRC"
echo "OUT:       $OUT"
echo

# guard: require pandas + duckdb
python - <<'PY'
import pandas, duckdb
print("✅ pandas + duckdb OK")
PY

rm -rf "$OUT"
mkdir -p "$OUT"

python3 "$(dirname "$0")/../pipelines/extract_masters_v1.py" \
  --src "$SRC" \
  --out "$OUT" \
  --clean \
  --batch 50000

python3 - <<PY
import duckdb
con = duckdb.connect()
p = r"$OUT/*.parquet"

n = con.execute(f"select count(*) from read_parquet('{p}')").fetchone()[0]
print("MASTERS rows:", n)
print()

print("MASTERS sample:")
print(con.execute(f"""
  select master_id, main_release_id, title, year, master_artists
  from read_parquet('{p}')
  where title is not null
  limit 5
""").df())
PY

echo
echo "PASS ✅"
