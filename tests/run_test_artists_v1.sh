#!/usr/bin/env bash
set -euo pipefail

DATA_LAKE="${DISCOGS_DATA_LAKE:-/Users/paoloolivieri/discogs_data_lake/hive-data}"
SRC="${1:-/Users/paoloolivieri/discogs_store/raw/artists/discogs_20251101_artists.xml.gz}"

OUT_BASE="${DISCOGS_TEST_ROOT:-$DATA_LAKE/_tmp_test}"
OUT="$OUT_BASE/artists_v1"

echo "DATA_LAKE: $DATA_LAKE"
echo "SRC:       $SRC"
echo "OUT:       $OUT"
echo

# guard: require pandas
python - <<'PY'
import pandas
print("✅ pandas OK")
PY

rm -rf "$OUT"
mkdir -p "$OUT"

python3 "$(dirname "$0")/../pipelines/extract_artists_v1.py" \
  --src "$SRC" \
  --out "$OUT" \
  --clean \
  --batch 50000

python3 - <<PY
import duckdb
con = duckdb.connect()
p = r"$OUT/*.parquet"
n = con.execute(f"select count(*) from read_parquet('{p}')").fetchone()[0]
print("ARTISTS rows:", n)
print()
print("ARTISTS sample:")
print(con.execute(f"select artist_id, name, realname from read_parquet('{p}') where name is not null limit 5").df())
PY

echo
echo "PASS ✅"
