set -euo pipefail

echo "[DEBUG] digdag run_id = 20260118_202946" >&2
echo "[DEBUG] digdag trino_container = trino" >&2
echo "[DEBUG] digdag trino_catalog   = hive" >&2
echo "[DEBUG] digdag project_root    = /Users/paoloolivieri/discogs_tools_refactor" >&2

LAKE="$(printenv DISCOGS_DATA_LAKE || true)"
echo "[DEBUG] env DISCOGS_DATA_LAKE = $LAKE" >&2

if [ -z "20260118_202946" ]; then
  echo "ERROR: digdag param run_id is empty (you are not passing -p run_id=... or wrong workflow)" >&2
  exit 2
fi

echo "[OK] params visible to Digdag" >&2
