set -euo pipefail

TRINO_CONTAINER="trino"
TRINO_CATALOG="hive"
PROJECT_ROOT="/Users/paoloolivieri/discogs_tools_refactor"

if [ -z "$(printenv DISCOGS_DATA_LAKE || true)" ]; then
  echo "ERROR: DISCOGS_DATA_LAKE not set" >&2
  exit 2
fi

python3 "$PROJECT_ROOT/scripts/export_history_csv.py" \
  --trino-container "$TRINO_CONTAINER" \
  --trino-catalog "$TRINO_CATALOG"
