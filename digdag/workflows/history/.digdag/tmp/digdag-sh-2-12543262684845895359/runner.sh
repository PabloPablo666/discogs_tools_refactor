set -euo pipefail

TRINO_CONTAINER="trino"
TRINO_CATALOG="hive"
PROJECT_ROOT="/Users/paoloolivieri/discogs_tools_refactor"

python3 "$PROJECT_ROOT/scripts/update_run_registry.py" \
  --trino-container "$TRINO_CONTAINER" \
  --trino-catalog "$TRINO_CATALOG" \
  --action reconcile_register \
  --schema-version 1 \
  --include-active
