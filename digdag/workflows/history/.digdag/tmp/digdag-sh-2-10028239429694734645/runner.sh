set -euo pipefail

TRINO_CONTAINER="trino"
TRINO_CATALOG="hive"
PROJECT_ROOT="/Users/paoloolivieri/discogs_tools_refactor"

# Digdag orchestration only: call Python
python3 "$PROJECT_ROOT/scripts/reconcile_register.py" \
  --trino-container "$TRINO_CONTAINER" \
  --trino-catalog "$TRINO_CATALOG"
