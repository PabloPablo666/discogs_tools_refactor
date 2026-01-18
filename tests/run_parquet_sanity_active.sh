#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   run_parquet_sanity_active.sh /path/to/hive-data/active
#   run_parquet_sanity_active.sh /path/to/hive-data/_runs/<run_id>
#
ROOT="${1:-}"
if [[ -z "$ROOT" ]]; then
  echo "Usage: $0 <ACTIVE_OR_RUN_ROOT_PATH>" >&2
  exit 2
fi
if [[ ! -d "$ROOT" ]]; then
  echo "ERROR: path not found: $ROOT" >&2
  exit 2
fi

python3 "$(dirname "$0")/parquet_sanity.py" --root "$ROOT"
