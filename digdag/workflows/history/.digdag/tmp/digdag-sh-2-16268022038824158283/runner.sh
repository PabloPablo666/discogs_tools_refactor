set -euo pipefail

# NOTE: do not write the sequence "$" + "{" in dig files. Digdag templates it.
DISCOGS_DATA_LAKE="$(printenv DISCOGS_DATA_LAKE || true)"
if [ -z "$DISCOGS_DATA_LAKE" ]; then
  echo "ERROR: DISCOGS_DATA_LAKE not set" >&2
  exit 2
fi

RUN_IDS_JSON="["20260118_202946","20260118_214034","20260118_004418"]"
if [ -z "$RUN_IDS_JSON" ]; then
  echo "ERROR: run_ids param missing" >&2
  exit 2
fi

echo "[HISTORY PREFLIGHT]" >&2
echo " lake:    $DISCOGS_DATA_LAKE" >&2
echo " run_ids: $RUN_IDS_JSON" >&2

RUN_IDS_JSON="$RUN_IDS_JSON" python3 "/Users/paoloolivieri/discogs_tools_refactor/scripts/validate_run_ids.py"
