set -euo pipefail

LAKE="$(printenv DISCOGS_DATA_LAKE || true)"
if [ -z "$LAKE" ]; then
  echo "ERROR: DISCOGS_DATA_LAKE not set" >&2
  exit 2
fi

RUN_IDS_RAW="20260118_004418 20260118_202946 20260118_214034"
if [ -z "$RUN_IDS_RAW" ]; then
  echo "ERROR: run_ids not set (pass -p run_ids=...)" >&2
  exit 2
fi

echo "[REGISTER_MANY_RUNS PREFLIGHT]" >&2
echo " lake: $LAKE" >&2
echo " run_ids_raw: $RUN_IDS_RAW" >&2

bad=""
count=0
for rid in $(echo "$RUN_IDS_RAW" | tr ',' ' '); do
  [ -z "$rid" ] && continue
  count=$((count+1))
  if ! echo "$rid" | grep -Eq '^[0-9]{8}_[0-9]{6}(__([0-9]{8}|[0-9]{4}_[0-9]{2}))?$'; then
    bad="$bad $rid"
  fi
done

if [ "$count" -eq 0 ]; then
  echo "ERROR: empty run_ids after parsing" >&2
  exit 2
fi

if [ -n "$bad" ]; then
  echo "ERROR: invalid run_id(s):$bad" >&2
  exit 2
fi
