set -euo pipefail

RUN_IDS_RAW="20260118_004418 20260118_202946 20260118_214034"
if [ -z "$RUN_IDS_RAW" ]; then
  echo "ERROR: run_ids not set (pass -p run_ids=...)" >&2
  exit 2
fi

# Build a clean list file
: > .run_ids.txt
for rid in $(echo "$RUN_IDS_RAW" | tr ',' ' '); do
  [ -z "$rid" ] && continue
  echo "$rid" >> .run_ids.txt
done

echo "[REGISTER_MANY_RUNS] will register:" >&2
cat .run_ids.txt >&2

while IFS= read -r rid; do
  [ -z "$rid" ] && continue
  echo "== register schema for $rid ==" >&2
  digdag run --session "2026-01-20T17:10:00+00:00" --project . register_run_schema -p run_id="$rid"
done < .run_ids.txt
