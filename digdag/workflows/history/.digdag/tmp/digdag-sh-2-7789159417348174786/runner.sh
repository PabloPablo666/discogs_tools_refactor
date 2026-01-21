set -euo pipefail

LAKE="$(printenv DISCOGS_DATA_LAKE || true)"
if [ -z "$LAKE" ]; then
  echo "ERROR: DISCOGS_DATA_LAKE not set" >&2
  exit 2
fi

RUN_IDS_RAW="$(printenv RUN_IDS_RAW || true)"
if [ -z "$RUN_IDS_RAW" ]; then
  echo "ERROR: run_ids not set (pass -p run_ids=...)" >&2
  exit 2
fi

echo "[REGISTER_MANY_RUNS PREFLIGHT]" >&2
echo " lake: $LAKE" >&2
echo " run_ids_raw: $RUN_IDS_RAW" >&2

python3 -c 'import os,re,sys; raw=os.environ.get("RUN_IDS_RAW","").strip(); parts=[p.strip().strip("\"").strip("'\'"'\'") for p in re.split(r"[\s,]+", raw) if p.strip()]; pat=re.compile(r"^\d{8}_\d{6}(__(\d{8}|\d{4}_\d{2}))?$"); bad=[p for p in parts if not pat.match(p)]; (print("ERROR: empty run_ids after parsing", file=sys.stderr) or sys.exit(2)) if not parts else ((print("ERROR: invalid run_id(s): %s"%bad, file=sys.stderr) or sys.exit(2)) if bad else (print("OK: %d run_id(s)"%len(parts), file=sys.stderr) or sys.exit(0)))'
