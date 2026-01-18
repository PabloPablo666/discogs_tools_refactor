#!/usr/bin/env bash
set -euo pipefail

MONTH="${1:-}"
RAW_ROOT="${2:-}"

if [[ -z "$MONTH" || -z "$RAW_ROOT" ]]; then
  echo "Usage: $0 YYYY-MM /absolute/path/to/raw_root" >&2
  exit 2
fi

BASE_URL="https://discogs-data-dumps.s3.us-west-2.amazonaws.com"
DUMP_DATE="$(python3 "$(dirname "$0")/find_discogs_dump_date.py" --month "$MONTH")"
YEAR="${DUMP_DATE:0:4}"

OUT_DIR="$RAW_ROOT/$MONTH"
mkdir -p "$OUT_DIR"

{
  echo ""
  echo "=============================================="
  echo " DISCOGS DUMP DOWNLOAD"
  echo "----------------------------------------------"
  echo " Month: $MONTH"
  echo " Date : $DUMP_DATE"
  echo " Out  : $OUT_DIR"
  echo "=============================================="
} >&2

types=(artists labels masters releases)

for t in "${types[@]}"; do
  fname="discogs_${DUMP_DATE}_${t}.xml.gz"
  url="${BASE_URL}/data/${YEAR}/${fname}"
  dest="$OUT_DIR/$fname"

  if [[ -f "$dest" ]]; then
    echo "[SKIP] exists: $dest" >&2
    continue
  fi

  echo "[GET ] $url" >&2

  tmp="${dest}.part"
  rm -f "$tmp"

  if curl -fL \
    --retry 10 \
    --retry-delay 5 \
    --retry-connrefused \
    -o "$tmp" \
    "$url"
  then
    mv -f "$tmp" "$dest"
    echo "[OK  ] $dest" >&2
  else
    rm -f "$tmp"
    echo "[FAIL] $url" >&2
    exit 2
  fi
done

echo "==============================================" >&2
echo " DONE" >&2
echo "==============================================" >&2
