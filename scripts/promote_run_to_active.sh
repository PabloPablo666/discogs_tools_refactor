#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:-}"
if [[ -z "$RUN_ID" ]]; then
  echo "Usage: $0 RUN_ID" >&2
  exit 2
fi

LAKE="${DISCOGS_DATA_LAKE:-}"
if [[ -z "$LAKE" ]]; then
  echo "ERROR: DISCOGS_DATA_LAKE is not set (must point to base lake, e.g. /data/hive-data)" >&2
  exit 2
fi

# ---------------------------------------------------------
# SAFETY GUARD: must be BASE lake, not a run dir
# ---------------------------------------------------------
if [[ "$LAKE" == */_runs/* ]]; then
  echo "ERROR: DISCOGS_DATA_LAKE points inside a run directory, not the base lake:" >&2
  echo "  $LAKE" >&2
  echo "Refusing to promote. Set DISCOGS_DATA_LAKE to the base lake (e.g. /data/hive-data)." >&2
  exit 2
fi

RUN_DIR="$LAKE/_runs/$RUN_ID"
if [[ ! -d "$RUN_DIR" ]]; then
  echo "ERROR: run dir not found: $RUN_DIR" >&2
  exit 2
fi

ts="$(date +%Y%m%d_%H%M%S)"

# ---------------------------------------------------------
# REQUIRED datasets: if any missing, ABORT (no partial promote)
# ---------------------------------------------------------
required=(
  artists_v1_typed
  masters_v1_typed
  releases_v6
  labels_v10
  warehouse_discogs
)

for ds in "${required[@]}"; do
  if [[ ! -d "$RUN_DIR/$ds" ]]; then
    echo "ERROR: missing required dataset in run: $ds (path: $RUN_DIR/$ds)" >&2
    exit 2
  fi
done

ACTIVE="$LAKE/active"
PREV="$LAKE/active__prev_${ts}"

echo "==============================================" >&2
echo " PROMOTE RUN -> ACTIVE (pointer mode)" >&2
echo " lake:   $LAKE" >&2
echo " run :   $RUN_ID" >&2
echo " runDir: $RUN_DIR" >&2
echo " active: $ACTIVE" >&2
echo "==============================================" >&2

# If active exists (dir or symlink), move it aside for rollback
if [[ -e "$ACTIVE" || -L "$ACTIVE" ]]; then
  echo "[BACKUP] $ACTIVE -> $PREV" >&2
  mv "$ACTIVE" "$PREV"
fi

# ---------------------------------------------------------
# Create RELATIVE symlink (Docker-safe)
# ---------------------------------------------------------
REL_TARGET="_runs/$RUN_ID"
echo "[LINK] $ACTIVE -> $REL_TARGET" >&2
ln -s "$REL_TARGET" "$ACTIVE"

# Basic smoke check
if [[ ! -L "$ACTIVE" ]]; then
  echo "ERROR: active is not a symlink after promote: $ACTIVE" >&2
  echo "NOTE: if $ACTIVE existed as a directory and couldn't be moved, fix permissions and retry." >&2
  exit 2
fi

active_target="$(readlink "$ACTIVE")"
if [[ "$active_target" != "$REL_TARGET" ]]; then
  echo "ERROR: active points somewhere else: $active_target" >&2
  exit 2
fi

# Sanity: target exists from the lake perspective
if [[ ! -d "$LAKE/$REL_TARGET" ]]; then
  echo "ERROR: symlink target missing: $LAKE/$REL_TARGET" >&2
  exit 2
fi

# Optional: if docker+trino container exists, ensure container can resolve active
if command -v docker >/dev/null 2>&1; then
  if docker ps --format '{{.Names}}' | grep -qx 'trino'; then
    docker exec -i trino sh -lc 'test -d /data/hive-data/active/artists_v1_typed' \
      || { echo "ERROR: trino container can't see /data/hive-data/active/artists_v1_typed" >&2; exit 2; }
  fi
fi

echo "✅ Promote completed (pointer mode)." >&2
echo "   Rollback: mv \"$PREV\" \"$ACTIVE\"" >&2
