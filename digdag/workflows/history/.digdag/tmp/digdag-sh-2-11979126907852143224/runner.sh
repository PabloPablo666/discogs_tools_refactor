set -euo pipefail

# Digdag params (templated by Digdag on purpose)
RUN_ID="2025-12__20260120_194923"
TRINO_CONTAINER="trino"
TRINO_CATALOG="hive"
PROJECT_ROOT="/Users/paoloolivieri/discogs_tools_refactor"

# Host env
BASE_LAKE="$(printenv DISCOGS_DATA_LAKE || true)"

echo "==============================================" >&2
echo " REGISTER RUN SCHEMA" >&2
echo " run_id : $RUN_ID" >&2
echo " lake   : $BASE_LAKE" >&2
echo " trino  : container=$TRINO_CONTAINER catalog=$TRINO_CATALOG" >&2
echo " root   : $PROJECT_ROOT" >&2
echo "==============================================" >&2

# Guardrails
if [ -z "$RUN_ID" ]; then
  echo "ERROR: run_id empty (pass -p run_id=...)" >&2
  exit 2
fi
if [ -z "$BASE_LAKE" ]; then
  echo "ERROR: DISCOGS_DATA_LAKE not set" >&2
  exit 2
fi
if [ -z "$TRINO_CONTAINER" ]; then
  echo "ERROR: trino_container empty (check _config.yml)" >&2
  exit 2
fi
if [ -z "$TRINO_CATALOG" ]; then
  echo "ERROR: trino_catalog empty (check _config.yml)" >&2
  exit 2
fi
if [ -z "$PROJECT_ROOT" ]; then
  echo "ERROR: project_root empty (check _config.yml)" >&2
  exit 2
fi

# -------------------------------------------------
# Accept NEW run_id format:
#   YYYY-MM__YYYYMMDD_HHMMSS
# Example:
#   2025-11__20260120_181200
# -------------------------------------------------
echo "$RUN_ID" | grep -Eq '^[0-9]{4}-[0-9]{2}__[0-9]{8}_[0-9]{6}$' || {
  echo "ERROR: invalid run_id format (expected YYYY-MM__YYYYMMDD_HHMMSS): $RUN_ID" >&2
  exit 2
}

# -------------------------------------------------
# Trino schema identifier must be SQL-safe.
# Replace '-' with '_' in the schema name only.
# RUN_ID stays unchanged for paths.
# -------------------------------------------------
RUN_SCHEMA_ID="$(echo "$RUN_ID" | tr '-' '_')"
RUN_SCHEMA="discogs_r_$RUN_SCHEMA_ID"

# Trino paths (inside container)
RUN_BASE="file:/data/hive-data/_runs/$RUN_ID"
META_LOC="file:/data/hive-data/_meta/discogs_history/$RUN_SCHEMA"

# Host paths (mac)
RUN_DIR_HOST="$BASE_LAKE/_runs/$RUN_ID"
MANIFEST_HOST="$RUN_DIR_HOST/run_manifest.json"
[ -f "$MANIFEST_HOST" ] || MANIFEST_HOST="$RUN_DIR_HOST/_meta/run_manifest.json"

echo " schema : hive.$RUN_SCHEMA" >&2
echo " run    : $RUN_BASE" >&2
echo " meta   : $META_LOC" >&2
echo " host   : $RUN_DIR_HOST" >&2

if [ ! -d "$RUN_DIR_HOST" ]; then
  echo "ERROR: run not found on host: $RUN_DIR_HOST" >&2
  exit 2
fi

# Optional manifest metadata (non-blocking)
if [ -f "$MANIFEST_HOST" ] && [ -f "$PROJECT_ROOT/scripts/manifest_to_env.py" ]; then
  echo " manifest: $MANIFEST_HOST" >&2
  MANIFEST_ENV="$(MANIFEST_HOST="$MANIFEST_HOST" python3 "$PROJECT_ROOT/scripts/manifest_to_env.py" 2>/dev/null || true)"
  if [ -n "$MANIFEST_ENV" ]; then
    echo "$MANIFEST_ENV" | grep -qvE '^[A-Z0-9_]+=.*$' && {
      echo "WARN: manifest_to_env produced unexpected output; skipping eval" >&2
    } || {
      eval "$MANIFEST_ENV"
      export DUMP_MONTH DUMP_DATE RUN_MODE GIT_SHA
      echo " dump_month: $DUMP_MONTH" >&2 || true
      echo " dump_date:  $DUMP_DATE" >&2 || true
      echo " run_mode:   $RUN_MODE" >&2 || true
      echo " git_sha:    $GIT_SHA" >&2 || true
    }
  fi
fi

# Ensure run exists inside container
docker exec -i "$TRINO_CONTAINER" sh -lc "test -d /data/hive-data/_runs/$RUN_ID" || {
  echo "ERROR: run not found in container: /data/hive-data/_runs/$RUN_ID" >&2
  exit 2
}

has_dir() {
  docker exec -i "$TRINO_CONTAINER" sh -lc "test -d \"$1\""
}

# Create schema (safe identifier already sanitized)
docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
  CREATE SCHEMA IF NOT EXISTS hive.$RUN_SCHEMA
  WITH (location='$META_LOC');
" >/dev/null

# Required datasets (HARD FAIL if missing)
req_list="/data/hive-data/_runs/$RUN_ID/artists_v1_typed|/data/hive-data/_runs/$RUN_ID/artist_aliases_v1_typed|/data/hive-data/_runs/$RUN_ID/artist_memberships_v1_typed|/data/hive-data/_runs/$RUN_ID/masters_v1_typed|/data/hive-data/_runs/$RUN_ID/releases_v6|/data/hive-data/_runs/$RUN_ID/labels_v10"

oldIFS="$IFS"
IFS="|"
for d in $req_list; do
  [ -z "$d" ] && continue
  if ! has_dir "$d"; then
    echo "ERROR: missing required dataset dir in run: $d" >&2
    IFS="$oldIFS"
    exit 2
  fi
done
IFS="$oldIFS"

# Core tables + views
docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
  CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.artists_v1_typed (
    artist_id      BIGINT,
    name           VARCHAR,
    realname       VARCHAR,
    profile        VARCHAR,
    data_quality   VARCHAR,
    urls           VARCHAR,
    namevariations VARCHAR,
    aliases        VARCHAR
  )
  WITH (external_location='$RUN_BASE/artists_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.artist_aliases_v1_typed (
    artist_id  BIGINT,
    alias_id   BIGINT,
    alias_name VARCHAR
  )
  WITH (external_location='$RUN_BASE/artist_aliases_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.artist_memberships_v1_typed (
    group_id    BIGINT,
    group_name  VARCHAR,
    member_id   BIGINT,
    member_name VARCHAR
  )
  WITH (external_location='$RUN_BASE/artist_memberships_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.masters_v1_typed (
    master_id         BIGINT,
    main_release_id   BIGINT,
    title             VARCHAR,
    year              BIGINT,
    master_artists    VARCHAR,
    master_artist_ids VARCHAR,
    genres            VARCHAR,
    styles            VARCHAR,
    data_quality      VARCHAR
  )
  WITH (external_location='$RUN_BASE/masters_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.releases_ref_v6 (
    release_id           BIGINT,
    master_id            BIGINT,
    title                VARCHAR,
    artists              VARCHAR,
    labels               VARCHAR,
    label_catnos         VARCHAR,
    country              VARCHAR,
    formats              VARCHAR,
    genres               VARCHAR,
    styles               VARCHAR,
    credits_flat         VARCHAR,
    status               VARCHAR,
    released             VARCHAR,
    data_quality         VARCHAR,
    format_qtys          VARCHAR,
    format_texts         VARCHAR,
    format_descriptions  VARCHAR,
    identifiers_flat     VARCHAR
  )
  WITH (external_location='$RUN_BASE/releases_v6', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.labels_ref_v10 (
    label_id           BIGINT,
    name               VARCHAR,
    profile            VARCHAR,
    contact_info       VARCHAR,
    data_quality       VARCHAR,
    parent_label_id    BIGINT,
    parent_label_name  VARCHAR,
    urls_csv           VARCHAR,
    sublabel_ids_csv   VARCHAR,
    sublabel_names_csv VARCHAR
  )
  WITH (external_location='$RUN_BASE/labels_v10', format='PARQUET');

  CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.artists_v1 AS
  SELECT * FROM hive.$RUN_SCHEMA.artists_v1_typed;

  CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.artist_aliases_v1 AS
  SELECT * FROM hive.$RUN_SCHEMA.artist_aliases_v1_typed;

  CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.artist_memberships_v1 AS
  SELECT * FROM hive.$RUN_SCHEMA.artist_memberships_v1_typed;

  CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.masters_v1 AS
  SELECT * FROM hive.$RUN_SCHEMA.masters_v1_typed;

  CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.artist_memberships_v1_typed_dedup AS
  SELECT DISTINCT group_id, group_name, member_id, member_name
  FROM hive.$RUN_SCHEMA.artist_memberships_v1_typed;
" >/dev/null

# Optional warehouse tables (register only if dirs exist)
if has_dir "/data/hive-data/_runs/$RUN_ID/warehouse_discogs/artist_name_map_v1"; then
  docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
    CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.artist_name_map_v1 (
      norm_name VARCHAR,
      artist_id BIGINT
    )
    WITH (external_location='$RUN_BASE/warehouse_discogs/artist_name_map_v1', format='PARQUET');
  " >/dev/null
  echo "[OK] warehouse: artist_name_map_v1" >&2
else
  echo "[WARN] warehouse missing: artist_name_map_v1" >&2
fi

if has_dir "/data/hive-data/_runs/$RUN_ID/warehouse_discogs/release_artists_v1"; then
  docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
    CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.release_artists_v1 (
      release_id  BIGINT,
      artist_norm VARCHAR
    )
    WITH (external_location='$RUN_BASE/warehouse_discogs/release_artists_v1', format='PARQUET');
  " >/dev/null
  echo "[OK] warehouse: release_artists_v1" >&2
else
  echo "[WARN] warehouse missing: release_artists_v1" >&2
fi

if has_dir "/data/hive-data/_runs/$RUN_ID/warehouse_discogs/release_label_xref_v1"; then
  docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
    CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.release_label_xref_v1 (
      release_id BIGINT,
      label_name VARCHAR,
      label_norm VARCHAR
    )
    WITH (external_location='$RUN_BASE/warehouse_discogs/release_label_xref_v1', format='PARQUET');

    CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.release_label_xref_v1_canon AS
    SELECT release_id, label_name, label_norm
    FROM hive.$RUN_SCHEMA.release_label_xref_v1;

    CREATE OR REPLACE VIEW hive.$RUN_SCHEMA.release_label_xref_v1_dedup AS
    SELECT DISTINCT release_id, label_name, label_norm
    FROM hive.$RUN_SCHEMA.release_label_xref_v1;
  " >/dev/null
  echo "[OK] warehouse: release_label_xref_v1 (+ views)" >&2
else
  echo "[WARN] warehouse missing: release_label_xref_v1" >&2
fi

if has_dir "/data/hive-data/_runs/$RUN_ID/warehouse_discogs/label_release_counts_v1"; then
  docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
    CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.label_release_counts_v1 (
      label_norm        VARCHAR,
      label_name_sample VARCHAR,
      n_total_releases  BIGINT
    )
    WITH (external_location='$RUN_BASE/warehouse_discogs/label_release_counts_v1', format='PARQUET');
  " >/dev/null
  echo "[OK] warehouse: label_release_counts_v1" >&2
else
  echo "[WARN] warehouse missing: label_release_counts_v1" >&2
fi

if has_dir "/data/hive-data/_runs/$RUN_ID/warehouse_discogs/release_style_xref_v1"; then
  docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
    CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.release_style_xref_v1 (
      release_id BIGINT,
      style      VARCHAR,
      style_norm VARCHAR
    )
    WITH (external_location='$RUN_BASE/warehouse_discogs/release_style_xref_v1', format='PARQUET');
  " >/dev/null
  echo "[OK] warehouse: release_style_xref_v1" >&2
else
  echo "[WARN] warehouse missing: release_style_xref_v1" >&2
fi

if has_dir "/data/hive-data/_runs/$RUN_ID/warehouse_discogs/release_genre_xref_v1"; then
  docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
    CREATE TABLE IF NOT EXISTS hive.$RUN_SCHEMA.release_genre_xref_v1 (
      release_id BIGINT,
      genre      VARCHAR,
      genre_norm VARCHAR
    )
    WITH (external_location='$RUN_BASE/warehouse_discogs/release_genre_xref_v1', format='PARQUET');
  " >/dev/null
  echo "[OK] warehouse: release_genre_xref_v1" >&2
else
  echo "[WARN] warehouse missing: release_genre_xref_v1" >&2
fi

echo "OK: registered hive.$RUN_SCHEMA" >&2
