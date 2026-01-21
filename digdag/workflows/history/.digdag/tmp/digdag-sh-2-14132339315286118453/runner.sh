set -euo pipefail

BASE_LAKE="$(printenv DISCOGS_DATA_LAKE || true)"
if [ -z "$BASE_LAKE" ]; then
  echo "ERROR: DISCOGS_DATA_LAKE is not set" >&2
  exit 2
fi

if [ -z "$RUN_ID" ]; then
  echo "ERROR: run_id empty" >&2
  exit 2
fi

if ! echo "$RUN_ID" | grep -Eq '^[0-9]{8}_[0-9]{6}(__([0-9]{8}|[0-9]{4}_[0-9]{2}))?$'; then
  echo "ERROR: invalid run_id format: $RUN_ID" >&2
  exit 2
fi

RUN_SCHEMA="discogs_r_$RUN_ID"

RUN_BASE="file:/data/hive-data/_runs/$RUN_ID"
META_LOC="file:/data/hive-data/_meta/discogs_history/$RUN_SCHEMA"

RUN_DIR_HOST="$BASE_LAKE/_runs/$RUN_ID"
MANIFEST_HOST="$RUN_DIR_HOST/run_manifest.json"

echo "==============================================" >&2
echo " REGISTER RUN SCHEMA" >&2
echo " schema : hive.$RUN_SCHEMA" >&2
echo " run_id : $RUN_ID" >&2
echo " run    : $RUN_BASE" >&2
echo " meta   : $META_LOC" >&2
echo " host   : $RUN_DIR_HOST" >&2
echo "==============================================" >&2

if [ ! -d "$RUN_DIR_HOST" ]; then
  echo "ERROR: run not found on host: $RUN_DIR_HOST" >&2
  exit 2
fi

if [ -f "$MANIFEST_HOST" ]; then
  MANIFEST_ENV="$(MANIFEST_HOST="$MANIFEST_HOST" python3 "$PROJECT_ROOT/scripts/read_run_manifest_env.py" 2>/dev/null || true)"
  if [ -n "$MANIFEST_ENV" ]; then
    eval "$MANIFEST_ENV"
    echo " manifest:  $MANIFEST_HOST" >&2
    echo " dump_month:$DUMP_MONTH" >&2
    echo " dump_date: $DUMP_DATE" >&2
    echo " run_mode:  $RUN_MODE" >&2
    echo " git_sha:   $GIT_SHA" >&2
  else
    echo " WARN: manifest present but unreadable: $MANIFEST_HOST" >&2
  fi
else
  echo " WARN: manifest missing on host: $MANIFEST_HOST" >&2
fi

docker exec -i "$TRINO_CONTAINER" sh -lc "test -d /data/hive-data/_runs/$RUN_ID" || {
  echo "ERROR: run not found in container: /data/hive-data/_runs/$RUN_ID" >&2
  exit 2
}

has_dir() {
  docker exec -i "$TRINO_CONTAINER" sh -lc "test -d \"$1\""
}

docker exec -i "$TRINO_CONTAINER" trino --catalog "$TRINO_CATALOG" --execute "
  CREATE SCHEMA IF NOT EXISTS hive.$RUN_SCHEMA
  WITH (location='$META_LOC');
" >/dev/null

required_dirs="/data/hive-data/_runs/$RUN_ID/artists_v1_typed
/data/hive-data/_runs/$RUN_ID/artist_aliases_v1_typed
/data/hive-data/_runs/$RUN_ID/artist_memberships_v1_typed
/data/hive-data/_runs/$RUN_ID/masters_v1_typed
/data/hive-data/_runs/$RUN_ID/releases_v6
/data/hive-data/_runs/$RUN_ID/labels_v10"

for d in $required_dirs; do
  if ! has_dir "$d"; then
    echo "ERROR: missing required dataset dir in run: $d" >&2
    exit 2
  fi
done

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

echo "OK: registered hive.$RUN_SCHEMA" >&2
