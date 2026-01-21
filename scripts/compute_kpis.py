#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RUN_ID_RE = re.compile(r"^[0-9]{4}-[0-9]{2}__[0-9]{8}_[0-9]{6}$")

# Trino objects
REGISTRY_LATEST_VIEW = "hive.discogs_history.run_registry_latest"
KPI_EVENTS_TABLE = "hive.discogs_history.kpi_snapshot_events"

# Storage locations (must be dirs inside the container)
KPI_EVENTS_LOCATION = "file:/data/hive-data/_meta/discogs_history/kpi/kpi_snapshot_events"
KPI_EVENTS_DIR_CONTAINER = "/data/hive-data/_meta/discogs_history/kpi/kpi_snapshot_events"

# ------------------------------------------------------------
# KPI definitions (kpi_name -> (sql_template, expected_type))
# sql_template will be formatted with: {schema}
# ------------------------------------------------------------
KPI_DEFS = {
    # -------- v1 core --------
    "n_releases_distinct": (
        "SELECT CAST(count(DISTINCT release_id) AS BIGINT) FROM hive.{schema}.releases_ref_v6",
        "BIGINT",
    ),
    "rows_releases_ref_v6": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.releases_ref_v6",
        "BIGINT",
    ),
    "n_artists_distinct": (
        "SELECT CAST(count(DISTINCT artist_id) AS BIGINT) FROM hive.{schema}.artists_v1_typed",
        "BIGINT",
    ),
    "rows_artists_v1_typed": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.artists_v1_typed",
        "BIGINT",
    ),
    "n_labels_distinct": (
        "SELECT CAST(count(DISTINCT label_id) AS BIGINT) FROM hive.{schema}.labels_ref_v10",
        "BIGINT",
    ),
    "rows_labels_ref_v10": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.labels_ref_v10",
        "BIGINT",
    ),
    "n_masters_distinct": (
        "SELECT CAST(count(DISTINCT master_id) AS BIGINT) FROM hive.{schema}.masters_v1_typed",
        "BIGINT",
    ),
    "rows_masters_v1_typed": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.masters_v1_typed",
        "BIGINT",
    ),

    # Optional warehouse v1 (kept)
    "rows_release_artists_v1": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.release_artists_v1",
        "BIGINT",
    ),
    "rows_release_label_xref_v1": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.release_label_xref_v1",
        "BIGINT",
    ),

    # -------- v2 base (derived tables) --------

    # release_artists_v1
    "n_release_artist_links": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.release_artists_v1",
        "BIGINT",
    ),
    "n_releases_with_artist_link": (
        "SELECT CAST(count(DISTINCT release_id) AS BIGINT) FROM hive.{schema}.release_artists_v1",
        "BIGINT",
    ),

    # release_label_xref_v1
    "n_release_label_links": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.release_label_xref_v1",
        "BIGINT",
    ),
    "n_releases_with_label_link": (
        "SELECT CAST(count(DISTINCT release_id) AS BIGINT) FROM hive.{schema}.release_label_xref_v1",
        "BIGINT",
    ),
    "n_label_norm_distinct": (
        "SELECT CAST(count(DISTINCT label_norm) AS BIGINT) FROM hive.{schema}.release_label_xref_v1",
        "BIGINT",
    ),

    # release_style_xref_v1
    "n_release_style_links": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.release_style_xref_v1",
        "BIGINT",
    ),
    "n_releases_with_style": (
        "SELECT CAST(count(DISTINCT release_id) AS BIGINT) FROM hive.{schema}.release_style_xref_v1",
        "BIGINT",
    ),
    "n_style_norm_distinct": (
        "SELECT CAST(count(DISTINCT style_norm) AS BIGINT) FROM hive.{schema}.release_style_xref_v1",
        "BIGINT",
    ),

    # release_genre_xref_v1
    "n_release_genre_links": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.release_genre_xref_v1",
        "BIGINT",
    ),
    "n_releases_with_genre": (
        "SELECT CAST(count(DISTINCT release_id) AS BIGINT) FROM hive.{schema}.release_genre_xref_v1",
        "BIGINT",
    ),
    "n_genre_norm_distinct": (
        "SELECT CAST(count(DISTINCT genre_norm) AS BIGINT) FROM hive.{schema}.release_genre_xref_v1",
        "BIGINT",
    ),

    # label_release_counts_v1 (concentration)
    "n_labels_in_counts_table": (
        "SELECT CAST(count(*) AS BIGINT) FROM hive.{schema}.label_release_counts_v1",
        "BIGINT",
    ),
    "label_counts_total_releases": (
        "SELECT CAST(coalesce(sum(n_total_releases), 0) AS BIGINT) FROM hive.{schema}.label_release_counts_v1",
        "BIGINT",
    ),
    "top_label_releases": (
        "SELECT CAST(coalesce(max(n_total_releases), 0) AS BIGINT) FROM hive.{schema}.label_release_counts_v1",
        "BIGINT",
    ),
    "top10_labels_releases": (
        """
        SELECT CAST(coalesce(sum(n_total_releases), 0) AS BIGINT)
        FROM (
          SELECT n_total_releases
          FROM hive.{schema}.label_release_counts_v1
          ORDER BY n_total_releases DESC
          LIMIT 10
        )
        """.strip(),
        "BIGINT",
    ),
}


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def utc_now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)


def docker_exec(container: str, args: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return run(["docker", "exec", "-i", container] + args, check=check, capture=capture)


def trino_exec(container: str, catalog: str, sql: str, capture: bool = False) -> subprocess.CompletedProcess:
    # TSV when capture=True to make parsing deterministic
    if capture:
        args = ["trino", "--output-format", "TSV", "--catalog", catalog, "--execute", sql]
    else:
        args = ["trino", "--catalog", catalog, "--execute", sql]
    return docker_exec(container, args, check=True, capture=capture)


def require_env(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        raise SystemExit(f"ERROR: {name} not set")
    return v


def validate_base_lake(lake: str) -> None:
    if "/_runs/" in lake:
        raise SystemExit(f"ERROR: DISCOGS_DATA_LAKE must be base lake, not inside _runs: {lake}")


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def schema_for_run_id(run_id: str) -> str:
    # Keep run_id unchanged for filesystem, but schema must be SQL-safe
    return "discogs_r_" + run_id.replace("-", "_")


def read_active_run_id(lake: Path) -> str:
    active = lake / "active"
    if not active.is_symlink():
        return ""
    target = os.readlink(active)
    m = re.match(r"^_runs/([0-9]{4}-[0-9]{2}__[0-9]{8}_[0-9]{6})$", target)
    return m.group(1) if m else ""


def ensure_kpi_objects(container: str, catalog: str) -> None:
    # Ensure directory exists for external_location (container-side)
    docker_exec(container, ["sh", "-lc", f"mkdir -p {KPI_EVENTS_DIR_CONTAINER}"], check=True)
    docker_exec(container, ["sh", "-lc", f"test -d {KPI_EVENTS_DIR_CONTAINER}"], check=True)

    # Create table (idempotent). Schema discogs_history should already exist.
    trino_exec(
        container,
        catalog,
        f"""
        CREATE TABLE IF NOT EXISTS {KPI_EVENTS_TABLE} (
          event_ts_utc   TIMESTAMP,
          run_id         VARCHAR,
          schema_name    VARCHAR,
          kpi_name       VARCHAR,
          kpi_value      BIGINT,
          status         VARCHAR,
          details        VARCHAR,
          schema_version BIGINT
        )
        WITH (
          external_location = '{KPI_EVENTS_LOCATION}',
          format = 'PARQUET'
        );
        """.strip(),
        capture=False,
    )


def fetch_runs_to_process(
    container: str,
    catalog: str,
    include_active: bool,
    only_run_id: str,
) -> list[tuple[str, str, bool]]:
    """
    Returns list of tuples: (run_id, schema_name, is_active)
    Reads from run_registry_latest to avoid filesystem scans.
    """

    where = ["status = 'ok'"]
    if not include_active:
        where.append("is_active = false")
    if only_run_id:
        where.append(f"run_id = '{sql_escape(only_run_id)}'")

    sql = f"""
    SELECT run_id, schema_name, is_active
    FROM {REGISTRY_LATEST_VIEW}
    WHERE {" AND ".join(where)}
    ORDER BY run_id
    """.strip()

    cp = trino_exec(container, catalog, sql, capture=True)
    rows = []
    for line in (cp.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        rid, schema, is_active_s = parts
        rows.append((rid, schema, is_active_s.lower() == "true"))
    return rows


def insert_kpi_event(
    container: str,
    catalog: str,
    event_ts: str,
    run_id: str,
    schema_name: str,
    kpi_name: str,
    kpi_value: int,
    status: str,
    details: str,
    schema_version: int,
) -> None:
    sql = f"""
    INSERT INTO {KPI_EVENTS_TABLE} (
      event_ts_utc, run_id, schema_name, kpi_name, kpi_value, status, details, schema_version
    )
    VALUES (
      TIMESTAMP '{sql_escape(event_ts)}',
      '{sql_escape(run_id)}',
      '{sql_escape(schema_name)}',
      '{sql_escape(kpi_name)}',
      {kpi_value},
      '{sql_escape(status)}',
      '{sql_escape(details)}',
      {schema_version}
    );
    """.strip()

    trino_exec(container, catalog, sql, capture=False)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Compute KPI snapshots for Discogs history runs into Trino.")
    ap.add_argument("--trino-container", required=True)
    ap.add_argument("--trino-catalog", required=True)
    ap.add_argument("--schema-version", type=int, default=1)
    ap.add_argument("--only-run-id", default="", help="Process only this run_id (safe mode)")
    ap.add_argument("--include-active", action="store_true", help="Also compute KPIs for active (schema 'discogs')")
    ap.add_argument("--kpi", default="", help="Compute only one KPI (by name) from KPI_DEFS")
    ap.add_argument("--strict", action="store_true", help="Fail the whole run if any KPI query fails")
    return ap.parse_args()


def first_tsv_value(stdout: str) -> str:
    out = (stdout or "").strip()
    if not out:
        return ""
    for line in out.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def safe_bp(numer: int, denom: int) -> int:
    # basis points: 10000 = 100.00%
    if denom <= 0:
        return 0
    return (numer * 10000) // denom


def main() -> None:
    args = parse_args()

    lake_s = require_env("DISCOGS_DATA_LAKE")
    validate_base_lake(lake_s)
    lake = Path(lake_s)

    if args.only_run_id and not RUN_ID_RE.match(args.only_run_id):
        raise SystemExit(f"ERROR: invalid run_id format: {args.only_run_id}")

    docker_exec(args.trino_container, ["sh", "-lc", "true"], check=True)
    ensure_kpi_objects(args.trino_container, args.trino_catalog)

    runs = fetch_runs_to_process(
        args.trino_container,
        args.trino_catalog,
        include_active=args.include_active,
        only_run_id=args.only_run_id,
    )

    if not runs:
        eprint("No runs to process (registry_latest returned empty set).")
        return

    # Decide KPI set
    kpi_items = list(KPI_DEFS.items())
    if args.kpi:
        if args.kpi not in KPI_DEFS:
            raise SystemExit(f"ERROR: unknown KPI name: {args.kpi}")
        kpi_items = [(args.kpi, KPI_DEFS[args.kpi])]

    active_run_id = read_active_run_id(lake)

    eprint("==============================================")
    eprint(" COMPUTE KPIs (append-only)")
    eprint(f" ts     : {utc_now_ts()}")
    eprint(f" lake   : {lake}")
    eprint(f" active : {active_run_id or '<unknown>'}")
    eprint(f" trino  : container={args.trino_container} catalog={args.trino_catalog}")
    eprint(f" schema_version : {args.schema_version}")
    eprint(f" runs   : {len(runs)}")
    if args.kpi:
        eprint(f" kpi    : {args.kpi}")
    eprint("==============================================")

    for rid, schema_name, is_active in runs:
        if not is_active:
            expected_schema = schema_for_run_id(rid)
            if schema_name != expected_schema:
                eprint(f"[WARN] registry schema mismatch for {rid}: registry={schema_name} expected={expected_schema}")

        eprint(f"== run {rid} (schema hive.{schema_name}) ==")

        # Collect base KPI results for derived calculations
        vals: dict[str, int] = {}

        for kpi_name, (sql_tpl, _) in kpi_items:
            sql = sql_tpl.format(schema=schema_name)
            event_ts = utc_now_ts()

            try:
                cp = trino_exec(args.trino_container, args.trino_catalog, sql, capture=True)
                first = first_tsv_value(cp.stdout or "")
                if not first:
                    raise RuntimeError("empty_result")

                val = int(first)
                vals[kpi_name] = val

                insert_kpi_event(
                    args.trino_container,
                    args.trino_catalog,
                    event_ts,
                    rid,
                    schema_name,
                    kpi_name,
                    val,
                    "ok",
                    "",
                    args.schema_version,
                )
                eprint(f"[OK] {kpi_name}={val}")

            except Exception as ex:
                msg = str(ex)
                insert_kpi_event(
                    args.trino_container,
                    args.trino_catalog,
                    event_ts,
                    rid,
                    schema_name,
                    kpi_name,
                    0,
                    "failed_query",
                    msg[:500],
                    args.schema_version,
                )
                eprint(f"[FAIL] {kpi_name}: {msg}")

                if args.strict:
                    raise SystemExit(f"ERROR: strict mode, aborting on KPI failure: {kpi_name} for run {rid}")

        # ------------------------------------------------------------
        # Derived KPI v2 (basis points) computed from base values
        # Only if we computed the needed inputs.
        # ------------------------------------------------------------
        # NOTE: If user runs --kpi, we intentionally do NOT invent derived KPIs.
        if not args.kpi:
            event_ts = utc_now_ts()

            n_releases = vals.get("n_releases_distinct", 0)

            # Artists density/coverage
            if "n_release_artist_links" in vals and n_releases > 0:
                v = safe_bp(vals["n_release_artist_links"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "avg_artists_per_release_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] avg_artists_per_release_bp={v}")

            if "n_releases_with_artist_link" in vals and n_releases > 0:
                v = safe_bp(vals["n_releases_with_artist_link"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "pct_releases_with_artist_link_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] pct_releases_with_artist_link_bp={v}")

            # Labels density/coverage
            if "n_release_label_links" in vals and n_releases > 0:
                v = safe_bp(vals["n_release_label_links"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "avg_labels_per_release_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] avg_labels_per_release_bp={v}")

            if "n_releases_with_label_link" in vals and n_releases > 0:
                v = safe_bp(vals["n_releases_with_label_link"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "pct_releases_with_label_link_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] pct_releases_with_label_link_bp={v}")

            # Styles density/coverage
            if "n_release_style_links" in vals and n_releases > 0:
                v = safe_bp(vals["n_release_style_links"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "avg_styles_per_release_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] avg_styles_per_release_bp={v}")

            if "n_releases_with_style" in vals and n_releases > 0:
                v = safe_bp(vals["n_releases_with_style"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "pct_releases_with_style_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] pct_releases_with_style_bp={v}")

            # Genres density/coverage
            if "n_release_genre_links" in vals and n_releases > 0:
                v = safe_bp(vals["n_release_genre_links"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "avg_genres_per_release_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] avg_genres_per_release_bp={v}")

            if "n_releases_with_genre" in vals and n_releases > 0:
                v = safe_bp(vals["n_releases_with_genre"], n_releases)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "pct_releases_with_genre_bp", v, "ok", "", args.schema_version)
                eprint(f"[OK] pct_releases_with_genre_bp={v}")

            # Label concentration shares (basis points)
            total = vals.get("label_counts_total_releases", 0)
            top1 = vals.get("top_label_releases", 0)
            top10 = vals.get("top10_labels_releases", 0)

            if total > 0:
                v1 = safe_bp(top1, total)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "top_label_share_bp", v1, "ok", "", args.schema_version)
                eprint(f"[OK] top_label_share_bp={v1}")

                v10 = safe_bp(top10, total)
                insert_kpi_event(args.trino_container, args.trino_catalog, event_ts, rid, schema_name,
                                 "top10_labels_share_bp", v10, "ok", "", args.schema_version)
                eprint(f"[OK] top10_labels_share_bp={v10}")

    eprint("==============================================")
    eprint(" DONE (kpi events appended)")
    eprint("==============================================")


if __name__ == "__main__":
    main()
