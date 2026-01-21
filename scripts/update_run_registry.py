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

# Core required dirs (host-side check) aligned to your register_run_schema logic
REQUIRED_DATASETS = [
    "artists_v1_typed",
    "artist_aliases_v1_typed",
    "artist_memberships_v1_typed",
    "masters_v1_typed",
    "releases_v6",
    "labels_v10",
]

# Sentinel table that indicates the schema is registered enough
SENTINEL_TABLE = "releases_ref_v6"

REGISTRY_SCHEMA = "hive.discogs_history"
REGISTRY_EVENTS_TABLE = "hive.discogs_history.run_registry_events"

REGISTRY_SCHEMA_LOCATION = "file:/data/hive-data/_meta/discogs_history"
REGISTRY_EVENTS_LOCATION = "file:/data/hive-data/_meta/discogs_history/registry/run_registry_events"

# Container paths (must exist as directories for Trino external_location)
REGISTRY_EVENTS_DIR_CONTAINER = "/data/hive-data/_meta/discogs_history/registry/run_registry_events"
REGISTRY_REGISTRY_DIR_CONTAINER = "/data/hive-data/_meta/discogs_history/registry"


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def utc_now_ts() -> str:
    # Trino TIMESTAMP literal friendly
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)


def docker_exec(container: str, args: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return run(["docker", "exec", "-i", container] + args, check=check, capture=capture)


def trino_exec(container: str, catalog: str, sql: str, capture: bool = False) -> subprocess.CompletedProcess:
    args = ["trino", "--catalog", catalog, "--execute", sql]
    if capture:
        # Machine-readable output for parsing
        args = ["trino", "--output-format", "TSV", "--catalog", catalog, "--execute", sql]
    return docker_exec(container, args, check=True, capture=capture)


def require_env(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        raise SystemExit(f"ERROR: {name} not set")
    return v


def validate_base_lake(lake: str) -> None:
    if "/_runs/" in lake:
        raise SystemExit(f"ERROR: DISCOGS_DATA_LAKE must be base lake, not inside _runs: {lake}")


def read_active_run_id(lake: Path) -> str:
    active = lake / "active"
    if not active.is_symlink():
        return ""
    target = os.readlink(active)
    m = re.match(r"^_runs/([0-9]{4}-[0-9]{2}__[0-9]{8}_[0-9]{6})$", target)
    return m.group(1) if m else ""


def list_run_ids(runs_dir: Path) -> list[str]:
    out: list[str] = []
    for p in runs_dir.iterdir():
        if p.is_dir() and RUN_ID_RE.match(p.name):
            out.append(p.name)
    return sorted(out)


def schema_for_run_id(run_id: str) -> str:
    # SQL-safe identifier: replace '-' -> '_' only for schema name
    return "discogs_r_" + run_id.replace("-", "_")


def missing_required_datasets(run_dir: Path) -> list[str]:
    missing = []
    for ds in REQUIRED_DATASETS:
        if not (run_dir / ds).is_dir():
            missing.append(ds)
    return missing


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def ensure_registry_objects(container: str, catalog: str) -> None:
    # Ensure directories exist inside container for external_location
    docker_exec(container, ["sh", "-lc", f"mkdir -p {REGISTRY_EVENTS_DIR_CONTAINER}"], check=True)
    docker_exec(container, ["sh", "-lc", f"test -d {REGISTRY_EVENTS_DIR_CONTAINER}"], check=True)

    # Create schema + events table (idempotent)
    trino_exec(
        container,
        catalog,
        f"CREATE SCHEMA IF NOT EXISTS {REGISTRY_SCHEMA} WITH (location='{REGISTRY_SCHEMA_LOCATION}');",
        capture=False,
    )

    trino_exec(
        container,
        catalog,
        f"""
        CREATE TABLE IF NOT EXISTS {REGISTRY_EVENTS_TABLE} (
          event_ts_utc   TIMESTAMP,
          run_id         VARCHAR,
          schema_name    VARCHAR,
          is_active      BOOLEAN,
          action         VARCHAR,
          status         VARCHAR,
          details        VARCHAR,
          dump_month     VARCHAR,
          dump_date      VARCHAR,
          run_mode       VARCHAR,
          git_sha        VARCHAR,
          schema_version BIGINT
        )
        WITH (
          external_location = '{REGISTRY_EVENTS_LOCATION}',
          format = 'PARQUET'
        );
        """.strip(),
        capture=False,
    )


def trino_table_exists(container: str, catalog: str, schema: str, table: str) -> bool:
    sql = f"""
    SELECT 1
    FROM hive.information_schema.tables
    WHERE table_schema = '{sql_escape(schema)}'
      AND table_name = '{sql_escape(table)}'
    LIMIT 1
    """.strip()

    cp = trino_exec(container, catalog, sql, capture=True)
    out = (cp.stdout or "").strip()
    # With TSV output, existence means we get "1"
    return out == "1"


def insert_event(
    container: str,
    catalog: str,
    event_ts: str,
    run_id: str,
    schema_name: str,
    is_active: bool,
    action: str,
    status: str,
    details: str,
    dump_month: str,
    dump_date: str,
    run_mode: str,
    git_sha: str,
    schema_version: int,
) -> None:
    is_active_lit = "true" if is_active else "false"

    sql = f"""
    INSERT INTO {REGISTRY_EVENTS_TABLE} (
      event_ts_utc, run_id, schema_name, is_active, action, status, details,
      dump_month, dump_date, run_mode, git_sha, schema_version
    )
    VALUES (
      TIMESTAMP '{sql_escape(event_ts)}',
      '{sql_escape(run_id)}',
      '{sql_escape(schema_name)}',
      {is_active_lit},
      '{sql_escape(action)}',
      '{sql_escape(status)}',
      '{sql_escape(details)}',
      '{sql_escape(dump_month)}',
      '{sql_escape(dump_date)}',
      '{sql_escape(run_mode)}',
      '{sql_escape(git_sha)}',
      {schema_version}
    );
    """.strip()

    trino_exec(container, catalog, sql, capture=False)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Append run status events into hive.discogs_history.run_registry_events.")
    ap.add_argument("--trino-container", required=True)
    ap.add_argument("--trino-catalog", required=True)
    ap.add_argument("--action", default="update_registry")
    ap.add_argument("--schema-version", type=int, default=1)
    ap.add_argument("--only-run-id", default="")
    ap.add_argument("--include-active", action="store_true", help="Log active as skipped_active")
    ap.add_argument("--dump-month", default="")
    ap.add_argument("--dump-date", default="")
    ap.add_argument("--run-mode", default="history")
    ap.add_argument("--git-sha", default="")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    lake_s = require_env("DISCOGS_DATA_LAKE")
    validate_base_lake(lake_s)

    lake = Path(lake_s)
    runs_dir = lake / "_runs"
    if not runs_dir.is_dir():
        raise SystemExit(f"ERROR: runs dir not found: {runs_dir}")

    # Sanity: container reachable
    docker_exec(args.trino_container, ["sh", "-lc", "true"], check=True)

    # Ensure registry schema/table + dirs exist (this is the whole point of the refactor)
    ensure_registry_objects(args.trino_container, args.trino_catalog)

    active_run = read_active_run_id(lake)

    run_ids = list_run_ids(runs_dir)
    if args.only_run_id:
        if not RUN_ID_RE.match(args.only_run_id):
            raise SystemExit(f"ERROR: invalid run_id format: {args.only_run_id}")
        run_ids = [args.only_run_id]

    if not run_ids:
        eprint("No runs found.")
        return

    eprint("==============================================")
    eprint(" UPDATE RUN REGISTRY (append-only)")
    eprint(f" ts     : {utc_now_ts()}")
    eprint(f" lake   : {lake}")
    eprint(f" active : {active_run or '<unknown>'}")
    eprint(f" trino  : container={args.trino_container} catalog={args.trino_catalog}")
    eprint(f" action : {args.action}")
    eprint(f" schema_version : {args.schema_version}")
    eprint("==============================================")

    for rid in run_ids:
        if not RUN_ID_RE.match(rid):
            continue

        is_active = (rid == active_run) and bool(active_run)

        if is_active:
            if args.include_active:
                insert_event(
                    args.trino_container,
                    args.trino_catalog,
                    utc_now_ts(),
                    rid,
                    "discogs",
                    True,
                    args.action,
                    "skipped_active",
                    "excluded_by_active_symlink",
                    args.dump_month,
                    args.dump_date,
                    args.run_mode,
                    args.git_sha,
                    args.schema_version,
                )
                eprint(f"[ACTIVE] logged skipped_active: {rid}")
            else:
                eprint(f"[SKIP] active run not logged (use --include-active): {rid}")
            continue

        run_dir = runs_dir / rid

        missing = missing_required_datasets(run_dir)
        schema = schema_for_run_id(rid)

        if missing:
            insert_event(
                args.trino_container,
                args.trino_catalog,
                utc_now_ts(),
                rid,
                schema,
                False,
                args.action,
                "missing_data",
                "missing_datasets=" + " ".join(missing),
                args.dump_month,
                args.dump_date,
                args.run_mode,
                args.git_sha,
                args.schema_version,
            )
            eprint(f"[MISS] {rid} -> missing_data ({' '.join(missing)})")
            continue

        ok = trino_table_exists(args.trino_container, args.trino_catalog, schema, SENTINEL_TABLE)
        status = "ok" if ok else "failed_incomplete"
        details = "sentinel_ok" if ok else f"sentinel_missing={SENTINEL_TABLE}"

        insert_event(
            args.trino_container,
            args.trino_catalog,
            utc_now_ts(),
            rid,
            schema,
            False,
            args.action,
            status,
            details,
            args.dump_month,
            args.dump_date,
            args.run_mode,
            args.git_sha,
            args.schema_version,
        )
        eprint(f"[LOG] {rid} -> {status} (schema hive.{schema})")

    eprint("==============================================")
    eprint(" DONE (events appended)")
    eprint("==============================================")


if __name__ == "__main__":
    main()
