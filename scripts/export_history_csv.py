#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RUN_ID_RE = re.compile(r"^[0-9]{4}-[0-9]{2}__[0-9]{8}_[0-9]{6}$")

KPI_LATEST_VIEW = "hive.discogs_history.kpi_snapshot_latest"
REGISTRY_LATEST_VIEW = "hive.discogs_history.run_registry_latest"

DEFAULT_REPORTS_SUBDIR = "_meta/discogs_history/reports"


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)


def docker_exec(container: str, args: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return run(["docker", "exec", "-i", container] + args, check=check, capture=capture)


def trino_exec_tsv(container: str, catalog: str, sql: str) -> str:
    cp = docker_exec(
        container,
        ["trino", "--output-format", "TSV", "--catalog", catalog, "--execute", sql],
        check=True,
        capture=True,
    )
    # Non usare .strip(): distrugge i TAB finali e quindi i campi vuoti in coda
    return (cp.stdout or "").rstrip("\n")


def require_env(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        raise SystemExit(f"ERROR: {name} not set")
    return v


def validate_base_lake(lake: str) -> None:
    if "/_runs/" in lake:
        raise SystemExit(f"ERROR: DISCOGS_DATA_LAKE must be base lake, not inside _runs: {lake}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Export Discogs history KPI latest to CSV (long + wide).")
    ap.add_argument("--trino-container", required=True)
    ap.add_argument("--trino-catalog", required=True)
    ap.add_argument("--out-dir", default="", help="Override output directory (default: $LAKE/_meta/discogs_history/reports)")
    ap.add_argument("--include-active", action="store_true", help="Include active run KPIs if present in kpi_snapshot_latest")
    ap.add_argument("--only-run-id", default="", help="Export only one run_id (safe mode)")
    ap.add_argument("--with-timestamp", action="store_true", help="Append UTC timestamp to filenames")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    lake_s = require_env("DISCOGS_DATA_LAKE")
    validate_base_lake(lake_s)
    lake = Path(lake_s)

    if args.only_run_id and not RUN_ID_RE.match(args.only_run_id):
        raise SystemExit(f"ERROR: invalid run_id format: {args.only_run_id}")

    out_dir = Path(args.out_dir) if args.out_dir else (lake / DEFAULT_REPORTS_SUBDIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Filter runs: use registry_latest so "active" handling is clean and you can exclude it
    where = ["1=1"]
    if not args.include_active:
        where.append("is_active = false")
    if args.only_run_id:
        where.append(f"run_id = '{args.only_run_id}'")

    run_filter_sql = f"""
    SELECT run_id
    FROM {REGISTRY_LATEST_VIEW}
    WHERE {" AND ".join(where)}
      AND status = 'ok'
    """.strip()

    run_ids_tsv = trino_exec_tsv(args.trino_container, args.trino_catalog, run_filter_sql)
    run_ids = []
    for line in run_ids_tsv.splitlines():
        rid = line.strip()
        if rid:
            run_ids.append(rid)

    if not run_ids:
        eprint("No runs selected (registry_latest returned empty set).")
        return

    # Fetch KPI latest rows for selected runs
    # We join against the selected run ids to avoid exporting junk.
    # Using VALUES list keeps it simple and stable.
    values_rows = ", ".join([f"('{rid}')" for rid in run_ids])

    kpi_sql = f"""
    WITH sel(run_id) AS (
      VALUES {values_rows}
    )
    SELECT
      k.event_ts_utc,
      k.run_id,
      k.schema_name,
      k.kpi_name,
      k.kpi_value,
      k.status,
      k.details
    FROM {KPI_LATEST_VIEW} k
    JOIN sel s
      ON k.run_id = s.run_id
    ORDER BY k.run_id, k.kpi_name
    """.strip()

    kpi_tsv = trino_exec_tsv(args.trino_container, args.trino_catalog, kpi_sql)

    long_rows = []
    all_kpis = set()
    for line in kpi_tsv.splitlines():
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        event_ts, run_id, schema_name, kpi_name, kpi_value, status, details = parts[:7]
        all_kpis.add(kpi_name)
        long_rows.append(
            {
                "event_ts_utc": event_ts,
                "run_id": run_id,
                "schema_name": schema_name,
                "kpi_name": kpi_name,
                "kpi_value": kpi_value,
                "status": status,
                "details": details,
            }
        )

    if not long_rows:
        eprint("No KPI rows found in kpi_snapshot_latest for selected runs.")
        return

    # Build wide rows: one row per run_id
    kpi_list = sorted(all_kpis)
    wide_map = {}
    for r in long_rows:
        rid = r["run_id"]
        wide_map.setdefault(rid, {})
        wide_map[rid]["run_id"] = rid
        wide_map[rid]["schema_name"] = r["schema_name"]
        wide_map[rid]["event_ts_utc"] = r["event_ts_utc"]
        # Only fill values for ok; keep failures as blank to avoid misleading zeros
        if r["status"] == "ok":
            wide_map[rid][r["kpi_name"]] = r["kpi_value"]
        else:
            wide_map[rid][r["kpi_name"]] = ""

    # Stable order: run_id ascending
    wide_rows = [wide_map[rid] for rid in sorted(wide_map.keys())]

    stamp = "_" + utc_now_stamp() if args.with_timestamp else ""
    long_path = out_dir / f"history_kpis_long_latest{stamp}.csv"
    wide_path = out_dir / f"history_kpis_wide_latest{stamp}.csv"

    # Write LONG
    long_fields = ["event_ts_utc", "run_id", "schema_name", "kpi_name", "kpi_value", "status", "details"]
    with long_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=long_fields)
        w.writeheader()
        for r in long_rows:
            w.writerow(r)

    # Write WIDE
    wide_fields = ["run_id", "schema_name", "event_ts_utc"] + kpi_list
    with wide_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=wide_fields)
        w.writeheader()
        for r in wide_rows:
            # ensure all columns exist
            row = {k: r.get(k, "") for k in wide_fields}
            w.writerow(row)

    eprint("==============================================")
    eprint(" EXPORT KPI CSV (latest)")
    eprint(f" out_dir: {out_dir}")
    eprint(f" long  : {long_path}")
    eprint(f" wide  : {wide_path}")
    eprint("==============================================")


if __name__ == "__main__":
    main()
