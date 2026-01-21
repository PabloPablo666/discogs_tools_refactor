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

REQUIRED_DATASETS = [
    "artists_v1_typed",
    "artist_aliases_v1_typed",
    "artist_memberships_v1_typed",
    "masters_v1_typed",
    "releases_v6",
    "labels_v10",
]

OPTIONAL_WAREHOUSE = [
    ("warehouse_discogs/artist_name_map_v1", """
      CREATE TABLE IF NOT EXISTS hive.{schema}.artist_name_map_v1 (
        norm_name VARCHAR,
        artist_id BIGINT
      )
      WITH (external_location='{run_base}/warehouse_discogs/artist_name_map_v1', format='PARQUET');
    """),

    ("warehouse_discogs/release_artists_v1", """
      CREATE TABLE IF NOT EXISTS hive.{schema}.release_artists_v1 (
        release_id  BIGINT,
        artist_norm VARCHAR
      )
      WITH (external_location='{run_base}/warehouse_discogs/release_artists_v1', format='PARQUET');
    """),

    ("warehouse_discogs/release_label_xref_v1", """
      CREATE TABLE IF NOT EXISTS hive.{schema}.release_label_xref_v1 (
        release_id BIGINT,
        label_name VARCHAR,
        label_norm VARCHAR
      )
      WITH (external_location='{run_base}/warehouse_discogs/release_label_xref_v1', format='PARQUET');

      CREATE OR REPLACE VIEW hive.{schema}.release_label_xref_v1_canon AS
      SELECT release_id, label_name, label_norm
      FROM hive.{schema}.release_label_xref_v1;

      CREATE OR REPLACE VIEW hive.{schema}.release_label_xref_v1_dedup AS
      SELECT DISTINCT release_id, label_name, label_norm
      FROM hive.{schema}.release_label_xref_v1;
    """),

    ("warehouse_discogs/label_release_counts_v1", """
      CREATE TABLE IF NOT EXISTS hive.{schema}.label_release_counts_v1 (
        label_norm        VARCHAR,
        label_name_sample VARCHAR,
        n_total_releases  BIGINT
      )
      WITH (external_location='{run_base}/warehouse_discogs/label_release_counts_v1', format='PARQUET');
    """),

    ("warehouse_discogs/release_style_xref_v1", """
      CREATE TABLE IF NOT EXISTS hive.{schema}.release_style_xref_v1 (
        release_id BIGINT,
        style      VARCHAR,
        style_norm VARCHAR
      )
      WITH (external_location='{run_base}/warehouse_discogs/release_style_xref_v1', format='PARQUET');
    """),

    ("warehouse_discogs/release_genre_xref_v1", """
      CREATE TABLE IF NOT EXISTS hive.{schema}.release_genre_xref_v1 (
        release_id BIGINT,
        genre      VARCHAR,
        genre_norm VARCHAR
      )
      WITH (external_location='{run_base}/warehouse_discogs/release_genre_xref_v1', format='PARQUET');
    """),
]

CORE_SQL = """
  CREATE TABLE IF NOT EXISTS hive.{schema}.artists_v1_typed (
    artist_id      BIGINT,
    name           VARCHAR,
    realname       VARCHAR,
    profile        VARCHAR,
    data_quality   VARCHAR,
    urls           VARCHAR,
    namevariations VARCHAR,
    aliases        VARCHAR
  )
  WITH (external_location='{run_base}/artists_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.{schema}.artist_aliases_v1_typed (
    artist_id  BIGINT,
    alias_id   BIGINT,
    alias_name VARCHAR
  )
  WITH (external_location='{run_base}/artist_aliases_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.{schema}.artist_memberships_v1_typed (
    group_id    BIGINT,
    group_name  VARCHAR,
    member_id   BIGINT,
    member_name VARCHAR
  )
  WITH (external_location='{run_base}/artist_memberships_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.{schema}.masters_v1_typed (
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
  WITH (external_location='{run_base}/masters_v1_typed', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.{schema}.releases_ref_v6 (
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
  WITH (external_location='{run_base}/releases_v6', format='PARQUET');

  CREATE TABLE IF NOT EXISTS hive.{schema}.labels_ref_v10 (
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
  WITH (external_location='{run_base}/labels_v10', format='PARQUET');

  CREATE OR REPLACE VIEW hive.{schema}.artists_v1 AS
  SELECT * FROM hive.{schema}.artists_v1_typed;

  CREATE OR REPLACE VIEW hive.{schema}.artist_aliases_v1 AS
  SELECT * FROM hive.{schema}.artist_aliases_v1_typed;

  CREATE OR REPLACE VIEW hive.{schema}.artist_memberships_v1 AS
  SELECT * FROM hive.{schema}.artist_memberships_v1_typed;

  CREATE OR REPLACE VIEW hive.{schema}.masters_v1 AS
  SELECT * FROM hive.{schema}.masters_v1_typed;

  CREATE OR REPLACE VIEW hive.{schema}.artist_memberships_v1_typed_dedup AS
  SELECT DISTINCT group_id, group_name, member_id, member_name
  FROM hive.{schema}.artist_memberships_v1_typed;
"""

def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)

def docker_exec(container: str, args: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return run(["docker", "exec", "-i", container] + args, check=check, capture=capture)

def trino_exec(container: str, catalog: str, sql: str) -> None:
    docker_exec(container, ["trino", "--catalog", catalog, "--execute", sql], check=True, capture=False)

def validate_run_id(rid: str) -> None:
    if not rid or not RUN_ID_RE.match(rid):
        raise SystemExit(f"ERROR: invalid run_id format (expected YYYY-MM__YYYYMMDD_HHMMSS): {rid}")

def schema_for_run(rid: str) -> str:
    # SQL-safe: replace '-' with '_' only for schema identifier
    return "discogs_r_" + rid.replace("-", "_")

def read_active_run_id(lake: Path) -> str:
    active = lake / "active"
    if not active.is_symlink():
        return ""
    target = os.readlink(active)
    m = re.match(r"^_runs/([0-9]{4}-[0-9]{2}__[0-9]{8}_[0-9]{6})$", target)
    return m.group(1) if m else ""

def list_runs(runs_dir: Path) -> list[str]:
    out: list[str] = []
    for p in runs_dir.iterdir():
        if p.is_dir() and RUN_ID_RE.match(p.name):
            out.append(p.name)
    return sorted(out)

def require_env(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        raise SystemExit(f"ERROR: {name} not set")
    return v

def main() -> None:
    ap = argparse.ArgumentParser(description="Ensure all historical run schemas are fully registered (fail-fast).")
    ap.add_argument("--trino-container", required=True)
    ap.add_argument("--trino-catalog", required=True)
    ap.add_argument("--include-active", action="store_true", help="Also register the active run (default: exclude).")
    ap.add_argument("--only-run-id", default="", help="If set, operate only on this run_id (for testing).")
    args = ap.parse_args()

    lake_s = require_env("DISCOGS_DATA_LAKE")
    if "/_runs/" in lake_s:
        raise SystemExit(f"ERROR: DISCOGS_DATA_LAKE must be base lake (not inside _runs): {lake_s}")

    lake = Path(lake_s)
    runs_dir = lake / "_runs"
    if not runs_dir.is_dir():
        raise SystemExit(f"ERROR: runs dir not found: {runs_dir}")

    active_run = read_active_run_id(lake)

    eprint("==============================================")
    eprint(" RECONCILE (ensure tables/views exist)")
    eprint(f" ts     : {utc_now()}")
    eprint(f" lake   : {lake}")
    eprint(f" active : {active_run or '<unknown>'}")
    eprint(f" trino  : container={args.trino_container} catalog={args.trino_catalog}")
    eprint(" mode   : FAIL-FAST")
    eprint("==============================================")

    # Sanity: container reachable
    docker_exec(args.trino_container, ["sh", "-lc", "true"], check=True)

    runs = list_runs(runs_dir)
    if args.only_run_id:
        validate_run_id(args.only_run_id)
        runs = [args.only_run_id]

    if not runs:
        eprint("No runs found.")
        return

    for rid in runs:
        validate_run_id(rid)

        is_active = (rid == active_run) and bool(active_run)
        if is_active and not args.include_active:
            eprint(f"[SKIP] active run excluded: {rid}")
            continue

        run_dir_host = runs_dir / rid
        if not run_dir_host.is_dir():
            raise SystemExit(f"ERROR: run dir not found on host: {run_dir_host}")

        # Host-side required dirs (fast guardrail)
        for ds in REQUIRED_DATASETS:
            p = run_dir_host / ds
            if not p.is_dir():
                raise SystemExit(f"ERROR: missing required dataset on host for {rid}: {p}")

        # Container-side run dir must exist
        docker_exec(args.trino_container, ["sh", "-lc", f"test -d /data/hive-data/_runs/{rid}"], check=True)

        # Container-side required dirs (authoritative for Trino paths)
        for ds in REQUIRED_DATASETS:
            docker_exec(args.trino_container, ["sh", "-lc", f"test -d /data/hive-data/_runs/{rid}/{ds}"], check=True)

        schema = schema_for_run(rid)
        run_base = f"file:/data/hive-data/_runs/{rid}"
        meta_loc = f"file:/data/hive-data/_meta/discogs_history/{schema}"

        eprint("--------------------------------------------------")
        eprint(f"[RUN] {rid}")
        eprint(f" schema: hive.{schema}")
        eprint(f" base  : {run_base}")
        eprint(f" meta  : {meta_loc}")

        # Create schema
        trino_exec(
            args.trino_container,
            args.trino_catalog,
            f"CREATE SCHEMA IF NOT EXISTS hive.{schema} WITH (location='{meta_loc}');",
        )

        # Core tables + views (idempotent)
        trino_exec(
            args.trino_container,
            args.trino_catalog,
            CORE_SQL.format(schema=schema, run_base=run_base),
        )

        # Optional warehouse (only if dirs exist)
        for rel, sql_tmpl in OPTIONAL_WAREHOUSE:
            container_path = f"/data/hive-data/_runs/{rid}/{rel}"
            ok = docker_exec(args.trino_container, ["sh", "-lc", f"test -d {container_path}"], check=False).returncode == 0
            if not ok:
                eprint(f"[WARN] warehouse missing: {rel}")
                continue

            trino_exec(
                args.trino_container,
                args.trino_catalog,
                sql_tmpl.format(schema=schema, run_base=run_base),
            )
            eprint(f"[OK] warehouse registered: {rel}")

        eprint(f"[OK] ensured tables/views for hive.{schema}")

    eprint("==============================================")
    eprint(" DONE")
    eprint("==============================================")

if __name__ == "__main__":
    main()
