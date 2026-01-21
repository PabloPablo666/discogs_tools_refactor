#!/usr/bin/env python3
import json, os, shlex

path = os.environ["MANIFEST_HOST"]
with open(path, "r", encoding="utf-8") as f:
    m = json.load(f)

dump_month = (m.get("dump_month","") or "").strip()
dump_date  = (m.get("dump_date","") or "").strip()
run_mode   = (m.get("run_mode","") or "").strip()
git = m.get("git") or {}
git_sha = (git.get("sha","") if isinstance(git, dict) else "") or ""

print("DUMP_MONTH=" + shlex.quote(dump_month))
print("DUMP_DATE="  + shlex.quote(dump_date))
print("RUN_MODE="   + shlex.quote(run_mode))
print("GIT_SHA="    + shlex.quote(git_sha))
