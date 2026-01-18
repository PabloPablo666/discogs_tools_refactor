#!/usr/bin/env python3
import argparse
import calendar
import sys
import urllib.request

BASE = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com"

def url_exists(url: str, timeout: int = 15) -> bool:
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--month", required=True, help="YYYY-MM, es. 2026-01")
    p.add_argument("--probe-type", default="artists",
                   choices=["artists","labels","masters","releases"],
                   help="Tipo usato per probing")
    args = p.parse_args()

    year_s, mon_s = args.month.split("-")
    year = int(year_s)
    mon = int(mon_s)
    last_day = calendar.monthrange(year, mon)[1]

    for day in range(last_day, 0, -1):
        ymd = f"{year:04d}{mon:02d}{day:02d}"
        url = f"{BASE}/data/{year:04d}/discogs_{ymd}_{args.probe_type}.xml.gz"
        if url_exists(url):
            print(ymd)
            return 0

    print(f"ERROR: no Discogs dump found for month={args.month}", file=sys.stderr)
    return 2

if __name__ == "__main__":
    raise SystemExit(main())
