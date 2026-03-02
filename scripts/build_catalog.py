#!/usr/bin/env python3
"""
Downloads Rebrickable CSV bulk exports and produces a compact catalog.json.z
for the Brickman iOS app. Python stdlib only — no pip dependencies.

Output uses zlib compression (RFC 1950) which Apple's Compression framework
handles natively via NSData.decompressed(using: .zlib).

Usage:
    python scripts/build_catalog.py              # writes catalog.json.z to cwd
    python scripts/build_catalog.py output.json.z
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import sys
import urllib.parse
import urllib.request
import zlib
from datetime import datetime, timezone

BASE_URL = "https://cdn.rebrickable.com/media/downloads/"
FILES = ["sets.csv.gz", "themes.csv.gz", "inventories.csv.gz", "inventory_minifigs.csv.gz"]
BRICKSET_API_URL = "https://brickset.com/api/v3.asmx"


def download_csv(filename: str) -> list[dict]:
    """Download a gzipped CSV from Rebrickable and return rows as dicts."""
    url = BASE_URL + filename
    print(f"Downloading {url} ...")
    with urllib.request.urlopen(url) as resp:
        raw = resp.read()
    with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_theme_lookup(themes_rows: list[dict]) -> dict:
    """Build theme_id -> {name, parent_id} lookup and resolve root ancestors."""
    by_id = {}
    for row in themes_rows:
        tid = int(row["id"])
        parent = row["parent_id"].strip()
        by_id[tid] = {
            "name": row["name"],
            "parent_id": int(parent) if parent else None,
        }

    def root_ancestor(tid: int) -> str:
        visited = set()
        current = tid
        while by_id[current]["parent_id"] is not None and current not in visited:
            visited.add(current)
            current = by_id[current]["parent_id"]
        return by_id[current]["name"]

    def direct_parent_name(tid: int) -> str | None:
        pid = by_id[tid]["parent_id"]
        if pid is None:
            return None
        return by_id[pid]["name"]

    lookup = {}
    for tid, info in by_id.items():
        root = root_ancestor(tid)
        # subtheme = this theme's own name if it has a parent, else None
        if info["parent_id"] is not None:
            subtheme = info["name"]
        else:
            subtheme = None
        lookup[tid] = {"theme": root, "subtheme": subtheme}

    return lookup


def build_inventory_map(inventories_rows: list[dict]) -> dict:
    """Build set_num -> highest-version inventory_id."""
    best = {}  # set_num -> (version, inventory_id)
    for row in inventories_rows:
        set_num = row["set_num"]
        version = int(row["version"])
        inv_id = int(row["id"])
        if set_num not in best or version > best[set_num][0]:
            best[set_num] = (version, inv_id)
    return {sn: inv_id for sn, (_, inv_id) in best.items()}


def build_minifig_counts(minifig_rows: list[dict]) -> dict:
    """Build inventory_id -> total minifig count."""
    counts = {}
    for row in minifig_rows:
        inv_id = int(row["inventory_id"])
        qty = int(row["quantity"])
        counts[inv_id] = counts.get(inv_id, 0) + qty
    return counts


def fetch_brickset_release_dates(api_key: str, years: list[int]) -> dict[str, str]:
    """Fetch release dates from Brickset API for the given years.
    Returns a dict of set_num (e.g. '72152-1') -> date string (e.g. '2026-03-01T00:00:00Z').
    """
    dates: dict[str, str] = {}
    for year in years:
        page = 1
        while True:
            params_json = f"{{'year':'{year}','pageSize':'500','pageNumber':'{page}'}}"
            body = urllib.parse.urlencode({
                "apiKey": api_key,
                "userHash": "",
                "params": params_json,
            }).encode()
            req = urllib.request.Request(
                f"{BRICKSET_API_URL}/getSets",
                data=body,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
            sets = data.get("sets") or []
            for s in sets:
                num = s.get("number", "")
                variant = s.get("numberVariant", 1)
                set_key = f"{num}-{variant}"
                lego_com = s.get("LEGOCom") or {}
                for region in ("US", "UK", "DE"):
                    region_data = lego_com.get(region) or {}
                    date_str = region_data.get("dateFirstAvailable")
                    if date_str:
                        dates[set_key] = date_str
                        break
            print(f"  Brickset year={year} page={page}: {len(sets)} sets")
            if len(sets) < 500:
                break
            page += 1
    print(f"  Brickset release dates fetched: {len(dates)}")
    return dates


def split_set_num(set_num: str) -> tuple[str, int]:
    """Split '75192-1' into ('75192', 1)."""
    if "-" in set_num:
        parts = set_num.rsplit("-", 1)
        try:
            return parts[0], int(parts[1])
        except ValueError:
            return set_num, 1
    return set_num, 1


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else "catalog.json.z"

    # Download all CSVs
    sets_rows = download_csv("sets.csv.gz")
    themes_rows = download_csv("themes.csv.gz")
    inventories_rows = download_csv("inventories.csv.gz")
    minifig_rows = download_csv("inventory_minifigs.csv.gz")

    print("Processing ...")

    # Step 1: Theme hierarchy
    theme_lookup = build_theme_lookup(themes_rows)

    # Step 2: Inventory mapping
    inv_map = build_inventory_map(inventories_rows)

    # Step 3: Minifig counts
    mf_counts = build_minifig_counts(minifig_rows)

    # Step 3b: Fetch release dates from Brickset (current + previous year)
    release_dates: dict[str, str] = {}
    brickset_api_key = os.environ.get("BRICKSET_API_KEY", "")
    if brickset_api_key:
        current_year = datetime.now(timezone.utc).year
        print("Fetching release dates from Brickset ...")
        release_dates = fetch_brickset_release_dates(
            brickset_api_key, [current_year - 1, current_year]
        )
    else:
        print("BRICKSET_API_KEY not set, skipping release dates")

    # Step 4: Process sets
    catalog_sets = []
    theme_stats = {}  # theme -> {sets: count, subthemes: set(), year_from, year_to}

    for row in sets_rows:
        set_num = row["set_num"]
        name = row["name"]
        year = int(row["year"])
        theme_id = int(row["theme_id"])
        num_parts = int(row["num_parts"]) if row["num_parts"] else 0
        img_url = row["img_url"].strip() if row.get("img_url") else ""

        # Resolve theme/subtheme
        t_info = theme_lookup.get(theme_id)
        if not t_info:
            continue
        theme = t_info["theme"]
        subtheme = t_info["subtheme"]

        # Split set number
        number, variant = split_set_num(set_num)

        # Minifig count
        inv_id = inv_map.get(set_num)
        mf = mf_counts.get(inv_id, 0) if inv_id else 0

        # Release date from Brickset
        rd = release_dates.get(set_num)

        entry = {"n": number, "v": variant, "nm": name, "y": year, "t": theme}
        if subtheme:
            entry["st"] = subtheme
        if num_parts:
            entry["p"] = num_parts
        if mf:
            entry["mf"] = mf
        if img_url:
            entry["img"] = img_url
        if rd:
            entry["rd"] = rd

        catalog_sets.append(entry)

        # Track theme stats
        if theme not in theme_stats:
            theme_stats[theme] = {
                "count": 0,
                "subthemes": set(),
                "year_from": year,
                "year_to": year,
            }
        ts = theme_stats[theme]
        ts["count"] += 1
        if subtheme:
            ts["subthemes"].add(subtheme)
        ts["year_from"] = min(ts["year_from"], year)
        ts["year_to"] = max(ts["year_to"], year)

    # Step 5: Build theme summaries
    catalog_themes = []
    for t_name, stats in sorted(theme_stats.items()):
        catalog_themes.append(
            {
                "t": t_name,
                "sc": stats["count"],
                "stc": len(stats["subthemes"]),
                "yf": stats["year_from"],
                "yt": stats["year_to"],
            }
        )

    # Step 6: Write output
    catalog = {
        "version": 1,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sets": catalog_sets,
        "themes": catalog_themes,
    }

    json_bytes = json.dumps(catalog, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(json_bytes, level=9)
    with open(output_path, "wb") as f:
        f.write(compressed)

    # Report
    print(f"Done: {len(catalog_sets)} sets, {len(catalog_themes)} themes")
    print(f"Raw JSON:   {len(json_bytes) / 1024 / 1024:.1f} MB")
    print(f"Compressed: {len(compressed) / 1024 / 1024:.1f} MB → {output_path}")


if __name__ == "__main__":
    main()
