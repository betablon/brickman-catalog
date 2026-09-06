#!/usr/bin/env python3
"""
Builds catalog.json.z for the Brickman iOS app, sourced entirely from Brickset.

Every entry carries a real Brickset `setID`. That is the point of this build:
the app no longer has to invent a synthetic identifier, so the whole class of
bug around `LegoSet.setID` being unique-keyed — a synthetic collision quietly
overwriting an unrelated row — cannot arise from catalog data, and adding a set
no longer needs a blocking API round trip to resolve its real ID.

Quota: only getSets counts against the key, and the default cap is 100/day.
  * a full build is ~47 calls  (23,500 sets at pageSize 500)
  * a delta build is ~1 call   (updatedSince, seeded from the last release)

An unfiltered getSets is rejected ("No valid parameters"), but `year` accepts a
comma-delimited list, so the whole corpus pages as a single query.

Python stdlib only. Output is zlib (RFC 1950), which Apple's Compression
framework reads via NSData.decompressed(using: .zlib).

Usage:
    python scripts/build_catalog.py                 # delta build
    python scripts/build_catalog.py out.json.z      # delta, explicit output
    python scripts/build_catalog.py --full          # full rebuild
    python scripts/build_catalog.py --years 2026    # limited, for testing
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from datetime import datetime, timedelta, timezone

BRICKSET_API_URL = "https://brickset.com/api/v3.asmx"
LATEST_RELEASE_URL = (
    "https://api.github.com/repos/betablon/brickman-catalog/releases/latest"
)

# 1949 is the earliest set Brickset holds. The upper bound runs deliberately
# ahead of the current year so next season's announcements aren't dropped —
# the previous build only covered two years, which is why 2027 sets never got
# a release date.
FIRST_YEAR = 1949
YEARS_AHEAD = 2

PAGE_SIZE = 500

# How far back a delta reaches beyond the last build, as insurance against a
# missed or partial run.
DELTA_OVERLAP_DAYS = 3


# --------------------------------------------------------------------------
# Brickset
# --------------------------------------------------------------------------


def brickset_get_sets(api_key: str, params: dict) -> dict:
    """One getSets call. Params go through json.dumps, never string formatting —
    a set name with an apostrophe otherwise produces malformed JSON."""
    body = urllib.parse.urlencode(
        {"apiKey": api_key, "userHash": "", "params": json.dumps(params)}
    ).encode()

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                f"{BRICKSET_API_URL}/getSets",
                data=body,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
            break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == 2:
                raise RuntimeError(f"Brickset request failed: {exc}") from exc
            time.sleep(2**attempt)
    else:  # pragma: no cover - the loop always breaks or raises
        raise RuntimeError(f"Brickset request failed: {last_error}")

    if data.get("status") != "success":
        raise RuntimeError(f"Brickset error: {data.get('message')!r}")
    return data


def all_years() -> str:
    last = datetime.now(timezone.utc).year + YEARS_AHEAD
    return ",".join(str(y) for y in range(FIRST_YEAR, last + 1))


def entry_key(number: str, variant) -> str:
    """The identity a set is merged on across builds.

    Brickset returns numbers with stray surrounding whitespace on a real
    minority of records (" 6626055"); unstripped, those drop out of any merge.
    """
    try:
        v = int(variant)
    except (TypeError, ValueError):
        v = 1
    return f"{str(number).strip()}-{v}"


def to_entry(s: dict) -> dict | None:
    """Reduce a Brickset record to the compact catalog shape.

    Keys stay short because the whole file ships to the phone: n/v/nm/y/t are
    required by the app's decoder, everything else is omitted when absent.
    """
    number = str(s.get("number") or "").strip()
    name = s.get("name")
    theme = s.get("theme")
    year = s.get("year")
    set_id = s.get("setID")

    # Without these the record can't be rendered or identified, so skip it
    # rather than emit something the app's non-optional decoding will reject.
    if not (number and name and theme and isinstance(year, int) and year > 0):
        return None
    if not (isinstance(set_id, int) and set_id > 0):
        return None

    try:
        variant = int(s.get("numberVariant") or 1)
    except (TypeError, ValueError):
        variant = 1

    entry = {
        "n": number,
        "v": variant,
        "nm": name,
        "y": year,
        "t": theme,
        "sid": set_id,
    }

    if s.get("subtheme"):
        entry["st"] = s["subtheme"]
    if isinstance(s.get("pieces"), int) and s["pieces"] > 0:
        entry["p"] = s["pieces"]
    if isinstance(s.get("minifigs"), int) and s["minifigs"] > 0:
        entry["mf"] = s["minifigs"]

    image = s.get("image") or {}
    if image.get("imageURL"):
        entry["img"] = image["imageURL"]

    # launchDate covers ~84% of recent sets against ~72% for the LEGO.com
    # per-region dateFirstAvailable, so it leads and the regional date backfills.
    date = s.get("launchDate")
    if not date:
        lego_com = s.get("LEGOCom") or {}
        for region in ("US", "UK", "DE"):
            date = (lego_com.get(region) or {}).get("dateFirstAvailable")
            if date:
                break
    if date:
        entry["rd"] = date

    if isinstance(s.get("released"), bool):
        entry["rel"] = s["released"]

    # Brickset's own classification (Normal, Gear, Book, Promotional, ...).
    # Carried because it is a far steadier basis for content filters than
    # matching on theme names, which is what the Rebrickable build relied on.
    if s.get("category"):
        entry["cat"] = s["category"]
    if s.get("themeGroup"):
        entry["tg"] = s["themeGroup"]

    return entry


def fetch_sets(api_key: str, years: str, since: str | None) -> tuple[dict, int]:
    """Page getSets into {key: entry}. `since` (yyyy-mm-dd) makes it a delta."""
    entries: dict[str, dict] = {}
    calls = 0
    page = 1
    skipped = 0

    while True:
        params = {"year": years, "pageSize": str(PAGE_SIZE), "pageNumber": str(page)}
        if since:
            params["updatedSince"] = since

        data = brickset_get_sets(api_key, params)
        calls += 1
        sets = data.get("sets") or []

        for s in sets:
            entry = to_entry(s)
            if entry is None:
                skipped += 1
                continue
            entries[entry_key(entry["n"], entry["v"])] = entry

        if page == 1:
            print(f"  Brickset reports {data.get('matches')} matching sets")
        print(f"  page {page}: {len(sets)} sets  (kept {len(entries)})")

        if len(sets) < PAGE_SIZE:
            break
        page += 1

    if skipped:
        print(f"  skipped {skipped} record(s) missing a required field")
    return entries, calls


def load_previous() -> tuple[dict[str, dict], str | None]:
    """Recover the last build from the published release asset, so a delta has
    something to merge into. Returns ({}, None) if unavailable, which callers
    treat as "do a full build"."""
    try:
        req = urllib.request.Request(
            LATEST_RELEASE_URL, headers={"Accept": "application/vnd.github+json"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            release = json.loads(resp.read())
        assets = release.get("assets") or []
        if not assets:
            return {}, None
        with urllib.request.urlopen(
            assets[0]["browser_download_url"], timeout=120
        ) as resp:
            previous = json.loads(zlib.decompress(resp.read()))
    except Exception as exc:  # noqa: BLE001 - any failure just means "no baseline"
        print(f"  no usable previous release ({exc})")
        return {}, None

    # Only a Brickset-sourced catalog can seed a delta. A v1/v2 asset is
    # Rebrickable-based and lacks a setID on most entries, so it is refused and
    # the build falls through to a full pull.
    if previous.get("version", 1) < 3:
        print("  previous release predates the Brickset-only build; ignoring it")
        return {}, None

    entries = {
        entry_key(e.get("n", ""), e.get("v", 1))
        : e
        for e in previous.get("sets", [])
        if e.get("sid")
    }
    synced = previous.get("generated")
    print(f"  recovered {len(entries)} entries, generated {synced}")
    return entries, synced


def build(api_key: str, force_full: bool, years: str) -> tuple[dict, int, bool]:
    """Delta where possible, full otherwise. Returns (entries, calls, was_full)."""
    if not force_full:
        previous, generated = load_previous()
        if previous and generated:
            try:
                since = (
                    datetime.fromisoformat(generated.replace("Z", "+00:00"))
                    - timedelta(days=DELTA_OVERLAP_DAYS)
                ).strftime("%Y-%m-%d")
                print(f"Delta build: Brickset changes since {since} ...")
                delta, calls = fetch_sets(api_key, years, since)
                previous.update(delta)
                print(f"  {len(delta)} sets changed, {calls} call(s)")
                return previous, calls, False
            except RuntimeError as exc:
                print(f"  delta failed ({exc}), falling back to a full build")

    print("Full build: fetching the complete Brickset corpus ...")
    entries, calls = fetch_sets(api_key, years, None)
    return entries, calls, True


# --------------------------------------------------------------------------
# Assembly
# --------------------------------------------------------------------------


def build_themes(sets: list[dict]) -> list[dict]:
    """Theme summaries, derived from the sets themselves."""
    stats: dict[str, dict] = {}
    for s in sets:
        theme, year = s["t"], s["y"]
        if theme not in stats:
            stats[theme] = {
                "count": 0,
                "subthemes": set(),
                "year_from": year,
                "year_to": year,
            }
        ts = stats[theme]
        ts["count"] += 1
        if s.get("st"):
            ts["subthemes"].add(s["st"])
        ts["year_from"] = min(ts["year_from"], year)
        ts["year_to"] = max(ts["year_to"], year)

    return [
        {
            "t": name,
            "sc": st["count"],
            "stc": len(st["subthemes"]),
            "yf": st["year_from"],
            "yt": st["year_to"],
        }
        for name, st in sorted(stats.items())
    ]


def main():
    parser = argparse.ArgumentParser(description="Build the Brickman catalog.")
    parser.add_argument("output", nargs="?", default="catalog.json.z")
    parser.add_argument(
        "--full", action="store_true", help="force a complete rebuild (~47 API calls)"
    )
    parser.add_argument(
        "--years", help="comma-delimited year filter, for testing (default: all)"
    )
    args = parser.parse_args()

    api_key = os.environ.get("BRICKSET_API_KEY", "")
    if not api_key:
        raise SystemExit("BRICKSET_API_KEY is not set — nothing to build from.")

    try:
        entries, calls, was_full = build(api_key, args.full, args.years or all_years())
    except RuntimeError as exc:
        # Publishing an empty or half-built catalog would strip the app's whole
        # search and browse corpus, so fail the run and leave the last good
        # release in place instead.
        raise SystemExit(f"Build failed, keeping the previous release: {exc}")

    catalog_sets = sorted(entries.values(), key=lambda e: (e["y"], e["n"], e["v"]))
    if not catalog_sets:
        raise SystemExit("Build produced no sets, keeping the previous release.")

    catalog = {
        "version": 3,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sets": catalog_sets,
        "themes": build_themes(catalog_sets),
    }

    json_bytes = json.dumps(catalog, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    compressed = zlib.compress(json_bytes, level=9)
    with open(args.output, "wb") as f:
        f.write(compressed)

    total = len(catalog_sets)
    def pct(n):
        return f"{n} ({100 * n / total:.1f}%)"

    print(f"\nDone: {total} sets, {len(catalog['themes'])} themes"
          f"  [{'full' if was_full else 'delta'}, {calls} getSets call(s)]")
    print(f"  real setID:    {pct(sum(1 for s in catalog_sets if s.get('sid')))}")
    print(f"  release date:  {pct(sum(1 for s in catalog_sets if s.get('rd')))}")
    print(f"  released flag: {pct(sum(1 for s in catalog_sets if 'rel' in s))}")
    print(f"  image:         {pct(sum(1 for s in catalog_sets if s.get('img')))}")
    print(f"  category:      {pct(sum(1 for s in catalog_sets if s.get('cat')))}")
    print(f"Raw JSON:   {len(json_bytes) / 1024 / 1024:.1f} MB")
    print(f"Compressed: {len(compressed) / 1024 / 1024:.1f} MB → {args.output}")


if __name__ == "__main__":
    main()
