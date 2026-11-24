#!/usr/bin/env python3
"""
Builds animals.json.z for the Brickman iOS app: every LEGO animal piece, from
Rebrickable's free bulk downloads.

Brickman's Animals collection is keyed by LEGO element number (one number per
shape, colour and print), so this is a list of element numbers with what the
app needs to show and group them: the part's name and kind, the colour, and a
picture link.

Rebrickable's terms, which shape what this does:
  * The CSVs on the Downloads page may be used in any app. Credit
    ("data sourced from Rebrickable") is appreciated; the app shows it in
    Settings.
  * Pictures may be shown by linking to Rebrickable's server, using the links
    in the data, ideally with a local cache. So this script only copies the
    links. It must never download the pictures themselves: no scripted bulk
    picture downloads, and nothing trained on their content. The app loads
    each picture when it's first shown and keeps it on the device.

Animals are three of Rebrickable's part categories: 28 "Animals / Creatures",
74 "Animal / Creature Accessories" and 75 "Animal / Creature Body Parts".
That's about 1,900 parts in 3,500 element numbers.

If anything goes wrong, the previous release's animals.json.z is written out
instead, so a release never goes out without one.

Python stdlib only. Output is zlib (RFC 1950), the same as catalog.json.z.

Usage:
    python scripts/build_animals.py                       # animals.json.z
    python scripts/build_animals.py out.json.z
    python scripts/build_animals.py --downloads DIR       # CSVs already in DIR
"""

from __future__ import annotations

import argparse
import collections
import csv
import gzip
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import zlib
from datetime import datetime, timezone

DOWNLOADS_URL = "https://cdn.rebrickable.com/media/downloads/{name}.csv.gz"
PREVIOUS_URL = (
    "https://github.com/betablon/brickman-catalog/releases/latest/download/animals.json.z"
)
USER_AGENT = "brickman-catalog-builder (+https://github.com/betablon/brickman-catalog)"

# Rebrickable part categories that hold animals.
ANIMAL_CATEGORIES = {28, 74, 75}

# Bumped when the shape changes in a way an older app can't read.
ANIMALS_VERSION = 1

# Fewer than this many elements means a download went wrong, not that LEGO
# stopped making animals. Publishing it would empty the app's Add sheet.
MINIMUM_ELEMENTS = 2000

# --------------------------------------------------------------------------
# Kinds
# --------------------------------------------------------------------------

# The sections the app groups animals into, in the order it shows them, with
# the words in a part's name that put it there. A name is matched word by
# word, from its start, so "Animal, Big Cat, Lion" is a cat before it's
# anything else, and "Animal Body Part, Alligator / Crocodile Body" a reptile.
KINDS: list[tuple[str, tuple[str, ...]]] = [
    ("Birds", (
        "bird", "birds", "chicken", "chick", "hen", "rooster", "duck", "duckling",
        "owl", "parrot", "eagle", "seagull", "gull", "penguin", "swan", "goose",
        "flamingo", "peacock", "turkey", "ostrich", "pelican", "toucan", "crow",
        "raven", "pigeon", "dove", "hawk", "falcon", "vulture", "hummingbird",
        "robin", "macaw", "cockatoo", "budgie", "kiwi", "puffin", "stork",
    )),
    ("Cats", (
        "cat", "kitten", "lion", "lioness", "tiger", "leopard", "panther",
        "cheetah", "jaguar", "lynx", "cougar", "puma",
    )),
    ("Dogs", (
        "dog", "puppy", "pup", "bulldog", "poodle", "dachshund", "husky",
        "dalmatian", "chihuahua", "terrier", "alsatian", "retriever", "beagle",
        "hound",
    )),
    ("Horses", (
        "horse", "foal", "pony", "unicorn", "pegasus", "donkey", "zebra",
        "mule", "barding", "saddle", "bridle", "hitching",
    )),
    ("Farm animals", (
        "cow", "calf", "cattle", "bull", "pig", "piglet", "sheep", "lamb",
        "ram", "goat", "llama", "alpaca", "ox", "buffalo", "bison", "yak",
        "wool",
    )),
    ("Small animals", (
        "squirrel", "rabbit", "bunny", "hare", "hamster", "rodent", "rat",
        "mouse", "mice", "guinea", "hedgehog", "chipmunk", "beaver", "mole",
        "ferret", "skunk", "raccoon", "possum", "opossum", "capybara",
    )),
    ("Wild animals", (
        "monkey", "ape", "gorilla", "orangutan", "chimpanzee", "bear", "panda",
        "koala", "sloth", "elephant", "mammoth", "giraffe", "hippo",
        "hippopotamus", "rhino", "rhinoceros", "deer", "reindeer", "moose",
        "elk", "antelope", "gazelle", "kangaroo", "camel", "fox", "wolf",
        "badger", "otter", "bat", "lemur", "meerkat", "boar", "warthog",
        "hyena", "anteater", "armadillo", "porcupine", "tapir", "okapi",
    )),
    ("Sea life", (
        "fish", "shark", "dolphin", "whale", "orca", "stingray", "ray", "manta",
        "crab", "lobster", "shrimp", "octopus", "squid", "jellyfish",
        "starfish", "seahorse", "seal", "walrus", "eel", "clam", "shell",
        "coral", "piranha", "swordfish", "narwhal", "anglerfish", "sea",
    )),
    ("Reptiles", (
        "crocodile", "alligator", "snake", "serpent", "turtle", "tortoise",
        "lizard", "chameleon", "gecko", "iguana", "frog", "toad", "salamander",
        "newt", "axolotl", "komodo", "cobra", "python",
    )),
    ("Insects", (
        "insect", "butterfly", "moth", "bee", "bumblebee", "wasp", "ant",
        "spider", "scorpion", "beetle", "ladybug", "ladybird", "dragonfly",
        "grasshopper", "caterpillar", "snail", "worm", "fly", "mosquito",
        "cricket", "web",
    )),
    ("Dinosaurs", (
        "dinosaur", "raptor", "rex", "triceratops", "stegosaurus", "pteranodon",
        "brachiosaurus", "velociraptor", "spinosaurus", "ankylosaurus",
    )),
    ("Creatures", (
        "creature", "dragon", "ghost", "monster", "troll", "warg", "griffin",
        "phoenix", "basilisk", "thestral", "hippogriff", "buckbeak", "dewback",
        "tauntaun", "wampa", "rancor", "bantha", "kakamora", "direhorse",
        "banshee", "ikran", "skimwing", "ilu", "yeti",
    )),
]

OTHER_KIND = "Other"
KIND_ORDER = [kind for kind, _ in KINDS] + [OTHER_KIND]

# Parts whose name points the wrong way, by part number.
KIND_OVERRIDES: dict[str, str] = {
    "77319pr0001": "Horses",  # "Animal, Eeyore", a donkey
}

_KIND_BY_WORD = {word: kind for kind, words in KINDS for word in words}


def words(text: str) -> list[str]:
    return re.findall(r"[a-z]+", text.lower())


def _first_kind(text: str) -> str | None:
    for word in words(text):
        kind = _KIND_BY_WORD.get(word) or _KIND_BY_WORD.get(word.rstrip("s"))
        if kind:
            return kind
    return None


def kind_of(part_num: str, name: str) -> str:
    """The section a part sits in, from the first word of its name that names
    an animal. "Animal, Bird, Eagle" is a bird; "Animal Body Part, Shark Head"
    is sea life. The type in front ("Animal / Creature Accessory, Sheep
    Wool") is only consulted when nothing after it names an animal."""
    if part_num in KIND_OVERRIDES:
        return KIND_OVERRIDES[part_num]
    segments = [s.strip() for s in name.split(",")]
    body = []
    for i, segment in enumerate(segments):
        if not body and segment.lower() in TYPE_PREFIXES:
            continue
        body = segments[i:]
        break
    return _first_kind(", ".join(body)) or _first_kind(name) or OTHER_KIND


# --------------------------------------------------------------------------
# Short names
# --------------------------------------------------------------------------

# Rebrickable's names lead with a type: "Animal, Cat, Kitten, Short Tail, Black
# Eyes and Nose Print". The app wants "Kitten".
TYPE_PREFIXES = {
    "animal", "animals", "insect", "creature", "animal body part",
    "creature body part", "animal / creature body part",
    "animal / creature accessory", "animal body part / plant",
    "insect accessory", "accessories",
}

# Words that describe an animal without naming it. Dropped from a name.
ADJECTIVES = {
    "small", "large", "big", "baby", "standing", "sitting", "lying", "adult",
    "new", "old", "style", "plain", "young", "running", "walking", "jumping",
    "swimming", "flying", "angular",
}

# And phrases made only of these say how it's posed, not what it is:
# "Moveable Legs", "One Arm Out", "Short Tail".
QUALIFIERS = ADJECTIVES | {
    "moveable", "movable", "legs", "one", "arm", "out", "short", "long",
    "tail", "features", "open", "closed", "wings", "mouth", "complete",
    "sheet", "left", "right", "medium", "straight",
}

_CUT = re.compile(r"(?:^|\s)(?:with|w/)\s.*$|\s*[\[(].*$|\s+-\s.*$", re.IGNORECASE)


def _clean(segment: str) -> str:
    return _CUT.sub("", segment).strip()


def _is_qualifier(text: str) -> bool:
    ws = words(text)
    return bool(ws) and all(w in QUALIFIERS for w in ws)


def _strip_qualifiers(text: str) -> str:
    kept = [w for i, w in enumerate(text.split()) if i == 0 or w.lower() not in ADJECTIVES]
    return " ".join(kept)


def _pick_alternative(segment: str) -> str:
    """One name from "Monkey / Ape / Orangutan" or "Shark / Hammerhead Shark
    Head". Words after the last slash belong to every alternative, so the
    last one wins when it's longer; otherwise the first that isn't just a
    qualifier ("Baby / Calf" is a calf)."""
    options = [o.strip() for o in segment.split("/") if o.strip()]
    if not options:
        return ""
    if len(options[-1].split()) > len(options[0].split()):
        return options[-1]
    for option in options:
        if not _is_qualifier(option):
            return option
    return options[0]


def _sentence_case(text: str) -> str:
    ws = text.split()
    if not ws:
        return ""
    # Keep acronyms ("T-Rex", "LEGO") as they are; lower the rest.
    rest = [w if (w.isupper() and len(w) > 1) or "-" in w else w.lower() for w in ws[1:]]
    return " ".join([ws[0][:1].upper() + ws[0][1:]] + rest)


def _descriptive_segments(name: str) -> list[str]:
    """A name's comma-separated parts after its type, up to the first that
    starts describing the print: "Animal, Chicken with Red Comb Pattern,
    Black Eyes" is a chicken, not "Black eyes"."""
    segments = [s.strip() for s in name.split(",")]
    while segments and segments[0].lower() in TYPE_PREFIXES:
        segments = segments[1:]
    kept = []
    for segment in segments:
        cleaned = _clean(segment)
        if cleaned:
            kept.append(cleaned)
        if cleaned != segment:
            break
    return kept


def short_name(name: str) -> str:
    """A plain name for a part, for an animal nobody has named yet."""
    cleaned = _descriptive_segments(name)
    if not cleaned:
        return _sentence_case(_clean(name)) or name

    family = _strip_qualifiers(_pick_alternative(cleaned[0]))
    species = cleaned[1] if len(cleaned) > 1 else ""
    first = words(species)[:1]
    if first == ["cub"]:
        picked = f"{family} cub"
    elif first == ["baby"]:
        picked = f"baby {family.lower()}"
    elif species and not _is_qualifier(species) and len(species.split()) <= 3:
        picked = _strip_qualifiers(_pick_alternative(species))
        if _is_qualifier(picked):
            picked = family
    else:
        picked = family
    return _sentence_case(picked) or name


# --------------------------------------------------------------------------
# Rebrickable
# --------------------------------------------------------------------------


def open_csv(name: str, downloads: str | None):
    """Rows of one of Rebrickable's CSVs, from a local copy or the CDN."""
    if downloads:
        path = os.path.join(downloads, f"{name}.csv.gz")
        raw = open(path, "rb").read()
    else:
        raw = fetch(DOWNLOADS_URL.format(name=name))
        print(f"  {name}.csv.gz: {len(raw) / 1024 / 1024:.1f} MB")
    text = io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw)), encoding="utf-8", newline="")
    return csv.DictReader(text)


def fetch(url: str, timeout: int = 120) -> bytes:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2**attempt)
    raise RuntimeError(f"download failed: {url}: {last_error}")


def picture_link(element_id: str, links: collections.Counter | None) -> str | None:
    """The picture for an element, from the links Rebrickable gives its part
    in its colour. An element's own photo (`/parts/elements/<element>.jpg`)
    wins; failing that, another element photo of the same part and colour,
    then whichever link is used most."""
    if not links:
        return None
    own = [url for url in links if re.search(rf"/elements/{element_id}\.\w+$", url)]
    if own:
        return own[0]
    elements = [url for url, _ in links.most_common() if "/parts/elements/" in url]
    if elements:
        return elements[0]
    return links.most_common(1)[0][0]


def build(downloads: str | None) -> dict:
    print("Reading Rebrickable's downloads ...")

    categories = {int(r["id"]): r["name"] for r in open_csv("part_categories", downloads)}
    missing = ANIMAL_CATEGORIES - categories.keys()
    if missing:
        raise RuntimeError(f"part categories {sorted(missing)} are gone")
    for cat in sorted(ANIMAL_CATEGORIES):
        print(f"  category {cat}: {categories[cat]}")

    parts: dict[str, dict] = {}
    for r in open_csv("parts", downloads):
        try:
            cat = int(r["part_cat_id"])
        except (TypeError, ValueError):
            continue
        if cat in ANIMAL_CATEGORIES:
            name = r["name"].strip()
            parts[r["part_num"]] = {
                "p": r["part_num"],
                "n": name,
                "s": short_name(name),
                "k": kind_of(r["part_num"], name),
                "c": cat,
            }

    elements: list[dict] = []
    for r in open_csv("elements", downloads):
        if r["part_num"] in parts:
            entry = {
                "e": r["element_id"].strip(),
                "p": r["part_num"],
                "c": int(r["color_id"]),
            }
            if (r.get("design_id") or "").strip():
                entry["d"] = r["design_id"].strip()
            elements.append(entry)

    # Picture links per part and colour, as the inventories give them.
    links: dict[tuple[str, int], collections.Counter] = collections.defaultdict(collections.Counter)
    for r in open_csv("inventory_parts", downloads):
        url = (r.get("img_url") or "").strip()
        if r["part_num"] in parts and url.startswith("https://"):
            links[(r["part_num"], int(r["color_id"]))][url] += 1

    for element in elements:
        link = picture_link(element["e"], links.get((element["p"], element["c"])))
        if link:
            element["img"] = link

    used_colours = {e["c"] for e in elements}
    colours = [
        {
            "id": int(r["id"]),
            "n": r["name"],
            "rgb": r["rgb"].upper(),
            **({"t": True} if r.get("is_trans") == "True" else {}),
        }
        for r in open_csv("colors", downloads)
        if int(r["id"]) in used_colours
    ]

    # Pieces that go together: "R" relations between two animal parts, such
    # as a crocodile's body with its jaw and its tail. Stored once per pair,
    # in order, so the file doesn't churn between builds.
    pairs = sorted({
        tuple(sorted((r["child_part_num"], r["parent_part_num"])))
        for r in open_csv("part_relationships", downloads)
        if r["rel_type"] == "R"
        and r["child_part_num"] in parts
        and r["parent_part_num"] in parts
        and r["child_part_num"] != r["parent_part_num"]
    })

    if len(elements) < MINIMUM_ELEMENTS:
        raise RuntimeError(f"only {len(elements)} elements, expected {MINIMUM_ELEMENTS}+")

    # Only parts that have an element: the app can't add a part without one.
    with_elements = {e["p"] for e in elements}
    kept_parts = sorted((p for p in parts.values() if p["p"] in with_elements), key=lambda p: p["p"])
    elements.sort(key=lambda e: e["e"])

    return {
        "version": ANIMALS_VERSION,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "Rebrickable",
        "kinds": KIND_ORDER,
        "colours": sorted(colours, key=lambda c: c["id"]),
        "parts": kept_parts,
        "elements": elements,
        "pairs": [list(pair) for pair in pairs],
    }


def republish_previous(output: str, reason: Exception) -> None:
    """Writes the last release's animals.json.z, so this release has one."""
    print(f"Build failed ({reason}); republishing the previous animals.json.z")
    try:
        previous = fetch(PREVIOUS_URL)
        json.loads(zlib.decompress(previous))
    except Exception as exc:  # noqa: BLE001 - nothing usable either way
        raise SystemExit(f"No previous animals.json.z to fall back on: {exc}")
    with open(output, "wb") as f:
        f.write(previous)
    print(f"  {len(previous) / 1024:.0f} KB → {output}")


def main():
    parser = argparse.ArgumentParser(description="Build the Brickman animal catalog.")
    parser.add_argument("output", nargs="?", default="animals.json.z")
    parser.add_argument(
        "--downloads", help="read Rebrickable's .csv.gz files from this folder instead"
    )
    args = parser.parse_args()

    try:
        catalog = build(args.downloads)
    except Exception as exc:  # noqa: BLE001 - any failure falls back the same way
        republish_previous(args.output, exc)
        return

    json_bytes = json.dumps(catalog, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(json_bytes, level=9)
    with open(args.output, "wb") as f:
        f.write(compressed)

    elements = catalog["elements"]
    total = len(elements)
    pictured = sum(1 for e in elements if e.get("img"))
    own_photo = sum(1 for e in elements if f"/elements/{e['e']}." in e.get("img", ""))
    kinds = collections.Counter(p["k"] for p in catalog["parts"])
    print(f"\nDone: {total} elements, {len(catalog['parts'])} parts, "
          f"{len(catalog['colours'])} colours, {len(catalog['pairs'])} pairs")
    print(f"  picture:       {pictured} ({100 * pictured / total:.1f}%)")
    print(f"  own photo:     {own_photo} ({100 * own_photo / total:.1f}%)")
    print("  kinds:         " + ", ".join(f"{k} {kinds[k]}" for k in KIND_ORDER if kinds[k]))
    print(f"Raw JSON:   {len(json_bytes) / 1024:.0f} KB")
    print(f"Compressed: {len(compressed) / 1024:.0f} KB → {args.output}")


if __name__ == "__main__":
    main()
