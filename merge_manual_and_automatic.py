#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
import sys
from typing import Dict, Set

# Resolve paths relative to this script file
ROOT = os.path.dirname(os.path.abspath(__file__))

# Input roots
DUBS_SOURCES_DIR        = os.path.join(ROOT, "dubs", "sources")
MANUAL_DIR              = os.path.join(DUBS_SOURCES_DIR, "manual")
AUTOMATIC_MAL_DIR       = os.path.join(DUBS_SOURCES_DIR, "automatic_mal")
AUTOMATIC_ANILIST_DIR   = os.path.join(DUBS_SOURCES_DIR, "automatic_anilist")
AUTOMATIC_ANN_DIR       = os.path.join(DUBS_SOURCES_DIR, "automatic_ann")
AUTOMATIC_ANISEARCH_DIR = os.path.join(DUBS_SOURCES_DIR, "automatic_anisearch")
AUTOMATIC_HIANIME_DIR   = os.path.join(DUBS_SOURCES_DIR, "automatic_hianime")
AUTOMATIC_NSFW_DIR      = os.path.join(DUBS_SOURCES_DIR, "automatic_nsfw")
AUTOMATIC_KENNY_DIR     = os.path.join(DUBS_SOURCES_DIR, "automatic_kenny")

# Output roots
CONFIDENCE_DIR         = os.path.join(ROOT, "dubs", "confidence")
CONFIDENCE_LEVELS      = {
    "low": 1,
    "normal": 2,
    "high": 3,
    "very-high": 4,
}
COUNTS_DIR             = os.path.join(ROOT, "dubs", "counts")

DEBUG = False

def log(msg: str):
    if DEBUG:
        print(msg)

def load_json(path: str):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Warning: failed to parse JSON: {path} ({e})", file=sys.stderr)
                return {}
    return {}

def save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def infer_language_from_filename(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    if name.startswith("dubbed_"):
        return name[len("dubbed_"):]
    return name

def int_set(iterable) -> Set[int]:
    s: Set[int] = set()
    for x in iterable or []:
        try:
            s.add(int(x))
        except Exception:
            continue
    return s

def list_language_files(*dirs: str) -> Set[str]:
    files: Set[str] = set()
    for d in dirs:
        if not os.path.isdir(d):
            continue
        for fname in os.listdir(d):
            if fname.startswith("dubbed_") and fname.endswith(".json"):
                files.add(fname)
    return files

def load_language_sources(filename: str):
    """Load per-language sets from manual and each automatic source."""
    manual_path            = os.path.join(MANUAL_DIR, filename)
    auto_mal_path          = os.path.join(AUTOMATIC_MAL_DIR, filename)
    auto_anilist_path      = os.path.join(AUTOMATIC_ANILIST_DIR, filename)
    auto_ann_path          = os.path.join(AUTOMATIC_ANN_DIR, filename)
    auto_anisearch_path    = os.path.join(AUTOMATIC_ANISEARCH_DIR, filename)
    auto_hianime_path      = os.path.join(AUTOMATIC_HIANIME_DIR, filename)
    auto_nsfw_path         = os.path.join(AUTOMATIC_NSFW_DIR, filename)
    auto_kenny_path        = os.path.join(AUTOMATIC_KENNY_DIR, filename)

    manual           = load_json(manual_path)
    auto_mal         = load_json(auto_mal_path)
    auto_anilist     = load_json(auto_anilist_path)
    auto_ann         = load_json(auto_ann_path)
    auto_anisearch   = load_json(auto_anisearch_path)
    auto_hianime     = load_json(auto_hianime_path)
    auto_nsfw        = load_json(auto_nsfw_path)
    auto_kenny       = load_json(auto_kenny_path)

    # Manual lists
    manual_dubbed     = int_set(manual.get("dubbed"))
    manual_not_dubbed = int_set(manual.get("not_dubbed"))
    manual_partial = int_set(manual.get("partial"))

    # Automatic lists
    auto_mal_dubbed       = int_set(auto_mal.get("dubbed"))
    auto_anilist_dubbed   = int_set(auto_anilist.get("dubbed"))
    auto_ann_dubbed       = int_set(auto_ann.get("dubbed"))
    auto_anisearch_dubbed = int_set(auto_anisearch.get("dubbed"))
    auto_hianime_dubbed   = int_set(auto_hianime.get("dubbed"))
    auto_nsfw_dubbed      = int_set(auto_nsfw.get("dubbed"))
    auto_kenny_dubbed     = int_set(auto_kenny.get("dubbed"))

    language_value = manual.get("language") or infer_language_from_filename(filename).replace("_", " ").title()

    return {
        "manual_dubbed": manual_dubbed,
        "manual_not_dubbed": manual_not_dubbed,
        "manual_partial": manual_partial,
        "auto_sources": {
            "mal": auto_mal_dubbed,
            "anilist": auto_anilist_dubbed,
            "ann": auto_ann_dubbed,
            "anisearch": auto_anisearch_dubbed,
            "hianime": auto_hianime_dubbed,
            "nsfw": auto_nsfw_dubbed,
            "kenny": auto_kenny_dubbed,
        },
        "language_value": language_value,
    }

def compute_counts(auto_sources: Dict[str, Set[int]]) -> Dict[int, int]:
    """Count in how many automatic sources each MAL id appears."""
    counts: Dict[int, int] = {}
    for _src_name, ids in auto_sources.items():
        for mid in ids:
            counts[mid] = counts.get(mid, 0) + 1
    return counts

def build_confidence_outputs(filename: str):
    """
    For one language file (dubbed_<lang>.json):
      - compute automatic source counts (MAL, AniList, ANN, AniSearch, HiAnime, NSFW, Kenny)
      - write outputs into dubs/confidence/<level>/dubbed_<lang>.json
      - write counts to dubs/counts/dubbed_<lang>.json
    """
    info = load_language_sources(filename)

    manual_dubbed     = info["manual_dubbed"]
    manual_not_dubbed = info["manual_not_dubbed"]
    manual_partial = info["manual_partial"]
    auto_sources      = info["auto_sources"]
    language_value    = info["language_value"]

    counts = compute_counts(auto_sources)

    # Save counts file (IDs from automatic sources only)
    counts_out = {str(mid): counts[mid] for mid in sorted(counts.keys())}
    # partial is copied straight from manual (untouched)
    counts_out["partial"] = sorted(manual_partial)
    save_json(os.path.join(COUNTS_DIR, filename), counts_out)

    # Build confidence-tier files
    for level, threshold in CONFIDENCE_LEVELS.items():
        # candidates from automatic sources by threshold
        auto_candidates = {mid for mid, c in counts.items() if c >= threshold}
        # final dubbed = manual base ∪ candidates, then subtract manual exclusions
        final_dubbed = (manual_dubbed | auto_candidates) - manual_not_dubbed - manual_partial
        final_partial = sorted(manual_partial)

        result = {
            "_license": "CC BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
            "_attribution": "MyDubList - https://mydublist.com - (CC BY 4.0)",
            "_origin": "https://github.com/Joelis57/MyDubList",
            "language": language_value,
            "dubbed": sorted(final_dubbed),
            "partial": final_partial,
        }

        save_json(os.path.join(CONFIDENCE_DIR, level, filename), result)
        log(f"[{filename}] level={level} → dubbed={len(result['dubbed'])}, partial={len(result['partial'])}")

def main():
    # Discover languages across manual and all automatic sources
    all_lang_files = list_language_files(
        MANUAL_DIR,
        AUTOMATIC_MAL_DIR,
        AUTOMATIC_ANILIST_DIR,
        AUTOMATIC_ANN_DIR,
        AUTOMATIC_ANISEARCH_DIR,
        AUTOMATIC_HIANIME_DIR,
        AUTOMATIC_NSFW_DIR,
        AUTOMATIC_KENNY_DIR,
    )

    if not all_lang_files:
        print("No language files found in dubs/sources/* — nothing to merge.")
        return

    # Ensure output roots exist
    os.makedirs(COUNTS_DIR, exist_ok=True)
    for level in CONFIDENCE_LEVELS:
        os.makedirs(os.path.join(CONFIDENCE_DIR, level), exist_ok=True)

    for filename in sorted(all_lang_files):
        build_confidence_outputs(filename)

    print("Done. Updated dub counts and confidences.")

if __name__ == "__main__":
    main()