#!/usr/bin/env python3
import os
import json
import argparse
import sys

# Hardcoded directory names
MANUAL_DIR          = "manual"
AUTOMATIC_MAL_DIR   = "automatic_mal"
AUTOMATIC_ANILIST_DIR = "automatic_anilist"
AUTOMATIC_NSFW_DIR  = "automatic_nsfw"
FINAL_DIR           = "final"

DEBUG = False

def log(msg):
    if DEBUG:
        print(msg)

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Warning: failed to parse JSON: {path} ({e})", file=sys.stderr)
                return {}
    return {}

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def infer_language_from_filename(filename: str) -> str:
    # e.g., dubbed_spanish.json -> spanish
    name = os.path.splitext(filename)[0]
    if name.startswith("dubbed_"):
        return name[len("dubbed_"):]
    return name

def merge_language_file(filename):
    manual_path            = os.path.join(MANUAL_DIR, filename)
    automatic_mal_path     = os.path.join(AUTOMATIC_MAL_DIR, filename)
    automatic_anilist_path = os.path.join(AUTOMATIC_ANILIST_DIR, filename)
    automatic_nsfw_path    = os.path.join(AUTOMATIC_NSFW_DIR, filename)
    final_path             = os.path.join(FINAL_DIR, filename)

    manual           = load_json(manual_path)
    auto_mal         = load_json(automatic_mal_path)
    auto_anilist     = load_json(automatic_anilist_path)
    auto_nsfw        = load_json(automatic_nsfw_path)

    # Manual lists
    manual_dubbed     = set(manual.get("dubbed", []))
    manual_not_dubbed = set(manual.get("not_dubbed", []))
    manual_incomplete = set(manual.get("incomplete", []))

    # Automatic lists (MAL + AniList + NSFW)
    auto_mal_dubbed     = set(auto_mal.get("dubbed", []))
    auto_anilist_dubbed = set(auto_anilist.get("dubbed", []))
    auto_nsfw_dubbed    = set(auto_nsfw.get("dubbed", []))

    combined_auto_dubbed = auto_mal_dubbed | auto_anilist_dubbed | auto_nsfw_dubbed

    log(f"[{filename}] auto_mal={len(auto_mal_dubbed)} "
        f"auto_anilist={len(auto_anilist_dubbed)} nsfw={len(auto_nsfw_dubbed)} "
        f"=> combined_auto={len(combined_auto_dubbed)}; manual_dubbed={len(manual_dubbed)}")

    # --- Update manual: remove IDs already present in auto (de-dup) ---
    original_manual_dubbed = manual_dubbed.copy()
    manual_dubbed -= combined_auto_dubbed
    removed = len(original_manual_dubbed - manual_dubbed)
    if removed > 0:
        manual["dubbed"] = sorted(manual_dubbed)
        if "language" not in manual or not manual["language"]:
            manual["language"] = infer_language_from_filename(filename)
        save_json(manual_path, manual)
        print(f"Updated manual: {filename} (removed {removed} auto-duplicated IDs)")

    # Merge logic (append-only)
    final_dubbed = (combined_auto_dubbed | manual_dubbed) - manual_not_dubbed - manual_incomplete
    final_incomplete = manual_incomplete

    # Prefer language in manual file; fall back to filename inference
    language_value = manual.get("language") or infer_language_from_filename(filename)

    result = {
        "_license": "This file is licensed under the MIT License. User visible attribution is required.",
        "_origin": "https://github.com/Joelis57/MyDubList",
        "language": language_value,
        "dubbed": sorted(final_dubbed),
        "incomplete": sorted(final_incomplete)
    }

    save_json(final_path, result)
    print(f"Merged: {filename} → {FINAL_DIR} (dubbed={len(result['dubbed'])}, incomplete={len(result['incomplete'])})")

def main():
    global DEBUG
    parser = argparse.ArgumentParser(description="Merge manual with automatic MAL/AniList (and NSFW) into final.")
    parser.add_argument("--debug", default="false", help="Enable verbose logging (true/false).")
    args = parser.parse_args()
    DEBUG = str(args.debug).lower() == "true"

    if not os.path.exists(MANUAL_DIR):
        print(f"Manual directory '{MANUAL_DIR}' does not exist.")
        sys.exit(1)

    os.makedirs(FINAL_DIR, exist_ok=True)

    for filename in os.listdir(MANUAL_DIR):
        if filename.endswith(".json"):
            merge_language_file(filename)

if __name__ == "__main__":
    main()
