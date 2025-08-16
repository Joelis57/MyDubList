#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
import sys

# Resolve paths relative to this script file
ROOT = os.path.dirname(os.path.abspath(__file__))

# Directories
MANUAL_DIR              = os.path.join(ROOT, "manual")
AUTOMATIC_MAL_DIR       = os.path.join(ROOT, "automatic_mal")
AUTOMATIC_ANILIST_DIR   = os.path.join(ROOT, "automatic_anilist")
AUTOMATIC_ANN_DIR       = os.path.join(ROOT, "automatic_ann")
AUTOMATIC_NSFW_DIR      = os.path.join(ROOT, "automatic_nsfw")
FINAL_DIR               = os.path.join(ROOT, "final")
README_PATH             = os.path.join(ROOT, "README.md")
FINAL_LANG_INDEX_FILE   = os.path.join(FINAL_DIR, "_languages.json")

DEBUG = False

# Optional native names (fallback to Title Case English if missing)
NATIVE_NAMES = {
    "arabic": "العربية",
    "catalan": "Català",
    "chinese": "中文",
    "danish": "Dansk",
    "dutch": "Nederlands",
    "english": "English",
    "filipino": "Filipino",
    "finnish": "Suomi",
    "french": "Français",
    "german": "Deutsch",
    "hebrew": "עברית",
    "hindi": "हिन्दी",
    "hungarian": "Magyar",
    "indonesian": "Bahasa Indonesia",
    "italian": "Italiano",
    "japanese": "日本語",
    "korean": "한국어",
    "norwegian": "Norsk",
    "polish": "Polski",
    "portuguese": "Português",
    "russian": "Русский",
    "spanish": "Español",
    "swedish": "Svenska",
    "tagalog": "Tagalog",
    "thai": "ไทย",
    "turkish": "Türkçe",
    "vietnamese": "Tiếng Việt",
}

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

def merge_language_file(filename: str):
    manual_path            = os.path.join(MANUAL_DIR, filename)
    automatic_mal_path     = os.path.join(AUTOMATIC_MAL_DIR, filename)
    automatic_anilist_path = os.path.join(AUTOMATIC_ANILIST_DIR, filename)
    automatic_ann_path     = os.path.join(AUTOMATIC_ANN_DIR, filename)
    automatic_nsfw_path    = os.path.join(AUTOMATIC_NSFW_DIR, filename)
    final_path             = os.path.join(FINAL_DIR, filename)

    manual           = load_json(manual_path)
    auto_mal         = load_json(automatic_mal_path)
    auto_anilist     = load_json(automatic_anilist_path)
    auto_ann         = load_json(automatic_ann_path)
    auto_nsfw        = load_json(automatic_nsfw_path)

    # Manual lists
    manual_dubbed     = set(manual.get("dubbed", []) or [])
    manual_not_dubbed = set(manual.get("not_dubbed", []) or [])
    manual_incomplete = set(manual.get("incomplete", []) or [])

    # Automatic lists (MAL + AniList + ANN + NSFW)
    auto_mal_dubbed     = set(auto_mal.get("dubbed", []) or [])
    auto_anilist_dubbed = set(auto_anilist.get("dubbed", []) or [])
    auto_ann_dubbed     = set(auto_ann.get("dubbed", []) or [])
    auto_nsfw_dubbed    = set(auto_nsfw.get("dubbed", []) or [])

    combined_auto_dubbed = (
        auto_mal_dubbed | auto_anilist_dubbed | auto_ann_dubbed | auto_nsfw_dubbed
    )

    log(f"[{filename}] auto_mal={len(auto_mal_dubbed)} "
        f"auto_anilist={len(auto_anilist_dubbed)} nsfw={len(auto_nsfw_dubbed)} "
        f"ann={len(auto_ann_dubbed)} "
        f"=> combined_auto={len(combined_auto_dubbed)}; manual_dubbed={len(manual_dubbed)}")

    # --- Update manual: remove IDs already present in auto (de-dup) ---
    original_manual_dubbed = manual_dubbed.copy()
    manual_dubbed -= combined_auto_dubbed
    removed = len(original_manual_dubbed - manual_dubbed)
    if removed > 0:
        manual["dubbed"] = sorted(manual_dubbed)
        if not manual.get("language"):
            manual["language"] = infer_language_from_filename(filename)
        save_json(manual_path, manual)
        print(f"Updated manual: {filename} (removed {removed} auto-duplicated IDs)")

    # Merge logic (append-only)
    final_dubbed = (combined_auto_dubbed | manual_dubbed) - manual_not_dubbed - manual_incomplete
    final_incomplete = manual_incomplete

    # Prefer language in manual file; fall back to filename inference
    language_value = manual.get("language") or infer_language_from_filename(filename)

    result = {
        "_license": "CC BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
        "_attribution": "MyDubList - https://mydublist.com - (CC BY 4.0)",
        "_origin": "https://github.com/Joelis57/MyDubList",
        "language": language_value,
        "dubbed": sorted(final_dubbed),
        "incomplete": sorted(final_incomplete),
    }

    # Calculate changes for summary
    previous_final = load_json(final_path)
    previous_dubbed = set(previous_final.get("dubbed", []) or [])
    previous_incomplete = set(previous_final.get("incomplete", []) or [])
    added_dubbed = final_dubbed - previous_dubbed
    added_incomplete = final_incomplete - previous_incomplete
    changes = []
    if added_dubbed:
        changes.append(f"+{len(added_dubbed)} dubbed")
    if added_incomplete:
        changes.append(f"+{len(added_incomplete)} incomplete")
    summary = f" ({', '.join(changes)})" if changes else ""

    save_json(final_path, result)
    print(f"Merged: {filename} → {os.path.relpath(FINAL_DIR, ROOT)}{summary}")

def build_language_index():
    index = []
    if not os.path.isdir(FINAL_DIR):
        save_json(FINAL_LANG_INDEX_FILE, {"languages": index})
        print(f"Wrote language index: {FINAL_LANG_INDEX_FILE} (0 entries)")
        return index

    for fname in sorted(os.listdir(FINAL_DIR)):
        if not (fname.startswith("dubbed_") and fname.endswith(".json")):
            continue
        path = os.path.join(FINAL_DIR, fname)
        data = load_json(path) or {}
        key = infer_language_from_filename(fname)
        english_name = key.replace("_", " ").title()
        native_name = NATIVE_NAMES.get(key, english_name)
        dubbed_count = len(data.get("dubbed", []) or [])
        incomplete_count = len(data.get("incomplete", []) or [])
        index.append({
            "key": key,
            "english_name": english_name,
            "native_name": native_name,
            "file": f"final/{fname}",
            "dubbed_count": dubbed_count,
            "incomplete_count": incomplete_count,
        })

    index.sort(key=lambda x: x["dubbed_count"], reverse=True)
    save_json(FINAL_LANG_INDEX_FILE, {"languages": index})
    print(f"Wrote language index: {FINAL_LANG_INDEX_FILE} ({len(index)} entries)")
    return index

def render_lang_table(index):
    header = "| Language | Native name | Dubbed | Incomplete | File |\n|---|---:|---:|---:|---|"
    lines = [header]
    for row in index:
        lines.append(
            f'| {row["english_name"]} | {row["native_name"]} | '
            f'{row["dubbed_count"]} | {row["incomplete_count"]} | '
            f'`{row["file"]}` |'
        )
    return "\n".join(lines)

def update_readme_language_stats(index):
    table = render_lang_table(index)
    block = f"<!-- LANG-STATS:START -->\n{table}\n<!-- LANG-STATS:END -->"

    try:
        with open(README_PATH, "r", encoding="utf-8") as fh:
            content = fh.read()
    except FileNotFoundError:
        print(f"README not found at {README_PATH}, skipping README update.")
        return False

    if "<!-- LANG-STATS:START -->" in content and "<!-- LANG-STATS:END -->" in content:
        new = re.sub(
            r"<!-- LANG-STATS:START -->.*?<!-- LANG-STATS:END -->",
            block,
            content,
            flags=re.S,
        )
    else:
        suffix = "\n\n## Language statistics\n\n" + block + "\n"
        new = content + suffix

    if new != content:
        with open(README_PATH, "w", encoding="utf-8") as fh:
            fh.write(new)
        print(f"Updated README language stats: {os.path.relpath(README_PATH, ROOT)}")
        return True

    print("README language stats already up to date.")
    return False

def main():
    if not os.path.exists(MANUAL_DIR):
        print(f"Manual directory '{os.path.relpath(MANUAL_DIR, ROOT)}' does not exist.")
        sys.exit(1)

    os.makedirs(FINAL_DIR, exist_ok=True)

    # Merge each manual language file
    for filename in sorted(os.listdir(MANUAL_DIR)):
        if filename.endswith(".json"):
            merge_language_file(filename)

    # Build language index from final/ and update README
    index = build_language_index()
    update_readme_language_stats(index)

if __name__ == "__main__":
    main()
