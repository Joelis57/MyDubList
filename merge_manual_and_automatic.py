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
DUBS_SOURCES_DIR            = os.path.join(ROOT, "dubs", "sources")
MANUAL_DIR                  = os.path.join(DUBS_SOURCES_DIR, "manual")
AUTOMATIC_MAL_DIR           = os.path.join(DUBS_SOURCES_DIR, "automatic_mal")
AUTOMATIC_ANILIST_DIR       = os.path.join(DUBS_SOURCES_DIR, "automatic_anilist")
AUTOMATIC_ANN_DIR           = os.path.join(DUBS_SOURCES_DIR, "automatic_ann")
AUTOMATIC_ANISEARCH_DIR     = os.path.join(DUBS_SOURCES_DIR, "automatic_anisearch")
AUTOMATIC_KITSU_DIR         = os.path.join(DUBS_SOURCES_DIR, "automatic_kitsu")
AUTOMATIC_HIANIME_DIR       = os.path.join(DUBS_SOURCES_DIR, "automatic_hianime")
AUTOMATIC_NSFW_DIR          = os.path.join(DUBS_SOURCES_DIR, "automatic_nsfw")
AUTOMATIC_KENNY_DIR         = os.path.join(DUBS_SOURCES_DIR, "automatic_kenny")
AUTOMATIC_ANIMESCHEDULE_DIR = os.path.join(DUBS_SOURCES_DIR, "automatic_animeschedule")

# Output roots
CONFIDENCE_DIR         = os.path.join(ROOT, "dubs", "confidence")
CONFIDENCE_LEVELS      = {
    "low": 1,
    "normal": 2,
    "high": 3,
    "very-high": 4,
}
COUNTS_DIR             = os.path.join(ROOT, "dubs", "counts")

README_DIR             = os.path.join(ROOT, "README.md")
DUBS_LOW_DIR           = os.path.join(ROOT, "dubs", "confidence", "low")

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
    "lithuanian": "Lietuvių",
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
                # A corrupt file is not an empty one. Returning {} here dropped
                # every id in that source from the merge, silently demoting the
                # ids it corroborated to a lower confidence tier.
                raise RuntimeError(f"Refusing to merge: {path} is unreadable ({e})") from e
    return {}

# Refusals recorded by save_json's shrink brake, reported at the end.
SHRINK_REFUSALS: list[str] = []

# Largest share of a published file that may vanish in one run, matching the 2%
# used by every other writer under dubs/. The floor lets small languages churn.
SHRINK_BRAKE_FRACTION = 0.02
SHRINK_BRAKE_FLOOR = 25

# Most curated ids that may leave a manual file in one run. The allowance is a
# fraction of a list this same file wrote last run, so an absolute ceiling stops
# junk ids buying a large loss later. Raise it deliberately, for one run, when a
# real batch curation needs it: MERGE_CURATION_DROP_LIMIT=60.
CURATION_DROP_LIMIT = int(os.environ.get("MERGE_CURATION_DROP_LIMIT") or 25)


def _published_size(data) -> int:
    """Comparable entry count for a counts file or a confidence tier."""
    if isinstance(data, dict):
        if isinstance(data.get("dubbed"), list):
            # Confidence tiers: `dubbed` EXCLUDES partial (see finalize below),
            # so the two are disjoint and adding them is a true total.
            partial = len(data["partial"]) if isinstance(data.get("partial"), list) else 0
            return len(data["dubbed"]) + partial
        # Counts files: most partial ids are also digit keys (63 of 66 in
        # english), so adding them again charged a partial -> not_dubbed veto
        # close to twice against the budget.
        return sum(1 for k in data if str(k).isdigit())
    return 0


def save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # tmp + replace, so an interrupted write cannot leave a truncated file that
    # the next run would then refuse to read.
    # Shrink brake. This script is the SOLE writer of every dubs/counts and
    # dubs/confidence file in a PUBLIC repo, and it regenerates all of them from
    # scratch each run -- yet it was the only writer under dubs/ with no brake.
    # load_json returns {} for a MISSING source (it only raises on a corrupt
    # one), so one absent file silently cut very-high/dubbed_japanese.json by
    # 40% with "Done." as the only output. Keeping the previous good file and
    # failing at the end matches how the rest of the system handles this.
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                before = _published_size(json.load(f))
        except Exception:
            before = 0
        after = _published_size(data)
        allowed = max(SHRINK_BRAKE_FLOOR, int(before * SHRINK_BRAKE_FRACTION))
        if before and before - after > allowed:
            SHRINK_REFUSALS.append(
                f"{path}: {before} -> {after} ({before - after} removed, limit {allowed})"
            )
            print(
                f"[merge] SAFETY BRAKE: {path} would drop {before - after} of {before} entries; "
                "keeping the previous file. Check whether a source file is missing.",
                flush=True,
            )
            return

    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        # Leave no orphan behind: a stray .tmp under dubs/ would otherwise be
        # would be picked up and published by the next stage's commit.
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise

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

def build_language_index():
    index = []
    for fname in sorted(os.listdir(DUBS_LOW_DIR)):
        if not (fname.startswith("dubbed_") and fname.endswith(".json")):
            continue
        if fname == "dubbed_japanese.json":
            continue  # skip Japanese
        path = os.path.join(DUBS_LOW_DIR, fname)
        data = load_json(path) or {}
        key = infer_language_from_filename(fname)
        english_name = key.replace("_", " ").title()
        native_name = NATIVE_NAMES.get(key, english_name)
        dubbed_count = len(data.get("dubbed", []) or [])
        index.append({
            "key": key,
            "english_name": english_name,
            "native_name": native_name,
            "dubbed_count": dubbed_count,
        })
    index.sort(key=lambda x: x["dubbed_count"], reverse=True)
    return index

def render_lang_table(index):
    header = "| Language | Native name | Dubbed |\n|---|---:|---:|"
    lines = [header]
    for row in index:
        lines.append(
            f'| {row["english_name"]} | {row["native_name"]} | '
            f'{row["dubbed_count"]} |'
        )
    return "\n".join(lines)

def update_readme_language_stats():
    index = build_language_index()
    table = render_lang_table(index)
    block = f"<!-- LANG-STATS:START -->\n{table}\n<!-- LANG-STATS:END -->"

    try:
        with open(README_DIR, "r", encoding="utf-8") as fh:
            content = fh.read()
    except FileNotFoundError:
        print(f"README not found at {README_DIR}, skipping README update.")
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
        with open(README_DIR, "w", encoding="utf-8") as fh:
            fh.write(new)
        return True

    print("README language stats already up to date.")
    return False

def load_language_sources(filename: str):
    """Load per-language sets from manual and each automatic source."""
    manual_path             = os.path.join(MANUAL_DIR, filename)
    auto_mal_path           = os.path.join(AUTOMATIC_MAL_DIR, filename)
    auto_anilist_path       = os.path.join(AUTOMATIC_ANILIST_DIR, filename)
    auto_ann_path           = os.path.join(AUTOMATIC_ANN_DIR, filename)
    auto_anisearch_path     = os.path.join(AUTOMATIC_ANISEARCH_DIR, filename)
    auto_kitsu_path         = os.path.join(AUTOMATIC_KITSU_DIR, filename)
    auto_hianime_path       = os.path.join(AUTOMATIC_HIANIME_DIR, filename)
    auto_nsfw_path          = os.path.join(AUTOMATIC_NSFW_DIR, filename)
    auto_kenny_path         = os.path.join(AUTOMATIC_KENNY_DIR, filename)
    auto_animeschedule_path = os.path.join(AUTOMATIC_ANIMESCHEDULE_DIR, filename)

    # A MISSING manual file is not an empty one. It carries the only copy of
    # `not_dubbed` (curator vetoes) and `partial` (curated partial dubs), and
    # load_json returns {} when it is absent -- so every published set GROWS and
    # the shrink brake, which only measures the dubbed set, cannot fire.
    # Measured with english's manual file removed: rc=0, "Done.", 30 hand-vetoed
    # ids published as dubbed, 63 partials published as fully dubbed, and every
    # Partial Dub badge gone from the site.
    published_counts = os.path.join(COUNTS_DIR, filename)
    _was_published = os.path.exists(published_counts)
    _previously = load_json(published_counts) if _was_published else {}

    # Rule 1: every published language HAS a manual file (all 27 do). If the
    # language was published before and its manual file is gone now, the
    # checkout is broken -- not a language that lost its curation.
    if _was_published and not os.path.exists(manual_path):
        raise RuntimeError(
            f"Refusing to merge: {manual_path} is missing, but {published_counts} "
            "was published from it. Restore the manual file rather than republishing "
            "without its vetoes."
        )

    # The counts file IS the baseline, so it needs the mirror of rule 1. A
    # missing or stubbed `{}` counts file made the curation guard vacuous and
    # `_published_size` return 0, which switches the shrink brake off too --
    # both in one run, at rc=0. The confidence tiers are written from the same
    # data, so they prove the language was published even if counts is gone.
    _tier = os.path.join(CONFIDENCE_DIR, "low", filename)
    if os.path.exists(_tier) and not _previously:
        raise RuntimeError(
            f"Refusing to merge: {published_counts} is missing or empty while "
            f"{_tier} exists. That file is the record this run is checked against; "
            "restore it from git rather than republishing unchecked."
        )

    manual             = load_json(manual_path)

    # Rule 2: PRESENT but gutted is the same corruption. Checking existence
    # alone let a stub `{}` through -- and a stub is the natural mis-recovery
    # from rule 1's own error message, so this is the likelier route.
    if _was_published and isinstance(manual, dict):
        if not any(k in manual for k in ("dubbed", "not_dubbed", "partial")):
            raise RuntimeError(
                f"Refusing to merge: {manual_path} carries none of dubbed/not_dubbed/"
                "partial, so every veto and partial dub it held would be republished "
                "as fully dubbed."
            )
        # Compare by SET MEMBERSHIP against the ids actually published last
        # run, not by cardinality against a baseline this same run rewrites.
        # Counting was too weak three ways: an exact halving passed; swapping
        # every id while keeping the count passed; and because the baseline was
        # refreshed from the same (damaged) file, six runs of "just under half"
        # drained 30 vetoes to 0 at rc=0 with the evidence erased each time.
        # The published lists are the one record the manual file cannot rewrite.
        # Migration window: counts files published before `not_dubbed` was
        # added carry no list to compare against. Membership resumes on the
        # next run; until then at least insist the key still exists, which is
        # the mutation that loses every veto at once.
        if "not_dubbed" not in _previously and "not_dubbed" not in manual:
            raise RuntimeError(
                f"Refusing to merge: {manual_path} has no not_dubbed key and "
                f"{published_counts} predates the published veto list, so nothing "
                "can confirm the vetoes survived. Restore the manual file."
            )

        # Cross-witness the baseline against the confidence tiers, which are a
        # SEPARATE file written from the same run. Partial ids are excluded from
        # every tier, so `counts digit keys - low tier` reproduces the partial
        # set (minus the few curator-only ids that carry no source count). If
        # the counts file's own `partial` list has been emptied or trimmed while
        # the digits and tiers are intact, the two disagree -- which is how a
        # gutted baseline slipped past a guard that only tested whether the
        # whole file was missing.
        _low_tier = load_json(_tier) if os.path.exists(_tier) else {}
        _tier_dubbed = int_set(_low_tier.get("dubbed"))
        _implied_partial = set()
        if _tier_dubbed:
            _implied_partial = {int(k) for k in _previously if str(k).isdigit()} - _tier_dubbed

        for _key in ("not_dubbed", "partial"):
            _was = int_set(_previously.get(_key))
            if _key == "partial":
                # Union, so trimming the published list cannot shrink the
                # baseline the manual file is measured against.
                _was |= _implied_partial
            if len(_was) < 4:
                continue
            _now = int_set(manual.get(_key))
            _lost = _was - _now
            if _lost:
                # Logged every time, not only on refusal: a set that shrinks a
                # little each run stays under the limit forever, and this is
                # the only trace of it.
                print(
                    f"[merge] {filename}: {len(_lost)} {_key} id(s) removed "
                    f"({len(_was)} -> {len(_now)}).",
                    flush=True,
                )
            # A quarter, floor 4. Curation moves a handful of ids at a time; a
            # lost key, a truncation, a garbage value or an id-remap gone wrong
            # all take out far more, and membership catches the swap that an
            # equal-size set hides.
            # Capped at 25: the allowance is a fraction of a list this same
            # file wrote last run, so 1000 junk ids would otherwise buy a
            # 250-id loss the run after. An absolute ceiling bounds that.
            _cap = min(CURATION_DROP_LIMIT, max(4, len(_was) // 4))
            if len(_lost) > _cap:
                raise RuntimeError(
                    f"Refusing to merge: {len(_lost)} of {len(_was)} {_key} id(s) "
                    f"published in {published_counts} are no longer in {manual_path} "
                    f"(e.g. {sorted(_lost)[:5]}). Restore it rather than republishing "
                    "them as fully dubbed."
                )

    auto_mal           = load_json(auto_mal_path)
    auto_anilist       = load_json(auto_anilist_path)
    auto_ann           = load_json(auto_ann_path)
    auto_anisearch     = load_json(auto_anisearch_path)
    auto_kitsu         = load_json(auto_kitsu_path)
    auto_hianime       = load_json(auto_hianime_path)
    auto_nsfw          = load_json(auto_nsfw_path)
    auto_kenny         = load_json(auto_kenny_path)
    auto_animeschedule = load_json(auto_animeschedule_path)

    # Manual lists
    manual_dubbed     = int_set(manual.get("dubbed"))
    manual_not_dubbed = int_set(manual.get("not_dubbed"))
    manual_partial    = int_set(manual.get("partial"))

    # Automatic lists
    auto_mal_dubbed           = int_set(auto_mal.get("dubbed"))
    auto_anilist_dubbed       = int_set(auto_anilist.get("dubbed"))
    auto_ann_dubbed           = int_set(auto_ann.get("dubbed"))
    auto_anisearch_dubbed     = int_set(auto_anisearch.get("dubbed"))
    auto_kitsu_dubbed         = int_set(auto_kitsu.get("dubbed"))
    auto_hianime_dubbed       = int_set(auto_hianime.get("dubbed"))
    auto_nsfw_dubbed          = int_set(auto_nsfw.get("dubbed"))
    auto_kenny_dubbed         = int_set(auto_kenny.get("dubbed"))
    auto_animeschedule_dubbed = int_set(auto_animeschedule.get("dubbed"))

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
            "kitsu": auto_kitsu_dubbed,
            "hianime": auto_hianime_dubbed,
            "nsfw": auto_nsfw_dubbed,
            "kenny": auto_kenny_dubbed,
            "animeschedule": auto_animeschedule_dubbed,
        },
        "language_value": language_value,
    }

def compute_counts(sources: Dict[str, Set[int]]) -> Dict[int, int]:
    """Count in how many sources each MAL id appears."""
    counts: Dict[int, int] = {}
    for _src_name, ids in sources.items():
        for mid in ids:
            counts[mid] = counts.get(mid, 0) + 1
    return counts

def build_confidence_outputs(filename: str):
    info = load_language_sources(filename)

    manual_dubbed     = info["manual_dubbed"]
    manual_not_dubbed = info["manual_not_dubbed"]
    manual_partial    = info["manual_partial"]
    auto_sources      = info["auto_sources"]
    language_value    = info["language_value"]

    sources_for_counts = dict(auto_sources)
    sources_for_counts["manual"] = manual_dubbed

    counts = compute_counts(sources_for_counts)

    if manual_not_dubbed:
        for mid in list(counts.keys()):
            if mid in manual_not_dubbed:
                del counts[mid]

    manual_partial = manual_partial - manual_not_dubbed

    counts_out = {str(mid): counts[mid] for mid in sorted(counts.keys())}
    counts_out["partial"] = sorted(manual_partial)
    # Published so the next run can compare curation by MEMBERSHIP. Vetoes
    # appeared nowhere in the output before, which is why losing them was
    # invisible: every published set GROWS, so no shrink brake can see it.
    counts_out["not_dubbed"] = sorted(manual_not_dubbed)
    save_json(os.path.join(COUNTS_DIR, filename), counts_out)

    for level, threshold in CONFIDENCE_LEVELS.items():
        # candidates from (overridden) counts by threshold
        candidates = {mid for mid, c in counts.items() if c >= threshold}
        # final dubbed = manual base ∪ candidates, then subtract manual exclusions & partial
        final_dubbed = (manual_dubbed | candidates) - manual_not_dubbed - manual_partial
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
        AUTOMATIC_KITSU_DIR,
        AUTOMATIC_HIANIME_DIR,
        AUTOMATIC_NSFW_DIR,
        AUTOMATIC_KENNY_DIR,
        AUTOMATIC_ANIMESCHEDULE_DIR,
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

    update_readme_language_stats()
    if SHRINK_REFUSALS:
        raise SystemExit(
            "[merge] " + str(len(SHRINK_REFUSALS))
            + " file(s) refused by the shrink brake:\n  "
            + "\n  ".join(SHRINK_REFUSALS)
        )
    print("Done. Updated dub counts and confidences.")

if __name__ == "__main__":
    main()
