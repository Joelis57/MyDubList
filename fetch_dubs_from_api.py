#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from collections import defaultdict
from functools import lru_cache
import re
import requests
from math import ceil
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, parse_qs

# ======================
# CONFIG
# ======================
MAL_BASE = "https://api.myanimelist.net/v2"
JIKAN_BASE = "https://api.jikan.moe/v4"
ANILIST_BASE = "https://graphql.anilist.co"
ANN_API = "https://cdn.animenewsnetwork.com/encyclopedia/api.xml"
MALSYNC_BASE = "https://api.malsync.moe/mal/anime"

MAX_IN_MEMORY_CACHE = 5000
CALL_RETRIES = 4
RETRY_DELAYS = [10, 60, 120]
FINALIZE_EVERY_N = 100

# AniList paging (fixed by request)
ANILIST_PER_PAGE = 50
ANILIST_CHAR_PER_PAGE = 50
ANILIST_PAGE_SLEEP = 2  # seconds

# ANN batching
ANN_BATCH_SIZE = 40

# Output locations
SOURCES_DIR = "dubs/sources"
MAPPINGS_DIR = "dubs/mappings"
ANILIST_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_anilist.jsonl")
ANN_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_ann.jsonl")
MALSYNC_JSONL_PATH = os.path.join(MAPPINGS_DIR, "mappings_malsync.jsonl")
MERGED_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_merged.jsonl")

MISSING_CACHE_PATH = "cache/missing_mal_ids.json"
# Used to detect end-of-range for MAL scanning
LARGEST_KNOWN_MAL_FILE = os.path.join(SOURCES_DIR, "automatic_mal", "dubbed_japanese.json")


# ======================

# Globals
jikan_last_call = 0
json_data = defaultdict(set)
debug_log = False
anilist_stats = {
    "pages": 0,
    "media_total": 0,
    "media_with_mal": 0,
    "media_without_mal": 0,
    "media_with_langs": 0,
    "media_without_langs": 0,
}

# MAL throttling + 404 tracking
mal_last_call = 0
MAL_MIN_INTERVAL = 2.0  # seconds -> max 0.5 req/sec
last_mal_404 = False

# ANN throttle
ann_last_call = 0
ANN_MIN_INTERVAL = 1.0  # seconds between ANN calls

# MALSync throttle
MALSYNC_MIN_INTERVAL = 3  # seconds
malsync_last_call = 0.0

missing_mal_ids = set()
largest_known_mal_id = 0  # determined from dubs/sources/automatic_mal/dubbed_japanese.json

# Per-API mappings accumulated this run
anilist_mapping: dict[int, int] = {}
ann_mapping: dict[int, int] = {}


def log(message: str):
    if debug_log:
        print(message)


# ----------------------
# FS helpers
# ----------------------

def _ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


# ----------------------
# Language normalization
# ----------------------

def sanitize_lang(lang: str) -> str:
    if not lang:
        return ""

    s = lang.strip().lower()

    # Portuguese variants → "portuguese"
    if s.startswith("portuguese"):
        return "portuguese"

    # Mandarin → Chinese (unify MAL + AniList)
    if s.startswith("mandarin"):
        return "chinese"

    # Map Filipino to Tagalog (only 1 occurrence of Filipino)
    if s.startswith("filipino"):
        return "tagalog"
    
    # Remove any (...) parenthetical chunk(s)
    s = re.sub(r"\(.*?\)", "", s).strip()

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)

    return s


def filename_for_lang(lang_key: str) -> str:
    """Turn normalized language key into a filename-friendly token."""
    return lang_key.replace(" ", "_")


# ANN code/name → our normalized keys

def ann_lang_to_key(raw: str) -> str:
    if not raw:
        return ""

    r = raw.strip().upper()

    # Normalize a few common region codes and oddities
    # Spanish (LATAM) variants → 'spanish'
    if r in {"ES-419", "ES-LA", "LATAM"}:
        return "spanish"

    # Portuguese (BR) variants
    if r in {"PT-BR", "BR"}:
        return "portuguese"

    # Chinese variants
    if r in {"ZH", "ZH-CN", "ZH-TW", "ZH-HK", "CH", "CN"}:
        return "chinese"

    # ISO-ish 639-1 map (common ones)
    iso = {
        "EN": "english",
        "FR": "french",
        "DE": "german",
        "HE": "hebrew",
        "IW": "hebrew",
        "HU": "hungarian",
        "IT": "italian",
        "JA": "japanese",
        "KO": "korean",
        "PT": "portuguese",
        "ES": "spanish",
        "TL": "tagalog",
        "PL": "polish",
        "RU": "russian",
        "TR": "turkish",
        "NL": "dutch",
        "SV": "swedish",
        "NO": "norwegian",
        "NB": "norwegian",
        "NN": "norwegian",
        "DA": "danish",
        "FI": "finnish",
        "CS": "czech",
        "SK": "slovak",
        "RO": "romanian",
        "BG": "bulgarian",
        "UK": "ukrainian",
        "UA": "ukrainian",
        "EL": "greek",
        "ID": "indonesian",
        "MS": "malay",
        "VI": "vietnamese",
        "TH": "thai",
        "AR": "arabic",
        "HI": "hindi",
    }
    if r in iso:
        return iso[r]

    # If they send a word (rare), funnel through sanitize_lang
    return sanitize_lang(raw)


# ----------------------
# Missing MAL IDs cache helpers
# ----------------------

def load_missing_cache() -> set[int]:
    """Load cached anime 404 MAL IDs from disk."""
    if not os.path.exists(MISSING_CACHE_PATH):
        return set()
    try:
        with open(MISSING_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # accept either {"missing":[...]} or a bare list for robustness
        ids = data.get("missing", data if isinstance(data, list) else [])
        out = set(int(x) for x in ids if isinstance(x, int) or (isinstance(x, str) and x.isdigit()))
        log(f"[cache] Loaded {len(out)} missing MAL IDs from {MISSING_CACHE_PATH}")
        return out
    except Exception as e:
        print(f"[cache] Failed to load {MISSING_CACHE_PATH}: {e}")
        return set()


def save_missing_cache():
    """Persist cached anime 404 MAL IDs to disk."""
    _ensure_dir_for(MISSING_CACHE_PATH)
    try:
        payload = {"missing": sorted(missing_mal_ids)}
        with open(MISSING_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"[cache] Saved {len(missing_mal_ids)} missing MAL IDs to {MISSING_CACHE_PATH}")
    except Exception as e:
        print(f"[cache] Failed to save {MISSING_CACHE_PATH}: {e}")


def get_largest_known_mal_id() -> int:
    """
    Read dubs/sources/automatic_mal/dubbed_japanese.json and return the last MAL ID in the 'dubbed' array.
    If not available, returns 0.
    """
    path = LARGEST_KNOWN_MAL_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        arr = data.get("dubbed", [])
        if isinstance(arr, list) and arr:
            # Use last element as requested
            val = arr[-1]
            if isinstance(val, int):
                return val
            if isinstance(val, str) and val.isdigit():
                return int(val)
        return 0
    except Exception as e:
        log(f"[cache] Could not read largest known MAL ID from {path}: {e}")
        return 0


# ----------------------
# Mapping JSONL helpers
# ----------------------

def load_simple_jsonl_map(path: str, value_key: str) -> dict[int, int]:
    """Load JSONL file of lines like {"mal_id": <int>, value_key: <int>} into dict."""
    if not os.path.exists(path):
        return {}
    result: dict[int, int] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                mid = obj.get("mal_id")
                val = obj.get(value_key)
                if isinstance(mid, int) and isinstance(val, int):
                    result[mid] = val
    except Exception as e:
        print(f"[map] Failed to load {path}: {e}")
    return result


def save_simple_jsonl_map(path: str, mapping: dict[int, int], value_key: str):
    """Save dict[int,int] to JSONL with keys mal_id and value_key."""
    _ensure_dir_for(path)
    try:
        # merge with existing
        existing = load_simple_jsonl_map(path, value_key)
        existing.update(mapping)
        with open(path, "w", encoding="utf-8") as f:
            for mid in sorted(existing.keys()):
                rec = {"mal_id": mid, value_key: int(existing[mid])}
                f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[map] Wrote {len(existing)} lines to {path}")
    except Exception as e:
        print(f"[map] Failed to save {path}: {e}")


def load_jsonl_map(path: str) -> dict[int, dict]:
    """Load JSONL (mal_id -> record dict) into a dict. If multiple lines for same mal_id, last one wins."""
    if not os.path.exists(path):
        return {}
    result: dict[int, dict] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                mid = obj.get("mal_id")
                if isinstance(mid, int):
                    # store the rest of the fields
                    rec = {k: v for k, v in obj.items() if k != "mal_id"}
                    # ensure 'sites' key exists for malsync style
                    if "sites" in rec and not isinstance(rec["sites"], dict):
                        rec["sites"] = {}
                    result[mid] = rec
    except Exception as e:
        print(f"[MALSync] Failed to load existing JSONL '{path}': {e}")
    return result


def save_jsonl_map(path: str, mapping: dict[int, dict]):
    """Write the entire mapping dict to JSONL, sorted by mal_id."""
    _ensure_dir_for(path)
    try:
        with open(path, "w", encoding="utf-8") as f:
            for mid in sorted(mapping.keys()):
                rec = mapping[mid] if isinstance(mapping[mid], dict) else {}
                # Ensure minimal structure
                out_line = {"mal_id": mid, **rec}
                f.write(json.dumps(out_line, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[MALSync] Wrote {len(mapping)} lines to {path}")
    except Exception as e:
        print(f"[MALSync] Failed to save JSONL '{path}': {e}")


def merge_all_mappings():
    """Merge existing mappings into a single JSONL at MERGED_MAPPING_JSONL."""
    master: dict[int, dict] = {}

    # AniList
    a_map = load_simple_jsonl_map(ANILIST_MAPPING_JSONL, "anilist_id")
    for mid, aid in a_map.items():
        master.setdefault(mid, {})["anilist_id"] = aid

    # ANN
    ann_map = load_simple_jsonl_map(ANN_MAPPING_JSONL, "ann_id")
    for mid, annid in ann_map.items():
        master.setdefault(mid, {})["ann_id"] = annid

    # MALSync
    ms_map = load_jsonl_map(MALSYNC_JSONL_PATH)
    for mid, rec in ms_map.items():
        master.setdefault(mid, {})["malsync"] = {
            "sites": rec.get("sites", {}),
            "total": rec.get("total"),
            "anidbId": rec.get("anidbId"),
        }

    # Write merged
    _ensure_dir_for(MERGED_MAPPING_JSONL)
    try:
        with open(MERGED_MAPPING_JSONL, "w", encoding="utf-8") as f:
            for mid in sorted(master.keys()):
                line = {"mal_id": mid, **master[mid]}
                f.write(json.dumps(line, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[merge] Wrote merged mappings: {len(master)} to {MERGED_MAPPING_JSONL}")
    except Exception as e:
        print(f"[merge] Failed to write merged mappings: {e}")


# ----------------------
# MAL + Jikan helpers
# ----------------------

@lru_cache(maxsize=MAX_IN_MEMORY_CACHE)
def get_anime_roles_for_va_cached(person_id):
    log(f"    Fetching VA {person_id} from API")
    return jikan_get(f"/people/{person_id}/voices")


def mal_get(url, client_id):
    global mal_last_call, last_mal_404

    now = time.time()
    to_wait = MAL_MIN_INTERVAL - (now - mal_last_call)
    if to_wait > 0:
        time.sleep(to_wait)

    headers = {"X-MAL-CLIENT-ID": client_id}
    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            mal_last_call = time.time()
            response = requests.get(url, headers=headers)
            if response.status_code == 404:
                last_mal_404 = True
                log("  404 Anime Not Found, skipping")
                return None
            response.raise_for_status()
            last_mal_404 = False
            return response.json()
        except Exception as e:
            last_exception = e
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"  MAL API call failed. Retrying in {delay} seconds...")
                time.sleep(delay)

    print(f"  All {CALL_RETRIES} attempts failed for {url}")
    raise last_exception


def jikan_get(url):
    global jikan_last_call
    now = time.time()
    to_wait = 1.0 - (now - jikan_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    jikan_last_call = time.time()

    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            response = requests.get(JIKAN_BASE + url)
            if response.status_code == 404:
                log("    404 Not Found, skipping")
                return None
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_exception = e
            print(f"    Attempt {attempt + 1} failed: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"    Jikan API call failed. Retrying in {delay} seconds...")
                time.sleep(delay)
    print(f"    All {CALL_RETRIES} attempts failed for {url}")
    raise last_exception


def get_characters(mal_id, client_id):
    return mal_get(f"{MAL_BASE}/anime/{mal_id}/characters?limit=1", client_id)


def get_voice_actors(char_id):
    return jikan_get(f"/characters/{char_id}/voices")


def process_anime_mal(mal_id, client_id):
    log(f"Processing MAL ID: {mal_id}")
    characters = get_characters(mal_id, client_id)

    # If the characters call 404'd for the anime, short-circuit and report it
    if 'last_mal_404' in globals() and last_mal_404:
        return True  # was_404

    if not characters or "data" not in characters or not characters["data"]:
        log("  No characters found, skipping")
        return False

    first_char = characters["data"][0]["node"]
    char_id = first_char["id"]

    voice_actors = get_voice_actors(char_id)
    if not voice_actors or "data" not in voice_actors:
        return False

    for va_entry in voice_actors["data"]:
        raw_lang = va_entry.get("language")
        lang_key = sanitize_lang(raw_lang)
        if not lang_key:
            continue

        person = va_entry.get("person") or {}
        person_id = person.get("mal_id")
        if not person_id:
            continue

        log(f"  Processing voice actor: {person.get('name', 'Unknown')} ({raw_lang} -> {lang_key})")

        va_roles = get_anime_roles_for_va_cached(person_id)
        if not va_roles or "data" not in va_roles:
            continue

        for role_entry in va_roles["data"]:
            anime = role_entry.get("anime")
            if anime and anime.get("mal_id") == mal_id:
                json_data[lang_key].add(mal_id)
                break

    return False


# ----------------------
# AniList helpers
# ----------------------
ANILIST_QUERY = """
query ($page: Int, $perPage: Int, $charPerPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { currentPage hasNextPage total }
    media(type: ANIME, sort: ID) {
      id
      idMal
      characters(perPage: $charPerPage) {
        edges {
          node { id }
          voiceActors {
            id
            languageV2
            name { full }
          }
        }
      }
    }
  }
}
"""

ANILIST_TOTAL_QUERY = """
query ($page: Int = 1, $perPage: Int = 50) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { total lastPage currentPage hasNextPage perPage }
    media(type: ANIME, sort: ID) { id }
  }
}
"""


def anilist_post(query, variables):
    last_exception = None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(CALL_RETRIES):
        try:
            r = requests.post(ANILIST_BASE, json={"query": query, "variables": variables}, headers=headers)
            if r.status_code >= 400:
                print(f"  AniList HTTP {r.status_code}: {r.text[:500]}")
                r.raise_for_status()
            data = r.json()
            if "errors" in data:
                raise RuntimeError(data["errors"])
            return data
        except Exception as e:
            last_exception = e
            print(f"  AniList attempt {attempt + 1} failed: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"  AniList call failed. Retrying in {delay} seconds...")
                time.sleep(delay)
    print("  All AniList attempts failed.")
    raise last_exception


def anilist_total_pages(per_page=ANILIST_PER_PAGE) -> int:
    data = anilist_post(ANILIST_TOTAL_QUERY, {"perPage": per_page})
    info = data["data"]["Page"]["pageInfo"]
    if info.get("lastPage"):
        return int(info["lastPage"])
    total = info.get("total", 0)
    return ceil(total / per_page) if total else 0


def process_anilist_page(page: int) -> tuple[bool, int, set[int]]:
    variables = {
        "page": page,
        "perPage": ANILIST_PER_PAGE,
        "charPerPage": ANILIST_CHAR_PER_PAGE,
    }
    data = anilist_post(ANILIST_QUERY, variables)
    if not data:
        return False, 0, set()

    page_data = data["data"]["Page"]
    has_next = page_data["pageInfo"]["hasNextPage"]
    media_list = page_data.get("media", [])

    # Local counters for this page
    page_media_total = len(media_list)
    page_with_mal = 0
    page_without_mal = 0
    page_with_langs = 0
    page_without_langs = 0

    processed = 0
    checked_ids: set[int] = set()

    for media in media_list:
        mal_id = media.get("idMal")
        if not mal_id:
            page_without_mal += 1
            if debug_log:
                log("  skip: no idMal")
            continue

        # mark as checked this run
        try:
            checked_ids.add(int(mal_id))
        except Exception:
            pass

        # Record mapping MAL → AniList ID
        try:
            aid = int(media.get("id"))
            anilist_mapping[int(mal_id)] = aid
        except Exception:
            pass

        page_with_mal += 1

        chars = media.get("characters") or {}
        edges = chars.get("edges") or []

        langs = set()
        va_count = 0
        for edge in edges:
            vas = edge.get("voiceActors") or []
            va_count += len(vas)
            for va in vas:
                key = sanitize_lang(va.get("languageV2"))
                if key:
                    langs.add(key)

        if not langs:
            page_without_langs += 1
            if debug_log:
                log(f"  MAL {mal_id}: {va_count} VAs across {len(edges)} chars, langs=[]")
        else:
            page_with_langs += 1
            if debug_log:
                log(f"  MAL {mal_id}: {va_count} VAs -> langs={sorted(langs)}")
            for key in langs:
                json_data[key].add(int(mal_id))

        processed += 1

    if debug_log:
        log(
            f"Page {page} summary: media={page_media_total} "
            f"with_mal={page_with_mal} no_mal={page_without_mal} "
            f"with_langs={page_with_langs} no_langs={page_without_langs}"
        )

    # Update global stats
    anilist_stats["pages"] += 1
    anilist_stats["media_total"] += page_media_total
    anilist_stats["media_with_mal"] += page_with_mal
    anilist_stats["media_without_mal"] += page_without_mal
    anilist_stats["media_with_langs"] += page_with_langs
    anilist_stats["media_without_langs"] += page_without_langs

    return has_next, processed, checked_ids


# ----------------------
# Finalize (dubbed_* sources + mappings) with safe removals for checked IDs
# ----------------------

def finalize_jsons(api_mode: str, checked_ok_ids: set[int] | None = None):
    """
    Write/merge dubbed lists under dubs/sources/automatic_<api>/.
    Removals are allowed, but only for MAL IDs contained in checked_ok_ids.
    If checked_ok_ids is None/empty, no removals occur; only additions happen.
    """
    output_dir = os.path.join(SOURCES_DIR, f"automatic_{api_mode}")
    os.makedirs(output_dir, exist_ok=True)

    # Build per-language found sets from this run
    found_per_lang: dict[str, set[int]] = {k: {int(x) for x in v} for k, v in json_data.items()}

    # Collect all languages to consider (existing files + new keys)
    languages: set[str] = set(found_per_lang.keys())
    try:
        for fname in os.listdir(output_dir):
            if not fname.startswith("dubbed_") or not fname.endswith(".json"):
                continue
            lang_key = fname[len("dubbed_"):-len(".json")].replace("_", " ")
            languages.add(lang_key)
    except FileNotFoundError:
        pass

    checked_ok_ids = checked_ok_ids or set()

    for lang_key in sorted(languages):
        fname_lang = filename_for_lang(lang_key)
        filename = os.path.join(output_dir, f"dubbed_{fname_lang}.json")

        # Load existing
        if os.path.exists(filename):
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                    existing_ids = set(int(x) for x in existing_data.get("dubbed", []))
            except Exception:
                existing_ids = set()
        else:
            existing_ids = set()

        found_ids = found_per_lang.get(lang_key, set())

        # Only remove IDs that were checked this run and are no longer present
        removal_candidates = (existing_ids & checked_ok_ids) - found_ids
        updated_ids = (existing_ids - removal_candidates) | found_ids

        if removal_candidates and debug_log:
            log(f"  [{api_mode}] {lang_key}: removing {len(removal_candidates)} ids; keeping {len(updated_ids)}")

        # Write file
        obj = {
            "_license": "CC BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
            "_attribution": "MyDubList - https://mydublist.com - (CC BY 4.0)",
            "_origin": "https://github.com/Joelis57/MyDubList",
            "language": lang_key.capitalize(),
            "dubbed": sorted(int(x) for x in updated_ids),
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    # Also finalize per-API mappings and the merged mapping
    if api_mode == "anilist" and anilist_mapping:
        save_simple_jsonl_map(ANILIST_MAPPING_JSONL, anilist_mapping, "anilist_id")
    if api_mode == "ann" and ann_mapping:
        save_simple_jsonl_map(ANN_MAPPING_JSONL, ann_mapping, "ann_id")

    # Always refresh merged mappings after a finalize
    merge_all_mappings()


# ----------------------
# ANN helpers
# ----------------------

def ann_get(url):
    """Throttled GET for ANN (XML)."""
    global ann_last_call
    now = time.time()
    to_wait = ANN_MIN_INTERVAL - (now - ann_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    ann_last_call = time.time()

    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            resp = requests.get(url, headers={"Accept": "text/xml"})
            if resp.status_code == 404:
                log("  ANN 404, skipping batch")
                return None
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_exception = e
            print(f"  ANN attempt {attempt + 1} failed: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"  ANN call failed. Retrying in {delay} seconds...")
                time.sleep(delay)
    print(f"  All {CALL_RETRIES} ANN attempts failed for {url}")
    raise last_exception


def extract_ann_id_from_url(url: str) -> int | None:
    """Parse ANN 'encyclopedia/anime.php?id=12345' from a URL."""
    if not url:
        return None
    try:
        # Be liberal: handle www/cdn subdomains and any query params
        parsed = urlparse(url)
        if "animenewsnetwork.com" not in parsed.netloc:
            return None
        if not parsed.path.endswith("/encyclopedia/anime.php"):
            # Also accept direct 'encyclopedia/anime.php' without leading slash variants
            if "encyclopedia/anime.php" not in parsed.path:
                return None
        qs = parse_qs(parsed.query)
        if "id" in qs and qs["id"]:
            return int(qs["id"][0])
    except Exception:
        return None
    return None


def jikan_ann_id_for_mal(mal_id: int) -> int | None:
    """Use Jikan external links to find ANN id for a MAL anime id."""
    data = jikan_get(f"/anime/{mal_id}/external")
    if not data or "data" not in data:
        return None
    for entry in data["data"]:
        url = entry.get("url") or ""
        ann_id = extract_ann_id_from_url(url)
        if ann_id:
            return ann_id
    return None


def parse_ann_batch_xml(xml_text: str) -> tuple[dict[int, set[str]], set[int]]:
    """
    Parse ANN XML and return (lang_map, present_ids) where:
      - lang_map: {ann_id: set(lang_keys)} inferred from <cast lang="...">
      - present_ids: set of ANN IDs that appeared in the response (even if no cast/dubs reported)
    """
    result: dict[int, set[str]] = {}
    present_ids: set[int] = set()
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        log(f"  Failed to parse ANN XML batch: {e}")
        return result, present_ids

    # ANN root tends to be <ann>
    for anime in root.findall(".//anime"):
        try:
            ann_id = int(anime.get("id"))
            present_ids.add(ann_id)
        except Exception:
            continue

        langs = set()

        # only languages attached to cast entries (voice acting)
        for cast in anime.findall(".//cast"):
            code = cast.get("lang")
            if not code:
                continue
            key = ann_lang_to_key(code)
            if key:
                langs.add(key)

        if langs:
            result[ann_id] = langs

    return result, present_ids


def process_ann_batch(ann_ids: list[int], ann_to_mal: dict[int, int]) -> set[int]:
    """
    Fetch a batch of ANN IDs and add MAL IDs to json_data by language.
    Also updates ann_mapping (mal_id -> ann_id) and returns MAL IDs present in ANN response.
    """
    if not ann_ids:
        return set()

    # Build ANN batch URL: title=ID1/ID2/... (trailing slash is fine)
    ids_part = "/".join(str(i) for i in ann_ids) + "/"
    url = f"{ANN_API}?title={ids_part}"

    xml_text = ann_get(url)
    if not xml_text:
        return set()

    ann_langs, present_ann_ids = parse_ann_batch_xml(xml_text)

    checked_ok_mals: set[int] = set()
    for ann_id in present_ann_ids:
        mal_id = ann_to_mal.get(ann_id)
        if mal_id:
            checked_ok_mals.add(int(mal_id))

    for ann_id, langs in ann_langs.items():
        mal_id = ann_to_mal.get(ann_id)
        if not mal_id:
            continue
        # Record mapping
        ann_mapping[int(mal_id)] = int(ann_id)
        for key in langs:
            json_data[key].add(int(mal_id))

    return checked_ok_mals


# ----------------------
# MALSync helpers
# ----------------------

def malsync_get(mal_id: int) -> dict | None:
    """Fetch MALSync mapping for a MAL anime ID. Returns dict or None on 404."""
    global malsync_last_call
    now = time.time()
    to_wait = MALSYNC_MIN_INTERVAL - (now - malsync_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    malsync_last_call = time.time()

    url = f"{MALSYNC_BASE}/{mal_id}"
    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            resp = requests.get(url, headers={"Accept": "application/json"})
            if resp.status_code == 404:
                log(f"[MALSync] 404 for MAL {mal_id}")
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exception = e
            print(f"[MALSync] Attempt {attempt + 1} failed for {mal_id}: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"[MALSync] Retrying in {delay} seconds...")
                time.sleep(delay)
    print(f"[MALSync] All {CALL_RETRIES} attempts failed for MAL {mal_id}")
    raise last_exception


def extract_sites_from_malsync_payload(payload: dict) -> dict[str, str]:
    """
    From MALSync payload, extract a provider -> FULL URL mapping.
    If multiple entries exist for a provider, pick the one with the shortest URL string.
    """
    out: dict[str, str] = {}
    sites = (payload or {}).get("Sites") or {}
    if not isinstance(sites, dict):
        return out

    for provider_name, entries in sites.items():
        if not isinstance(entries, dict) or not entries:
            continue

        best_url = None
        # Choose a stable "best" URL: shortest string length (usually canonical)
        for _key, obj in entries.items():
            if not isinstance(obj, dict):
                continue
            url = obj.get("url")
            if not url or not isinstance(url, str):
                continue
            if best_url is None or len(url) < len(best_url):
                best_url = url

        if best_url:
            out[provider_name] = best_url

    return out


# ----------------------
# Runners
# ----------------------

def run_ann(mal_start: int, mal_end: int):
    pending: list[int] = []
    ann_to_mal: dict[int, int] = {}
    processed = 0
    checked_ok_ids: set[int] = set()

    try:
        for mal_id in range(mal_start, mal_end + 1):
            ann_id = jikan_ann_id_for_mal(mal_id)
            if ann_id:
                pending.append(ann_id)
                ann_to_mal[ann_id] = mal_id
                # Also record mapping immediately
                ann_mapping[int(mal_id)] = int(ann_id)

            if len(pending) >= ANN_BATCH_SIZE:
                if debug_log:
                    log(f"ANN batch {processed // ANN_BATCH_SIZE + 1}: ids={pending[:3]}... (+{len(pending)-3} more)")
                newly_checked = process_ann_batch(pending, ann_to_mal)
                checked_ok_ids.update(newly_checked)
                processed += len(pending)
                pending.clear()

            if processed and processed % (FINALIZE_EVERY_N) == 0:
                finalize_jsons("ann", checked_ok_ids)

        # flush remainder
        if pending:
            newly_checked = process_ann_batch(pending, ann_to_mal)
            checked_ok_ids.update(newly_checked)

    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error (ANN): {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("ann", checked_ok_ids)
        print("Done (ANN).")


def run_mal(client_id: str, start_id: int, end_id: int):
    global missing_mal_ids, largest_known_mal_id

    # Load cache + largest known MAL ID once per run
    missing_mal_ids = load_missing_cache()
    largest_known_mal_id = get_largest_known_mal_id()
    if debug_log:
        log(f"[cache] largest_known_mal_id={largest_known_mal_id or 0}")

    MAX_CONSECUTIVE_404 = 500
    consecutive_404 = 0
    checked_ok_ids: set[int] = set()
    try:
        for idx, mal_id in enumerate(range(start_id, end_id + 1), 1):
            # Skip using cache only if ID is <= largest_known_mal_id
            if largest_known_mal_id and mal_id <= largest_known_mal_id and mal_id in missing_mal_ids:
                if debug_log:
                    log(f"  Skipping MAL ID {mal_id} (cached 404)")
                was_404 = True
                # Do NOT mark as checked_ok; we didn't actually check it this run
            else:
                was_404 = process_anime_mal(mal_id, client_id)
                # mark as successfully checked this run (even if 404)
                checked_ok_ids.add(mal_id)
                # If MAL anime returned 404, record to cache
                if was_404 and mal_id not in missing_mal_ids:
                    missing_mal_ids.add(mal_id)

            if was_404:
                consecutive_404 += 1
                if debug_log:
                    log(f"  Consecutive MAL 404s: {consecutive_404}")
                if consecutive_404 >= MAX_CONSECUTIVE_404:
                    print(f"[MAL] Hit {MAX_CONSECUTIVE_404} consecutive 404s at MAL ID {mal_id}. "
                          f"Assuming end-of-range and stopping early.")
                    break
            else:
                consecutive_404 = 0

            if idx % FINALIZE_EVERY_N == 0:
                log(f"--- Updating files at MAL ID {start_id + idx - 1} ---")
                finalize_jsons("mal", checked_ok_ids)
                save_missing_cache()
    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("mal", checked_ok_ids)
        save_missing_cache()
        print("Done (MAL).")


def run_anilist(start_page: int | None, end_page: int | None):
    page = start_page or 1
    total_processed = 0
    checked_ok_ids: set[int] = set()

    try:
        while True:
            log(f"AniList Page {page}")
            has_next, processed, checked_ids = process_anilist_page(page)
            checked_ok_ids.update(checked_ids)
            total_processed += processed

            if page % 10 == 0:
                finalize_jsons("anilist", checked_ok_ids)

            if end_page is not None and page >= end_page:
                break

            if not has_next:
                break

            page += 1
            time.sleep(ANILIST_PAGE_SLEEP)
    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error (AniList): {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("anilist", checked_ok_ids)
        print(f"Done (AniList). Processed ~{total_processed} media items.")

    log(f"AniList totals: pages={anilist_stats['pages']}, media={anilist_stats['media_total']}, "
        f"with_mal={anilist_stats['media_with_mal']}, no_mal={anilist_stats['media_without_mal']}, "
        f"with_langs={anilist_stats['media_with_langs']}, no_langs={anilist_stats['media_without_langs']}")


def run_malsync(mal_start: int, mal_end: int):
    """
    New mode:
      - For each MAL ID in range, fetch MALSync mapping JSON.
      - Extract provider -> FULL URL map.
      - Also extract `total` (episodes) and `anidbId`.
      - Update JSONL at dubs/mappings/mappings_malsync.jsonl (overwrite with merged content).
      - Respect missing_mal_ids.json for skipping known missing MAL IDs (from MAL 404s).
        NOTE: We do NOT add to missing_mal_ids when MALSync returns 404.
    """
    global missing_mal_ids, largest_known_mal_id
    missing_mal_ids = load_missing_cache()
    largest_known_mal_id = get_largest_known_mal_id()
    if debug_log:
        log(f"[MALSync] largest_known_mal_id={largest_known_mal_id or 0}, cached_missing={len(missing_mal_ids)}")

    # Load existing JSONL to update/merge instead of blindly appending.
    # Each record: {"mal_id": <int>, "sites": {...}, "total": <int?>, "anidbId": <int?>}
    master_map = load_jsonl_map(MALSYNC_JSONL_PATH)

    try:
        for idx, mal_id in enumerate(range(mal_start, mal_end + 1), 1):
            # Skip known missing MAL IDs (based on MAL API 404s, within known range)
            if largest_known_mal_id and mal_id <= largest_known_mal_id and mal_id in missing_mal_ids:
                if debug_log:
                    log(f"[MALSync] Skip MAL {mal_id} (cached missing MAL ID)")
                if idx % FINALIZE_EVERY_N == 0:
                    save_jsonl_map(MALSYNC_JSONL_PATH, master_map)
                    merge_all_mappings()
                continue

            data = malsync_get(mal_id)
            if not data:
                # No mapping for this MAL ID (do not record as missing MAL).
                # Mark as processed with empty sites but keep existing total/anidbId if any.
                rec = master_map.get(mal_id, {})
                rec["sites"] = rec.get("sites", {})
                master_map[mal_id] = rec
                if debug_log:
                    log(f"[MALSync] No mapping for MAL {mal_id}")
                if idx % FINALIZE_EVERY_N == 0:
                    save_jsonl_map(MALSYNC_JSONL_PATH, master_map)
                    merge_all_mappings()
                continue

            sites_map = extract_sites_from_malsync_payload(data)
            total = data.get("total")
            anidb_id = data.get("anidbId")

            rec = master_map.get(mal_id, {})
            if isinstance(total, int):
                rec["total"] = total
            if isinstance(anidb_id, int):
                rec["anidbId"] = anidb_id
            rec["sites"] = sites_map if sites_map else {}
            master_map[mal_id] = rec

            if debug_log:
                preview = dict(list(rec.get("sites", {}).items())[:5])
                log(f"[MALSync] MAL {mal_id}: sites={len(rec.get('sites', {}))}, total={rec.get('total')}, anidbId={rec.get('anidbId')}, sample={preview}")

            if idx % FINALIZE_EVERY_N == 0:
                log(f"[MALSync] --- Updating JSONL at MAL ID {mal_start + idx - 1} ---")
                save_jsonl_map(MALSYNC_JSONL_PATH, master_map)
                merge_all_mappings()

    except KeyboardInterrupt:
        print("\nInterrupted. Saving mappings...")
    except Exception as e:
        print(f"\nUnexpected error (MALSync): {e}")
        import traceback
        traceback.print_exc()
    finally:
        save_jsonl_map(MALSYNC_JSONL_PATH, master_map)
        merge_all_mappings()
        print("Done (MALSync).")


# ----------------------
# Main
# ----------------------

def main():
    global debug_log

    parser = argparse.ArgumentParser(
        description=(
            "Fetch dubbed languages per anime and save to JSON with safe, incremental removals "
            "(only for IDs checked this run). Also writes per-API mappings and a merged mapping JSONL."
        )
    )
    parser.add_argument("--api", choices=["mal", "anilist", "ann", "malsync"], default="mal", help="Which source to use.")
    parser.add_argument("--debug", default="false", help="Enable verbose logging (true/false).")

    # MAL-specific
    parser.add_argument("--client-id", help="MyAnimeList API Client ID (required for --api mal).")
    parser.add_argument("--mal-start", type=int, help="Start MAL ID (inclusive) for --api mal/ann/malsync.")
    parser.add_argument("--mal-end", type=int, help="End MAL ID (inclusive) for --api mal/ann/malsync.")

    # AniList-specific
    parser.add_argument("--anilist-start-page", type=int, help="AniList: start page number (1-based).")
    parser.add_argument("--anilist-end-page", type=int, help="AniList: end page number (inclusive).")
    parser.add_argument("--anilist-check-pages", default="false",
                        help="AniList: if true, prints total pages (perPage=50) and exits.")

    args = parser.parse_args()
    debug_log = str(args.debug).lower() == "true"

    # Ensure output dirs exist up-front
    os.makedirs(SOURCES_DIR, exist_ok=True)
    os.makedirs(MAPPINGS_DIR, exist_ok=True)

    if args.api == "mal":
        if not args.client_id or args.mal_start is None or args.mal_end is None:
            print("For --api mal you must provide --client-id, --mal-start, and --mal-end.")
            sys.exit(1)
        run_mal(args.client_id, args.mal_start, args.mal_end)

    elif args.api == "anilist":
        if str(args.anilist_check_pages).lower() == "true":
            try:
                pages = anilist_total_pages(ANILIST_PER_PAGE)
                print(pages)
                return
            except Exception as e:
                print(f"Failed to fetch AniList total pages: {e}")
                sys.exit(2)
        run_anilist(args.anilist_start_page, args.anilist_end_page)

    elif args.api == "ann":
        if args.mal_start is None or args.mal_end is None:
            print("For --api ann you must provide --mal-start and --mal-end.")
            sys.exit(1)
        run_ann(args.mal_start, args.mal_end)

    else:  # malsync
        if args.mal_start is None or args.mal_end is None:
            print("For --api malsync you must provide --mal-start and --mal-end.")
            sys.exit(1)
        run_malsync(args.mal_start, args.mal_end)


if __name__ == "__main__":
    main()
