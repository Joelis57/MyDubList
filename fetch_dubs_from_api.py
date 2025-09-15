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
KITSU_BASE = "https://kitsu.io/api/edge"
KITSU_TOKEN_URL = "https://kitsu.io/api/oauth/token"

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
KITSU_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_kitsu.jsonl")
MERGED_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_merged.jsonl")
HIANIME_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_hianime.jsonl")
ANISEARCH_MAPPING_JSONL = os.path.join(MAPPINGS_DIR, "mappings_anisearch.jsonl")

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

HIANIME_MIN_INTERVAL = 1.0  # seconds
hianime_last_call = 0.0

# Kitsu throttle
KITSU_MIN_INTERVAL = 1.0
kitsu_last_call = 0.0
kitsu_auth_header = None

missing_mal_ids = set()
largest_known_mal_id = 0  # determined from dubs/sources/automatic_mal/dubbed_japanese.json

# Per-API mappings accumulated this run
anilist_mapping: dict[int, int] = {}
ann_mapping: dict[int, int] = {}
kitsu_mapping: dict[int, int] = {}
hianime_mapping: dict[int, str] = {}
anisearch_mapping: dict[int, int] = {}


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

    # Brazilian (and common misspelling) → Portuguese
    if s.startswith("brazil") or "brazilian" in s or "brazillian" in s:
        return "portuguese"

    # Portuguese variants → "portuguese"
    if s.startswith("portuguese"):
        return "portuguese"

    # Mandarin → Chinese (unify MAL + AniList + Kitsu)
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

def load_simple_jsonl_map(path: str, value_key: str) -> dict[int, object]:
    if not os.path.exists(path):
        return {}
    result: dict[int, object] = {}
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
                try:
                    if isinstance(mid, str) and mid.isdigit():
                        mid = int(mid)
                    if isinstance(val, (int, float)):
                        val = int(val)
                    elif isinstance(val, str) and val.isdigit():
                        val = int(val)
                    # else: leave non-numeric strings untouched
                except Exception:
                    pass
                if isinstance(mid, int) and (isinstance(val, int) or isinstance(val, str)):
                    result[mid] = val
    except Exception as e:
        print(f"[map] Failed to load {path}: {e}")
    return result


def save_simple_jsonl_map(path: str, mapping: dict[int, int | str | None], value_key: str):
    _ensure_dir_for(path)
    try:
        existing = load_simple_jsonl_map(path, value_key)

        for k, v in mapping.items():
            mid = int(k)
            if v is None:
                if mid in existing:
                    existing.pop(mid, None)
                continue

            if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
                v = int(v)

            existing[mid] = v

        with open(path, "w", encoding="utf-8") as f:
            for mid in sorted(existing.keys(), key=int):
                val = existing[mid]
                rec = {"mal_id": int(mid), value_key: val}
                f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[map] Wrote {len(existing)} lines to {path}")
    except Exception as e:
        print(f"[map] Failed to save {path}: {e}")


def save_simple_jsonl_map_overwrite(path: str, mapping: dict[int, int], value_key: str):
    """Overwrite JSONL file with mapping exactly as provided."""
    _ensure_dir_for(path)
    try:
        with open(path, "w", encoding="utf-8") as f:
            for mid in sorted(mapping.keys(), key=int):
                rec = {"mal_id": int(mid), value_key: int(mapping[mid])}
                f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[map] Overwrote {path} with {len(mapping)} mappings")
    except Exception as e:
        print(f"[map] Failed to overwrite {path}: {e}")


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
                try:
                    if isinstance(mid, str) and str(mid).isdigit():
                        mid = int(mid)
                except Exception:
                    pass
                if isinstance(mid, int):
                    rec = {k: v for k, v in obj.items() if k != "mal_id"}
                    if "sites" in rec and not isinstance(rec["sites"], dict):
                        rec["sites"] = {}
                    result[mid] = rec
    except Exception as e:
        print(f"[jsonl] Failed to load JSONL '{path}': {e}")
    return result


def save_jsonl_map(path: str, mapping: dict[int, dict]):
    """Write the entire mapping dict to JSONL, sorted by mal_id."""
    _ensure_dir_for(path)
    try:
        with open(path, "w", encoding="utf-8") as f:
            for mid in sorted(mapping.keys(), key=int):
                rec = mapping[mid] if isinstance(mapping[mid], dict) else {}
                out_line = {"mal_id": int(mid), **rec}
                f.write(json.dumps(out_line, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[jsonl] Wrote {len(mapping)} lines to {path}")
    except Exception as e:
        print(f"[jsonl] Failed to save JSONL '{path}': {e}")


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

    # Kitsu
    ki_map = load_simple_jsonl_map(KITSU_MAPPING_JSONL, "kitsu_id")
    for mid, kid in ki_map.items():
        master.setdefault(mid, {})["kitsu_id"] = kid

    # HiAnime
    hi_map = load_simple_jsonl_map(HIANIME_MAPPING_JSONL, "hianime_id")
    for mid, hiid in hi_map.items():
        master.setdefault(mid, {})["hianime_id"] = hiid

    # aniSearch
    as_map = load_simple_jsonl_map(ANISEARCH_MAPPING_JSONL, "anisearch_id")
    for mid, asid in as_map.items():
        master.setdefault(mid, {})["anisearch_id"] = asid

    # Write merged
    _ensure_dir_for(MERGED_MAPPING_JSONL)
    try:
        with open(MERGED_MAPPING_JSONL, "w", encoding="utf-8") as f:
            for mid in sorted(master.keys(), key=int):
                line = {"mal_id": int(mid), **master[mid]}
                f.write(json.dumps(line, ensure_ascii=False, separators=(",", ":")) + "\n")
        log(f"[merge] Wrote merged mappings: {len(master)} to {MERGED_MAPPING_JSONL}")
    except Exception as e:
        print(f"[merge] Failed to write merged mappings: {e}")


# ----------------------
# MAL + Jikan helpers
# ----------------------

class TransientJikanError(Exception):
    pass

@lru_cache(maxsize=MAX_IN_MEMORY_CACHE)
def get_anime_roles_for_va_cached(person_id):
    data = jikan_get(f"/people/{person_id}/voices")
    if data is None:
        return None
    if not isinstance(data, dict) or "data" not in data or not isinstance(data["data"], list):
        raise TransientJikanError(f"Malformed VA payload for person {person_id}")  # don't cache
    return data


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
            resp = requests.get(url, headers=headers, timeout=20)

            if resp.status_code == 404:
                last_mal_404 = True
                log("  404 Anime Not Found, skipping")
                return None

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                try:
                    delay = int(ra) if ra is not None else RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                except Exception:
                    delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                print(f"  MAL 429. Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)
                continue

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError("MAL returned non-object JSON")

            last_mal_404 = False
            return data

        except (requests.RequestException, ValueError) as e:
            last_exception = e
            print(f"  Attempt {attempt + 1} failed: {e}", flush=True)
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"  MAL API call failed. Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)

    print(f"  All {CALL_RETRIES} attempts failed for {url}", flush=True)
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
            resp = requests.get(JIKAN_BASE + url, timeout=20)

            if resp.status_code == 404:
                log("    404 Not Found, skipping")
                return None

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                try:
                    delay = int(ra) if ra is not None else RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                except Exception:
                    delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                print(f"    429 Too Many Requests. Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)
                continue

            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, dict) or "data" not in data:
                raise TransientJikanError(f"Malformed JSON for {url}")

            return data

        except (requests.RequestException, ValueError, TransientJikanError) as e:
            last_exception = e
            print(f"    Attempt {attempt + 1} failed: {e}", flush=True)
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"    Jikan API call failed. Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)

    print(f"    All {CALL_RETRIES} attempts failed for {url}", flush=True)
    raise last_exception


def get_characters(mal_id, client_id):
    return mal_get(f"{MAL_BASE}/anime/{mal_id}/characters?limit=1", client_id)


def get_voice_actors(char_id):
    return jikan_get(f"/characters/{char_id}/voices")


def process_anime_mal(mal_id: int, client_id: str) -> bool | None:
    """
    Process a single MAL anime id.

    Returns:
      True  -> MAL anime 404 (permanent; safe to cache & allow removals)
      False -> processed successfully (reliable result, whether dubbed or not)
      None  -> transient failure (do NOT mark as checked; prevents accidental removals)
    """
    log(f"Processing MAL ID: {mal_id}")
    characters = get_characters(mal_id, client_id)

    # If the MAL characters call 404'd, short-circuit as a permanent miss
    if 'last_mal_404' in globals() and last_mal_404:
        return True  # was_404

    # If we got a valid response but no characters, treat as processed (not transient)
    if not characters or "data" not in characters or not characters["data"]:
        log("  No characters found, skipping")
        return False

    first_char = characters["data"][0]["node"]
    char_id = first_char["id"]

    # Fetch voice actors for the first character
    try:
        voice_actors = get_voice_actors(char_id)
    except Exception as e:
        log(f"  Transient error fetching character voices for char {char_id}: {e}")
        return None

    # Treat missing/invalid VA payload as transient to avoid false negatives
    if voice_actors is None or "data" not in voice_actors or not isinstance(voice_actors["data"], list):
        return None

    had_transient = False

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

        # Look up this VA's anime roles (cached), but DO NOT cache malformed/empty results
        try:
            va_roles = get_anime_roles_for_va_cached(person_id)
        except TransientJikanError as e:
            log(f"    Transient VA fetch failed for person {person_id}: {e}")
            had_transient = True
            continue
        except Exception as e:
            log(f"    Unexpected error fetching VA {person_id}: {e}")
            had_transient = True
            continue

        if not va_roles or "data" not in va_roles or not isinstance(va_roles["data"], list):
            # Either 404 (None) or empty list: nothing to add; not transient here
            continue

        # Confirm this VA actually voiced this MAL anime; if yes, record the language
        for role_entry in va_roles["data"]:
            anime = role_entry.get("anime")
            if anime and anime.get("mal_id") == mal_id:
                json_data[lang_key].add(int(mal_id))
                break

    # If any transient errors happened for any VA, mark whole anime as transient
    return None if had_transient else False


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
            "dubbed": sorted((int(x) for x in updated_ids), key=int),
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    # Also finalize per-API mappings and the merged mapping
    if api_mode == "anilist" and anilist_mapping:
        save_simple_jsonl_map(ANILIST_MAPPING_JSONL, anilist_mapping, "anilist_id")
    if api_mode == "ann" and ann_mapping:
        save_simple_jsonl_map(ANN_MAPPING_JSONL, ann_mapping, "ann_id")
    if api_mode == "kitsu" and kitsu_mapping:
        save_simple_jsonl_map(KITSU_MAPPING_JSONL, kitsu_mapping, "kitsu_id")
    if api_mode == "hianime" and hianime_mapping:

        save_simple_jsonl_map(HIANIME_MAPPING_JSONL, hianime_mapping, "hianime_id")

    # Always refresh merged mappings after a finalize
    merge_all_mappings()


def write_dubbed_files_overwrite(api_mode: str, per_lang_ids: dict[str, set[int]]):
    """Completely overwrite dubbed_* files for a given api_mode with the provided data.
    Removes any language files that are not present in per_lang_ids.
    """
    output_dir = os.path.join(SOURCES_DIR, f"automatic_{api_mode}")
    os.makedirs(output_dir, exist_ok=True)

    # Remove obsolete files
    existing = []
    try:
        existing = [fn for fn in os.listdir(output_dir) if fn.startswith("dubbed_") and fn.endswith(".json")]
    except FileNotFoundError:
        pass

    new_langs = set(per_lang_ids.keys())
    for fn in existing:
        lang_key = fn[len("dubbed_"):-len(".json")].replace("_", " ")
        if lang_key not in new_langs:
            try:
                os.remove(os.path.join(output_dir, fn))
                log(f"[anisearch] Removed obsolete file {fn}")
            except Exception as e:
                print(f"[anisearch] Failed to remove {fn}: {e}")

    # Write new files
    for lang_key in sorted(new_langs):
        fname_lang = filename_for_lang(lang_key)
        filename = os.path.join(output_dir, f"dubbed_{fname_lang}.json")
        ids_sorted = sorted((int(x) for x in per_lang_ids.get(lang_key, set())), key=int)
        obj = {
            "_license": "CC BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
            "_attribution": "MyDubList - https://mydublist.com - (CC BY 4.0)",
            "_origin": "https://github.com/Joelis57/MyDubList",
            "language": lang_key.capitalize(),
            "dubbed": ids_sorted,
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)


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
        parsed = urlparse(url)
        if "animenewsnetwork.com" not in parsed.netloc:
            return None
        if not parsed.path.endswith("/encyclopedia/anime.php"):
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
# HiAnime helpers
# ----------------------

def _slug_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        parts = [seg for seg in parsed.path.split('/') if seg]
        return parts[-1] if parts else ""
    except Exception:
        return ""

def build_hianime_slug_to_mal_map(source_file: str) -> dict[str, int]:
    if not source_file or not os.path.exists(source_file):
        print(f"[HiAnime] Source file not found: {source_file}")
        return {}

    try:
        with open(source_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[HiAnime] Failed to parse source file '{source_file}': {e}")
        return {}

    slug_to_mal: dict[str, int] = {}

    if isinstance(data, dict):
        for k, v in data.items():
            try:
                mal_id = int(k)
            except Exception:
                continue
            if not isinstance(v, dict):
                continue
            sites = v.get("sites") or {}
            if not isinstance(sites, dict):
                continue

            chosen_url = None
            for prov, url in sites.items():
                if not isinstance(url, str):
                    continue
                prov_lower = str(prov).lower()
                if prov_lower == "zoro" or "hianime." in url:
                    chosen_url = url
                    if prov_lower == "zoro":
                        break

            if not chosen_url:
                continue

            slug = _slug_from_url(chosen_url)
            if slug:
                slug_to_mal[slug] = mal_id

    log(f"[HiAnime] Built slug→MAL map for {len(slug_to_mal)} entries from source file.")
    return slug_to_mal

def hianime_get_page(api_host: str, page: int) -> dict | None:
    """
    Throttled GET for HiAnime (aniwatch) dubbed-anime category page.
    """
    global hianime_last_call
    now = time.time()
    to_wait = HIANIME_MIN_INTERVAL - (now - hianime_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    hianime_last_call = time.time()

    url = f"{api_host.rstrip('/')}/api/v2/hianime/category/dubbed-anime?page={page}"
    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            resp = requests.get(url, headers={"Accept": "application/json"})
            if resp.status_code == 404:
                log(f"[HiAnime] 404 on page {page}")
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exception = e
            print(f"[HiAnime] Attempt {attempt + 1} failed for page {page}: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"[HiAnime] Retrying in {delay} seconds...")
                time.sleep(delay)
    print(f"[HiAnime] All {CALL_RETRIES} attempts failed for page {page}")
    raise last_exception


# ----------------------
# Kitsu helpers
# ----------------------

def kitsu_login(email: str, password: str) -> str | None:
    payload = {
        "grant_type": "password",
        "username": email,
        "password": password,
    }
    try:
        r = requests.post(KITSU_TOKEN_URL, data=payload, timeout=20)
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if token:
            return token
    except Exception as e:
        print(f"[Kitsu] Login failed: {e}")
    return None


def kitsu_get(url: str) -> dict | None:
    """Throttled GET for Kitsu JSON:API."""
    global kitsu_last_call
    now = time.time()
    to_wait = KITSU_MIN_INTERVAL - (now - kitsu_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    kitsu_last_call = time.time()

    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            headers = {"Accept": "application/vnd.api+json"}
            if kitsu_auth_header:
                headers.update(kitsu_auth_header)
            if debug_log:
                log(f"[Kitsu] GET {url}")
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code in (401, 403, 404):
                log(f"[Kitsu] {resp.status_code} for {url} (skipping)")
                return None
            if resp.status_code == 429:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                print(f"  Kitsu 429. Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exception = e
            print(f"  Kitsu attempt {attempt + 1} failed: {e}")
            if attempt < CALL_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"  Kitsu call failed. Retrying in {delay} seconds...")
                time.sleep(delay)
    print("  All Kitsu attempts failed.")
    raise last_exception


def kitsu_find_kitsu_id_by_mal(mal_id: int) -> int | None:
    """Use /mappings to find the Kitsu anime id by MAL id."""
    url = (f"{KITSU_BASE}/mappings?"
           f"filter[externalSite]=myanimelist/anime&filter[externalId]={int(mal_id)}&include=item")
    data = kitsu_get(url)
    if not data or "data" not in data:
        return None
    incl = {x.get("id"): x for x in data.get("included", []) if isinstance(x, dict)}
    for m in data.get("data") or []:
        attrs = m.get("attributes") or {}
        if attrs.get("externalSite") == "myanimelist/anime" and str(attrs.get("externalId")) == str(mal_id):
            rel = m.get("relationships", {}).get("item", {}).get("data") or {}
            kid = rel.get("id")
            try:
                return int(kid)
            except Exception:
                # try included
                itm = incl.get(kid) if kid else None
                if itm and itm.get("type") == "anime":
                    try:
                        return int(itm.get("id"))
                    except Exception:
                        pass
    return None


def kitsu_lang_to_key(raw: str) -> str:
    """Map Kitsu casting language to our normalized key."""
    if not raw:
        return ""
    iso_try = ann_lang_to_key(raw)
    return sanitize_lang(iso_try or raw)


def kitsu_list_languages(kitsu_anime_id: int) -> set[str]:
    url = f"{KITSU_BASE}/anime/{int(kitsu_anime_id)}/_languages"
    data = kitsu_get(url)
    langs: set[str] = set()
    if isinstance(data, list):
        for raw in data:
            key = kitsu_lang_to_key(str(raw))
            if key:
                langs.add(key)
    return langs

# ----------------------
# aniSearch helpers
# ----------------------

def load_anisearch_source(source_file: str) -> tuple[dict[str, set[int]], dict[int, int]]:
    """Load a JSON mapping of MAL → { 'anisearch-id': str/int, 'dubbed': ["en", ...] }.
    Returns (per_language_ids, anisearch_mapping).
    """
    if not source_file or not os.path.exists(source_file):
        print(f"[aniSearch] Source file not found: {source_file}")
        return {}, {}

    try:
        with open(source_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[aniSearch] Failed to parse source file '{source_file}': {e}")
        return {}, {}

    per_lang: dict[str, set[int]] = defaultdict(set)
    mapping: dict[int, int] = {}

    if not isinstance(data, dict):
        print("[aniSearch] Source JSON must be an object keyed by MAL id")
        return {}, {}

    for mal_key, rec in data.items():
        try:
            mal_id = int(mal_key)
        except Exception:
            continue
        if not isinstance(rec, dict):
            continue
        as_id = rec.get("anisearch-id")
        try:
            if isinstance(as_id, str) and as_id.isdigit():
                as_id = int(as_id)
            elif isinstance(as_id, (int, float)):
                as_id = int(as_id)
            else:
                as_id = None
        except Exception:
            as_id = None
        if as_id:
            mapping[mal_id] = as_id

        # dubbed languages: ISO codes like ["ja","en",...]
        langs = rec.get("dubbed") or []
        if isinstance(langs, list):
            for code in langs:
                key = ann_lang_to_key(str(code))  # handles ISO codes
                key = sanitize_lang(key)
                if key:
                    per_lang[key].add(mal_id)

    return per_lang, mapping


# ----------------------
# Runners
# ----------------------

def run_ann(mal_start: int, mal_end: int):
    existing_map = load_simple_jsonl_map(ANN_MAPPING_JSONL, "ann_id")
    pending: list[int] = []
    ann_to_mal: dict[int, int] = {}
    processed = 0
    checked_ok_ids: set[int] = set()

    try:
        for mal_id in range(mal_start, mal_end + 1):
            try:
                found = jikan_ann_id_for_mal(mal_id)
            except Exception as e:
                log(f"[ANN] Transient Jikan failure for MAL {mal_id}: {e} (keeping existing mapping, skipping)")
                continue

            if found is not None:
                try:
                    found = int(found)
                except Exception:
                    found = None

            if found:
                prev = existing_map.get(mal_id)
                if prev != found:
                    log(f"[ANN] Mapping update MAL {mal_id}: {prev} -> {found}")
                ann_mapping[int(mal_id)] = int(found)
                ann_to_mal[int(found)] = int(mal_id)
                pending.append(int(found))
            else:
                # Successful Jikan call but no ANN link => mapping removal
                if mal_id in existing_map:
                    log(f"[ANN] Mapping removed for MAL {mal_id} (was {existing_map[mal_id]}), will delete")
                ann_mapping[int(mal_id)] = None
                checked_ok_ids.add(int(mal_id))

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
            # If we've previously cached that this ID 404s (and it's <= largest known),
            # skip calling the API. Do NOT mark as checked this run.
            if largest_known_mal_id and mal_id <= largest_known_mal_id and mal_id in missing_mal_ids:
                if debug_log:
                    log(f"  Skipping MAL ID {mal_id} (cached 404)")
                was_404 = True
            else:
                res = process_anime_mal(mal_id, client_id)

                if res is None:
                    was_404 = False
                    if debug_log:
                        log(f"  MAL ID {mal_id}: transient failure; deferring")
                else:
                    was_404 = bool(res)
                    checked_ok_ids.add(mal_id)

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


def run_hianime(api_host: str, start_page: int | None, end_page: int | None, source_file: str | None):
    if not source_file:
        print("For --api hianime you must provide --source-file pointing to a JSON/JSONL mapping file.")
        sys.exit(1)

    slug_to_mal = build_hianime_slug_to_mal_map(source_file)
    page = start_page or 1
    checked_ok_ids: set[int] = set()

    try:
        total_pages = None
        while True:
            log(f"[HiAnime] Page {page}")
            data = hianime_get_page(api_host, page)
            if not data or "data" not in data:
                break

            d = data["data"]
            animes = d.get("animes", []) or []
            total_pages = d.get("totalPages") or total_pages
            has_next = bool(d.get("hasNextPage"))
            current_page = d.get("currentPage", page)

            for item in animes:
                hi_id = item.get("id")
                eps = item.get("episodes") or {}
                sub_count = int(eps.get("sub", 0) or 0)
                dub_count = int(eps.get("dub", 0) or 0)

                if not hi_id:
                    continue

                if dub_count < 1:
                    continue

                mal_id = slug_to_mal.get(hi_id)
                if not mal_id:
                    continue

                json_data["english"].add(int(mal_id))
                checked_ok_ids.add(int(mal_id))

                hianime_mapping[int(mal_id)] = hi_id

            if current_page % 10 == 0:
                finalize_jsons("hianime", checked_ok_ids)

            if end_page is not None and page >= end_page:
                break
            if not has_next:
                break

            page += 1
            now = time.time()
            to_wait = HIANIME_MIN_INTERVAL - (time.time() - hianime_last_call)
            if to_wait > 0:
                time.sleep(to_wait)

    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error (HiAnime): {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("hianime", checked_ok_ids)
        print("Done (HiAnime).")


def run_kitsu(mal_start: int, mal_end: int):
    """
    Kitsu mode:
      - Load existing MAL→Kitsu mappings from JSONL.
      - For MAL IDs missing a mapping, query /mappings with externalSite=myanimelist/anime to discover Kitsu id.
        (If needed we can also confirm via /anime/{kitsu}/mappings to get MAL id.)
      - For each mapped Kitsu anime, fetch castings and record languages.
      - Save dubbed_* under dubs/sources/automatic_kitsu and update mappings_kitsu.jsonl.
    """
    existing_map = load_simple_jsonl_map(KITSU_MAPPING_JSONL, "kitsu_id")
    known_missing = load_missing_cache()
    checked_ok_ids: set[int] = set()
    processed = 0
    with_langs = 0
    without_langs = 0
    missing_map = 0

    try:
        for idx, mal_id in enumerate(range(mal_start, mal_end + 1), 1):
            if mal_id in known_missing:
                if debug_log:
                    log(f"[Kitsu] Skipping MAL {mal_id} (cached missing)")
                continue

            kid = existing_map.get(mal_id)
            if not kid:
                kid = kitsu_find_kitsu_id_by_mal(mal_id)
                if kid:
                    kitsu_mapping[int(mal_id)] = int(kid)
                    existing_map[int(mal_id)] = int(kid)
                    log(f"[Kitsu] Discovered mapping MAL {mal_id} -> Kitsu {kid}")
                else:
                    missing_map += 1
                    log(f"[Kitsu] MAL {mal_id}: no Kitsu ID found")
                    continue

            log(f"[Kitsu] MAL {mal_id} -> Kitsu {kid}: fetching languages")
            langs_found = kitsu_list_languages(kid)

            if langs_found:
                for key in langs_found:
                    json_data[key].add(int(mal_id))
                with_langs += 1
                log(f"[Kitsu] MAL {mal_id}: languages={sorted(langs_found)}")
            else:
                without_langs += 1
                log(f"[Kitsu] MAL {mal_id}: no languages found")

            checked_ok_ids.add(int(mal_id))
            processed += 1

            if idx % FINALIZE_EVERY_N == 0:
                log(f"[Kitsu] --- Updating files at MAL ID {mal_id} ---")
                finalize_jsons("kitsu", checked_ok_ids)

    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error (Kitsu): {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("kitsu", checked_ok_ids)
        log(f"[Kitsu] Summary: processed={processed}, with_langs={with_langs}, "
            f"without_langs={without_langs}, missing_map={missing_map}")
        print("Done (Kitsu).")


def run_anisearch(source_file: str):
    per_lang, mapping = load_anisearch_source(source_file)
    if not per_lang and not mapping:
        print("[aniSearch] Nothing to write (empty or invalid source).")
        return

    # Write dubbed files (overwrite mode)
    write_dubbed_files_overwrite("anisearch", per_lang)

    # Write mapping JSONL (overwrite)
    save_simple_jsonl_map_overwrite(ANISEARCH_MAPPING_JSONL, mapping, "anisearch_id")

    # Refresh merged mappings
    merge_all_mappings()

    total_ids = len({i for s in per_lang.values() for i in s})
    print(f"Done (aniSearch). Languages={len(per_lang)} unique MAL IDs={total_ids} mappings={len(mapping)}.")


# ----------------------
# Main
# ----------------------

def main():
    global debug_log, kitsu_auth_header

    parser = argparse.ArgumentParser(
        description=(
            "Fetch dubbed languages per anime and save to JSON with safe, incremental removals "
            "(only for IDs checked this run). Also writes per-API mappings and a merged mapping JSONL."
        )
    )
    parser.add_argument("--api", choices=["mal", "anilist", "ann", "hianime", "kitsu", "anisearch"], default="mal", help="Which source to use.")
    parser.add_argument("--debug", default="false", help="Enable verbose logging (true/false).")

    # MAL-specific
    parser.add_argument("--client-id", help="MyAnimeList API Client ID (required for --api mal).")

    parser.add_argument("--mal-start", type=int, help="Start MAL ID (inclusive) for --api mal/ann/kitsu.")
    parser.add_argument("--mal-end", type=int, help="End MAL ID (inclusive) for --api mal/ann/kitsu.")

    # Generic paging
    parser.add_argument("--start-page", type=int, help="Start page number (1-based) for AniList/HiAnime.")
    parser.add_argument("--end-page", type=int, help="End page number (inclusive) for AniList/HiAnime.")

    # AniList-specific
    parser.add_argument("--anilist-check-pages", default="false",
                        help="AniList: if true, prints total pages (perPage=50) and exits.")

    # API host
    parser.add_argument("--api-host", default="http://localhost:6969",
                        help="Base host for the aniwatch API (e.g., http://localhost:6969).")
    # Source file
    parser.add_argument("--source-file", help="Path to a JSON/JSONL mapping file.")

    # Authentication
    parser.add_argument("--email", help="Account email for OAuth (optional).")
    parser.add_argument("--password", help="Account password for OAuth (optional).")
    parser.add_argument("--token", help="Bearer token (optional, overrides email/password).")

    args = parser.parse_args()
    debug_log = str(args.debug).lower() == "true"

    # Ensure output dirs exist up-front
    os.makedirs(SOURCES_DIR, exist_ok=True)
    os.makedirs(MAPPINGS_DIR, exist_ok=True)

    # Kitsu auth setup
    if args.token:
        kitsu_auth_header = {"Authorization": f"Bearer {args.token}"}
        log("[Kitsu] Using provided token")
    elif args.email and args.password:
        token = kitsu_login(args.email, args.password)
        if token:
            kitsu_auth_header = {"Authorization": f"Bearer {token}"}
            log("[Kitsu] Logged in and obtained token")
        else:
            print("[Kitsu] Warning: proceeding without auth (NSFW may be hidden)")

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
        run_anilist(args.start_page, args.end_page)

    elif args.api == "ann":
        if args.mal_start is None or args.mal_end is None:
            print("For --api ann you must provide --mal-start and --mal-end.")
            sys.exit(1)
        run_ann(args.mal_start, args.mal_end)

    elif args.api == "hianime":
        run_hianime(args.api_host, args.start_page, args.end_page, args.source_file)

    elif args.api == "anisearch":
        if not args.source_file:
            print("For --api anisearch you must provide --source-file pointing to the aniSearch JSON file.")
            sys.exit(1)
        run_anisearch(args.source_file)

    else:  # kitsu
        if args.mal_start is None or args.mal_end is None:
            print("For --api kitsu you must provide --mal-start and --mal-end.")
            sys.exit(1)
        run_kitsu(args.mal_start, args.mal_end)


if __name__ == "__main__":
    main()
