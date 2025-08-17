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

# JustWatch wrapper (used only in --api justwatch mode)
# pip install simple-justwatch-python-api
try:
    from simplejustwatchapi.justwatch import search as jw_search, offers_for_countries as jw_offers_for_countries
except Exception:
    jw_search = None
    jw_offers_for_countries = None

# ======================
# CONFIG
# ======================
MAL_BASE = "https://api.myanimelist.net/v2"
JIKAN_BASE = "https://api.jikan.moe/v4"
ANILIST_BASE = "https://graphql.anilist.co"
ANN_API = "https://cdn.animenewsnetwork.com/encyclopedia/api.xml"

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

MISSING_CACHE_PATH = "cache/missing_mal_ids.json"
LARGEST_KNOWN_MAL_FILE = "final/dubbed_japanese.json"

# MAL -> JustWatch node id cache
MAL_JW_MAP_PATH = "cache/mal_justwatch_ids.json"

# JustWatch coverage — 20 high-coverage markets (keeps payloads small)
JW_COVERAGE = {
    # Americas
    "US", "CA", "MX", "BR", "AR",
    # Europe (West/North)
    "GB", "FR", "DE", "IT", "ES", "PT", "NL",
    # Europe (East / extra)
    "PL", "TR",
    # APAC
    "JP", "KR", "IN", "AU", "NZ",
}

# Trusted providers (filter offers to reduce false positives)
# NOTE: Amazon/Prime is intentionally NOT present and is hard-excluded below.
TRUSTED_PROVIDERS = {
    "netflix",
    "disneyplus",
    "hulu",
    "appletvplus",  # Apple TV+
    "itunes",       # allow iTunes storefront
    "crunchyroll",
    "max", "hbomax",
    "paramountplus",
    "peacock",
    "hidive",
}
# ======================

# Globals
jikan_last_call = 0
jikan_last_404 = False   # remember if last Jikan call was a 404
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

# JustWatch throttle
JW_MIN_INTERVAL = 1.05  # seconds
jw_last_call = 0.0

missing_mal_ids = set()
largest_known_mal_id = 0  # determined from final/dubbed_japanese.json

# MAL→JW map (in-memory).
mal_to_jw_map = {}  # str(mal_id) -> str(node_id)

def log(message: str):
    if debug_log:
        print(message)


# ----------------------
# Language normalization
# ----------------------
def sanitize_lang(lang: str) -> str:
    if not lang:
        return ""

    s = lang.strip().lower()

    if s.startswith("portuguese"):
        return "portuguese"

    if s.startswith("mandarin"):
        return "chinese"

    if s.startswith("filipino"):
        return "tagalog"

    s = re.sub(r"\(.*?\)", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def filename_for_lang(lang_key: str) -> str:
    return lang_key.replace(" ", "_")


def ann_lang_to_key(raw: str) -> str:
    if not raw:
        return ""
    r = raw.strip().upper()
    if r in {"ES-419", "ES-LA", "LATAM"}:
        return "spanish"
    if r in {"PT-BR", "BR"}:
        return "portuguese"
    if r in {"ZH", "ZH-CN", "ZH-TW", "ZH-HK", "CH", "CN"}:
        return "chinese"

    iso = {
        "EN": "english","FR":"french","DE":"german","HE":"hebrew","IW":"hebrew","HU":"hungarian",
        "IT":"italian","JA":"japanese","KO":"korean","PT":"portuguese","ES":"spanish","TL":"tagalog",
        "PL":"polish","RU":"russian","TR":"turkish","NL":"dutch","SV":"swedish","NO":"norwegian",
        "NB":"norwegian","NN":"norwegian","DA":"danish","FI":"finnish","CS":"czech","SK":"slovak",
        "RO":"romanian","BG":"bulgarian","UK":"ukrainian","UA":"ukrainian","EL":"greek","ID":"indonesian",
        "MS":"malay","VI":"vietnamese","TH":"thai","AR":"arabic","HI":"hindi","ET":"estonian","EU":"basque",
        "LT":"lithuanian","LV":"latvian","CA":"catalan","IS":"icelandic","MK":"macedonian","SL":"slovenian",
        "SR":"serbian","HR":"croatian","UR":"urdu","FA":"persian","BN":"bengali","TA":"tamil","TE":"telugu",
        "MR":"marathi","GU":"gujarati","PA":"punjabi",
    }
    if r in iso:
        return iso[r]
    return sanitize_lang(raw)


def jw_lang_to_key(raw: str) -> str:
    return ann_lang_to_key(raw)

# ----------------------
# Missing MAL IDs cache helpers
# ----------------------
def _ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def load_missing_cache() -> set[int]:
    if not os.path.exists(MISSING_CACHE_PATH):
        return set()
    try:
        with open(MISSING_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        ids = data.get("missing", data if isinstance(data, list) else [])
        out = set(int(x) for x in ids if isinstance(x, int) or (isinstance(x, str) and x.isdigit()))
        log(f"[cache] Loaded {len(out)} missing MAL IDs from {MISSING_CACHE_PATH}")
        return out
    except Exception as e:
        print(f"[cache] Failed to load {MISSING_CACHE_PATH}: {e}")
        return set()


def save_missing_cache():
    _ensure_dir_for(MISSING_CACHE_PATH)
    try:
        payload = {"missing": sorted(missing_mal_ids)}
        with open(MISSING_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"[cache] Saved {len(missing_mal_ids)} missing MAL IDs to {MISSING_CACHE_PATH}")
    except Exception as e:
        print(f"[cache] Failed to save {MISSING_CACHE_PATH}: {e}")


def get_largest_known_mal_id() -> int:
    path = LARGEST_KNOWN_MAL_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        arr = data.get("dubbed", [])
        if isinstance(arr, list) and arr:
            val = arr[-1]
            if isinstance(val, int):
                return val
            if isinstance(val, str) and val.isdigit():
                return int(val)
        return 0
    except Exception as e:
        log(f"[cache] Could not read largest known MAL ID from {path}: {e}")
        return 0


# MAL -> JW map helpers
def load_mal_jw_map() -> dict:
    if not os.path.exists(MAL_JW_MAP_PATH):
        return {}
    try:
        with open(MAL_JW_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("map", data if isinstance(data, dict) else {})
    except Exception as e:
        print(f"[cache] Failed to load {MAL_JW_MAP_PATH}: {e}")
        return {}


def save_mal_jw_map(mapping: dict):
    _ensure_dir_for(MAL_JW_MAP_PATH)
    try:
        with open(MAL_JW_MAP_PATH, "w", encoding="utf-8") as f:
            json.dump({"map": mapping}, f, ensure_ascii=False, indent=2)
        log(f"[cache] Saved {len(mapping)} MAL→JW mappings to {MAL_JW_MAP_PATH}")
    except Exception as e:
        print(f"[cache] Failed to save {MAL_JW_MAP_PATH}: {e}")


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
    global jikan_last_call, jikan_last_404
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
                jikan_last_404 = True
                log("    404 Not Found, skipping")
                return None
            response.raise_for_status()
            jikan_last_404 = False
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


def process_anilist_page(page: int):
    variables = {
        "page": page,
        "perPage": ANILIST_PER_PAGE,
        "charPerPage": ANILIST_CHAR_PER_PAGE,
    }
    data = anilist_post(ANILIST_QUERY, variables)
    if not data:
        return False, 0

    page_data = data["data"]["Page"]
    has_next = page_data["pageInfo"]["hasNextPage"]
    media_list = page_data.get("media", [])

    page_media_total = len(media_list)
    page_with_mal = 0
    page_without_mal = 0
    page_with_langs = 0
    page_without_langs = 0

    processed = 0

    for media in media_list:
        mal_id = media.get("idMal")
        if not mal_id:
            page_without_mal += 1
            if debug_log:
                log("  skip: no idMal")
            continue

        page_with_mal += 1

        chars = media.get("characters") or {}
        edges = chars.get("edges") or []

        if debug_log:
            if not edges:
                log("    No character edges returned.")
            else:
                empty_va_edges = sum(1 for e in edges if not (e.get("voiceActors") or []))
                log(f"    edges={len(edges)} empty_va_edges={empty_va_edges}")
                if empty_va_edges == len(edges) and len(edges) > 0:
                    import json as _json
                    log("    sample edge dump:")
                    try:
                        log(_json.dumps(edges[0], ensure_ascii=False, indent=2)[:2000])
                    except Exception:
                        pass

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

    anilist_stats["pages"] += 1
    anilist_stats["media_total"] += page_media_total
    anilist_stats["media_with_mal"] += page_with_mal
    anilist_stats["media_without_mal"] += page_without_mal
    anilist_stats["media_with_langs"] += page_with_langs
    anilist_stats["media_without_langs"] += page_without_langs

    return has_next, processed


# ----------------------
# Append-only finalize
# ----------------------
def finalize_jsons(api_mode: str):
    output_dir = f"automatic_{api_mode}"
    os.makedirs(output_dir, exist_ok=True)

    for lang_key, new_ids in json_data.items():
        fname_lang = filename_for_lang(lang_key)
        filename = os.path.join(output_dir, f"dubbed_{fname_lang}.json")

        if os.path.exists(filename):
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                    existing_ids = set(existing_data.get("dubbed", []))
            except Exception:
                existing_ids = set()
        else:
            existing_ids = set()

        updated_ids = existing_ids | set(new_ids)

        added_ids = sorted(updated_ids - existing_ids)
        if added_ids:
            log(f"  Changes in {filename}:")
            log(f"    Added: {added_ids}")

        obj = {
            "_license": "CC BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
            "_attribution": "MyDubList - https://mydublist.com - (CC BY 4.0)",
            "_origin": "https://github.com/Joelis57/MyDubList",
            "language": lang_key.capitalize(),
            "dubbed": sorted(updated_ids),
        }

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)


# ----------------------
# ANN helpers
# ----------------------
def ann_get(url):
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
    data = jikan_get(f"/anime/{mal_id}/external")
    if not data or "data" not in data:
        return None
    for entry in data["data"]:
        url = entry.get("url") or ""
        ann_id = extract_ann_id_from_url(url)
        if ann_id:
            return ann_id
    return None


def parse_ann_batch_xml(xml_text: str) -> dict[int, set[str]]:
    result: dict[int, set[str]] = {}
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        log(f"  Failed to parse ANN XML batch: {e}")
        return result

    for anime in root.findall(".//anime"):
        try:
            ann_id = int(anime.get("id"))
        except Exception:
            continue

        langs = set()
        for cast in anime.findall(".//cast"):
            code = cast.get("lang")
            if not code:
                continue
            key = ann_lang_to_key(code)
            if key:
                langs.add(key)

        if langs:
            result[ann_id] = langs

    return result


def process_ann_batch(ann_ids: list[int], ann_to_mal: dict[int, int]):
    if not ann_ids:
        return
    ids_part = "/".join(str(i) for i in ann_ids) + "/"
    url = f"{ANN_API}?title={ids_part}"
    xml_text = ann_get(url)
    if not xml_text:
        return

    ann_langs = parse_ann_batch_xml(xml_text)
    for ann_id, langs in ann_langs.items():
        mal_id = ann_to_mal.get(ann_id)
        if not mal_id:
            continue
        for key in langs:
            json_data[key].add(mal_id)


# ----------------------
# JustWatch helpers
# ----------------------
def _respect_jw_rps():
    global jw_last_call
    now = time.time()
    to_wait = JW_MIN_INTERVAL - (now - jw_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    jw_last_call = time.time()

def jw_search_rl(*args, **kwargs):
    _respect_jw_rps()
    return jw_search(*args, **kwargs)

def jw_offers_for_countries_rl(*args, **kwargs):
    _respect_jw_rps()
    return jw_offers_for_countries(*args, **kwargs)

def _norm_title(s: str) -> str:
    import unicodedata as _u
    s = _u.normalize("NFKC", s or "")
    return "".join(ch for ch in s if (_u.category(ch)[0] != "P" and not ch.isspace())).casefold()

# --- Season marker detection (very conservative)
_SEASON_RE = re.compile(
    r"\b("
    r"season\s*\d+|"
    r"\d+(?:st|nd|rd|th)\s*season|"
    r"final\s*season|"
    r"part\s*\d+|"
    r"s\d+\b"
    r")",
    re.IGNORECASE
)

def _has_season_marker_in_titles(raw_titles: list[str]) -> bool:
    for t in raw_titles:
        if t and _SEASON_RE.search(t):
            return True
    return False

def jikan_is_multi_season(mal_id: int) -> bool:
    """True if Jikan relations include Sequel/Prequel (treat as multi-season risk)."""
    d = jikan_get(f"/anime/{mal_id}/relations")
    if not d or "data" not in d:
        return False
    for rel in d["data"]:
        rel_type = (rel.get("relation") or "").strip().lower()
        if rel_type in {"sequel", "prequel"}:
            return True
    return False

def _collect_titles_from_jikan_data(d: dict):
    """Return (set_of_norm_titles, ordered_query_titles, raw_titles_list)."""
    titles_set, queries, raw_titles = set(), [], []
    if not d:
        return titles_set, queries, raw_titles

    seen = set()
    def add(t: str | None, priority=False):
        if not t:
            return
        if t in seen:
            return
        seen.add(t)
        raw_titles.append(t)
        titles_set.add(_norm_title(t))
        if priority:
            queries.append(t)

    for obj in (d.get("titles") or []):
        ttype = (obj.get("type") or "").lower()
        if ttype == "english":
            add(obj.get("title"), True)
    for obj in (d.get("titles") or []):
        ttype = (obj.get("type") or "").lower()
        if ttype in {"default", "synonym"}:
            add(obj.get("title"), True)
    for obj in (d.get("titles") or []):
        ttype = (obj.get("type") or "").lower()
        if ttype == "japanese":
            add(obj.get("title"), True)

    add(d.get("title_english"), True)
    add(d.get("title"), True)
    add(d.get("title_japanese"), True)
    for syn in (d.get("title_synonyms") or []):
        add(syn, True)

    queries = list(dict.fromkeys(queries))
    return titles_set, queries, raw_titles


def _map_mal_type_to_jw_type(mal_type: str | None) -> str | None:
    mt = (mal_type or "").upper()
    if mt in {"TV", "ONA", "OVA", "SPECIAL"}:
        return "show"
    if mt == "MOVIE":
        return "movie"
    return None


def _norm_jw_type(t) -> str | None:
    if t is None:
        return None
    t = str(t).strip().lower()
    if t in {"show", "series", "tv", "tv_show"}:
        return "show"
    if t in {"movie", "film"}:
        return "movie"
    return t


def _extract_year_from_aired(d: dict) -> int | None:
    aired = d.get("aired") or {}
    prop = aired.get("prop") or {}
    y = (prop.get("from") or {}).get("year")
    if isinstance(y, int):
        return y
    iso = aired.get("from")
    if isinstance(iso, str) and len(iso) >= 4 and iso[:4].isdigit():
        try:
            return int(iso[:4])
        except Exception:
            return None
    return None


def jikan_mal_meta_for_jw(mal_id: int):
    data = jikan_get(f"/anime/{mal_id}")
    if not data or "data" not in data:
        log(f"[JW] Jikan meta missing for MAL {mal_id}")
        return None
    d = data["data"]
    titles_set, queries, raw_titles = _collect_titles_from_jikan_data(d)
    year = d.get("year")
    if year is None:
        y2 = _extract_year_from_aired(d)
        if y2 is not None:
            year = y2
            log(f"[{mal_id}] Using fallback year from 'aired': {year}")
        else:
            log(f"[{mal_id}] No 'year' and could not derive from 'aired'.")
    jw_type = _map_mal_type_to_jw_type(d.get("type"))

    has_season_marker = _has_season_marker_in_titles(raw_titles)
    is_multi_season = jikan_is_multi_season(mal_id)  # extra Jikan call (throttled)

    if not titles_set:
        log(f"[{mal_id}] Jikan returned no titles for strict match.")
    if jw_type is None:
        log(f"[{mal_id}] MAL type '{d.get('type')}' not mappable to JW type.")
    return {
        "titles_set": titles_set,
        "queries": queries,
        "year": year,
        "jw_type": jw_type,
        "has_season_marker": has_season_marker,
        "is_multi_season": is_multi_season,
    }


def _jw_entry_has_ani_genre(e) -> bool:
    """
    Return True iff the JW search entry has the 'ani' genre code.
    JW often returns 3-letter genre codes like: act, scf, wsn, drm, ani, cmy.
    """
    raw_genres = getattr(e, "genres", None) or getattr(e, "genre", None) or []
    tokens = []
    for g in raw_genres:
        if isinstance(g, str):
            tokens.append(g.strip().lower())
        else:
            # Try common fields on genre objects
            for attr in ("short_name", "technical_name", "name"):
                val = getattr(g, attr, None)
                if val:
                    tokens.append(str(val).strip().lower())
    return "ani" in tokens

def jw_strict_pick(mal_id: int, titles_set: set, queries: list, year: int | None, jw_type: str | None,
                   is_multi_season: bool, has_mal_season_marker: bool):
    """
    Return an entry that matches:
      (title ∈ MAL titles) AND (year) AND (type) AND ('ani' genre present)
    plus Season-guard for shows:
      If MAL looks multi-season but MAL titles lack season markers -> reject (avoid series-wide aggregation).
    """
    if jw_search is None:
        print("simple-justwatch-python-api not installed. Please `pip install simple-justwatch-python-api`.")
        sys.exit(2)

    if not titles_set or year is None or jw_type is None:
        log(f"[{mal_id}] Strict picker aborted: titles={bool(titles_set)}, year={year}, jw_type={jw_type}")
        return None

    want_type = _norm_jw_type(jw_type)

    for q in queries[:8]:
        try:
            results = jw_search_rl(q, "US", "en", 20, True) or []
        except Exception as e:
            log(f"[{mal_id}] JW search error for '{q}': {e}")
            continue

        if not results:
            log(f"[{mal_id}] JW search returned 0 results for '{q}'.")
            continue

        examined = 0
        by_type = by_year = by_title = by_anicode = by_seasonguard = 0
        best = None
        for e in results:
            examined += 1
            title = getattr(e, "title", None) or getattr(e, "name", None) or ""
            ty = _norm_jw_type(getattr(e, "object_type", None) or getattr(e, "content_type", None) or getattr(e, "type", None))
            yr = getattr(e, "release_year", None) or getattr(e, "original_release_year", None) or getattr(e, "year", None)

            # Required checks
            if ty != want_type:
                by_type += 1; continue
            if yr != year:
                by_year += 1; continue
            if _norm_title(title) not in titles_set:
                by_title += 1; continue
            if not _jw_entry_has_ani_genre(e):
                by_anicode += 1
                continue

            # Season false-positive guard (only for shows)
            if want_type == "show" and is_multi_season and not has_mal_season_marker:
                by_seasonguard += 1
                if debug_log:
                    log(f"      [season-guard] Reject series-level match for MAL {mal_id} (multi-season w/o season marker).")
                continue

            best = e
            log(f"[{mal_id}] strict OK → type:{ty}, year:{yr}, title✓, ani-genre✓")
            break

        if best:
            return best

        # Per-candidate summary
        log(f"[{mal_id}] No strict match for '{q}'. Examined={examined}, by_type={by_type}, by_year={by_year}, by_title={by_title}, by_anicode={by_anicode}, by_seasonguard={by_seasonguard}")
        for idx, e in enumerate(results, 1):
            title = getattr(e, "title", None) or getattr(e, "name", None) or ""
            ty = _norm_jw_type(getattr(e, "object_type", None) or getattr(e, "content_type", None) or getattr(e, "type", None))
            yr = getattr(e, "release_year", None) or getattr(e, "original_release_year", None) or getattr(e, "year", None)
            nid = getattr(e, "node_id", None) or getattr(e, "entry_id", None) or getattr(e, "id", None)
            t_ok = _norm_title(title) in titles_set
            y_ok = (yr == year)
            ty_ok = (ty == want_type)
            ani_ok = _jw_entry_has_ani_genre(e)
            log(f"    [{idx:02d}] '{title}' | type={ty} ({'✓' if ty_ok else '×'}) | year={yr} ({'✓' if y_ok else '×'}) | node={nid} | title={'✓' if t_ok else '×'} | ani={'✓' if ani_ok else '×'}")
        time.sleep(0.05)

    return None

# Trusted provider filtering (Amazon fully excluded)
def _normalize_provider_string(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", "", s.lower())

def _is_trusted_provider(offer_pkg) -> bool:
    # Raw strings
    raw_name = (getattr(offer_pkg, "name", "") or "").lower()
    raw_tech = (getattr(offer_pkg, "technical_name", "") or "").lower()

    # Normalized tokens (letters/digits only)
    name = _normalize_provider_string(raw_name)
    tech = _normalize_provider_string(raw_tech)

    # 0) Global Amazon/Prime hard-exclude (channels included)
    if ("amazon" in raw_name or "amazon" in raw_tech or
        "prime" in raw_name or "prime" in raw_tech or
        "amazon" in name or "amazon" in tech or
        "prime"  in name or "prime"  in tech):
        return False

    # 1) Allowlist FIRST:
    #    - iTunes storefront (often shows with display name "Apple TV", tech "itunes")
    #    - Apple TV+ SVOD
    if tech == "itunes" or name == "itunes":
        return True
    if tech == "appletvplus" or name == "appletvplus":
        return True

    # 2) Hard-exclude ALL Apple TV Channels (anything starting with "appletv"
    #    that is NOT Apple TV+ and NOT iTunes)
    if name.startswith("appletv") or tech.startswith("appletv"):
        return False

    # 3) Other trusted providers (no change)
    if tech in TRUSTED_PROVIDERS or name in TRUSTED_PROVIDERS:
        return True

    # simple alias map
    aliases = {"disney+": "disneyplus", "hbomax": "max"}
    mapped = aliases.get(tech) or aliases.get(name)
    return bool(mapped and mapped in TRUSTED_PROVIDERS)

def jw_collect_langs_filtered(offers_by_country: dict[str, list]) -> tuple[set[str], dict]:
    langs: set[str] = set()
    diag = {
        "total_offers": 0,
        "kept_offers": 0,
        "skipped_untrusted_provider": 0,
        "skipped_no_audio": 0,
        "kept_providers": set(),
        "skipped_providers": set(),
    }

    for cc, offers in (offers_by_country or {}).items():
        for off in (offers or []):
            diag["total_offers"] += 1

            pkg = getattr(off, "package", None)
            if not pkg or not _is_trusted_provider(pkg):
                diag["skipped_untrusted_provider"] += 1
                if debug_log and pkg:
                    tname = getattr(pkg, "technical_name", "") or ""
                    pname = getattr(pkg, "name", "") or ""
                    diag["skipped_providers"].add((tname or pname).lower())
                continue

            auds = getattr(off, "audio_languages", None) or []
            if not auds:
                diag["skipped_no_audio"] += 1
                continue

            diag["kept_offers"] += 1
            diag["kept_providers"].add((getattr(pkg, "technical_name", "") or getattr(pkg, "name", "")).lower())
            for two_letter in auds:
                two_letter = (two_letter or "").strip().lower()
                if two_letter:
                    langs.add(two_letter)

    return langs, diag

def jw_union_audio_langs(node_id: str, countries: set[str]) -> list[str]:
    try:
        offers_map = jw_offers_for_countries_rl(node_id, countries, "en", True)
    except Exception as e:
        log(f"[node {node_id}] JW offers error: {e}")
        return []
    langs_set, diag = jw_collect_langs_filtered(offers_map)
    if debug_log:
        log(f"    JW diag: total={diag['total_offers']}, kept={diag['kept_offers']}, "
            f"skipped_provider={diag['skipped_untrusted_provider']}, skipped_no_audio={diag['skipped_no_audio']}")
        if diag["kept_providers"]:
            log(f"    kept providers={sorted(diag['kept_providers'])}")
        if diag["skipped_providers"]:
            log(f"    skipped providers={sorted(diag['skipped_providers'])}")
    return sorted(langs_set)


# ----------------------
# Runners
# ----------------------
def run_ann(mal_start: int, mal_end: int):
    pending: list[int] = []
    ann_to_mal: dict[int, int] = {}
    processed = 0

    try:
        for mal_id in range(mal_start, mal_end + 1):
            ann_id = jikan_ann_id_for_mal(mal_id)
            if ann_id:
                pending.append(ann_id)
                ann_to_mal[ann_id] = mal_id

            if len(pending) >= ANN_BATCH_SIZE:
                if debug_log:
                    log(f"ANN batch {processed // ANN_BATCH_SIZE + 1}: ids={pending[:3]}... (+{len(pending)-3} more)")
                process_ann_batch(pending, ann_to_mal)
                processed += len(pending)
                pending.clear()

            if processed and processed % (FINALIZE_EVERY_N) == 0:
                finalize_jsons("ann")

        if pending:
            process_ann_batch(pending, ann_to_mal)

    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error (ANN): {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("ann")
        print("Done (ANN).")


def run_mal(client_id: str, start_id: int, end_id: int):
    global missing_mal_ids, largest_known_mal_id

    missing_mal_ids = load_missing_cache()
    largest_known_mal_id = get_largest_known_mal_id()
    if debug_log:
        log(f"[cache] largest_known_mal_id={largest_known_mal_id or 0}")

    MAX_CONSECUTIVE_404 = 500
    consecutive_404 = 0
    try:
        for idx, mal_id in enumerate(range(start_id, end_id + 1), 1):
            if largest_known_mal_id and mal_id <= largest_known_mal_id and mal_id in missing_mal_ids:
                if debug_log:
                    log(f"  Skipping MAL ID {mal_id} (cached 404)")
                was_404 = True
            else:
                was_404 = process_anime_mal(mal_id, client_id)
                if was_404:
                    if mal_id not in missing_mal_ids:
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
                finalize_jsons("mal")
                save_missing_cache()
    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("mal")
        save_missing_cache()
        print("Done (MAL).")


def run_anilist(start_page: int | None, end_page: int | None):
    page = start_page or 1
    total_processed = 0

    try:
        while True:
            log(f"AniList Page {page}")
            has_next, processed = process_anilist_page(page)
            total_processed += processed

            if page % 10 == 0:
                finalize_jsons("anilist")

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
        finalize_jsons("anilist")
        print(f"Done (AniList). Processed ~{total_processed} media items.")

    log(f"AniList totals: pages={anilist_stats['pages']}, media={anilist_stats['media_total']}, "
        f"with_mal={anilist_stats['media_with_mal']}, no_mal={anilist_stats['media_without_mal']}, "
        f"with_langs={anilist_stats['media_with_langs']}, no_langs={anilist_stats['media_without_langs']}")


def run_justwatch(mal_start: int, mal_end: int):
    if jw_search is None or jw_offers_for_countries is None:
        print("For --api justwatch you must `pip install simple-justwatch-python-api` first.")
        sys.exit(2)

    global missing_mal_ids, largest_known_mal_id, mal_to_jw_map
    missing_mal_ids = load_missing_cache()
    largest_known_mal_id = get_largest_known_mal_id()
    mal_to_jw_map = load_mal_jw_map()

    try:
        for idx, mal_id in enumerate(range(mal_start, mal_end + 1), 1):
            # Skip known MAL 404s (only trust cache for IDs up to the largest known range)
            if largest_known_mal_id and mal_id <= largest_known_mal_id and mal_id in missing_mal_ids:
                if debug_log:
                    log(f"[JW] Skip MAL {mal_id} (cached as missing)")
                if idx % FINALIZE_EVERY_N == 0:
                    log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                    finalize_jsons("justwatch")
                    save_mal_jw_map(mal_to_jw_map)
                    save_missing_cache()
                continue

            # If we have a cached JW node_id, use it
            node_id = mal_to_jw_map.get(str(mal_id))
            if isinstance(node_id, str) and node_id:
                langs_codes = jw_union_audio_langs(node_id, JW_COVERAGE)
                if not langs_codes:
                    if debug_log:
                        log(f"[JW] No languages returned for MAL {mal_id} (node {node_id})")
                else:
                    for code in langs_codes:
                        key = jw_lang_to_key(code)
                        if key:
                            json_data[key].add(mal_id)
                time.sleep(0.03)
                if idx % FINALIZE_EVERY_N == 0:
                    log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                    finalize_jsons("justwatch")
                    save_mal_jw_map(mal_to_jw_map)
                    save_missing_cache()
                continue

            # Resolve strictly via Jikan + JW search
            meta = jikan_mal_meta_for_jw(mal_id)
            if meta is None:
                # If Jikan said 404, record in missing_mal_ids
                if jikan_last_404:
                    if mal_id not in missing_mal_ids:
                        missing_mal_ids.add(mal_id)
                        log(f"[JW] Added MAL {mal_id} to missing_mal_ids (Jikan 404).")
                if idx % FINALIZE_EVERY_N == 0:
                    log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                    finalize_jsons("justwatch")
                    save_mal_jw_map(mal_to_jw_map)
                    save_missing_cache()
                continue

            if not meta["titles_set"] or meta["year"] is None or meta["jw_type"] is None:
                if debug_log:
                    log(f"[JW] MAL {mal_id}: insufficient meta for strict match.")
                if idx % FINALIZE_EVERY_N == 0:
                    log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                    finalize_jsons("justwatch")
                    save_mal_jw_map(mal_to_jw_map)
                    save_missing_cache()
                continue

            entry = jw_strict_pick(
                mal_id,
                meta["titles_set"], meta["queries"], meta["year"], meta["jw_type"],
                meta["is_multi_season"], meta["has_season_marker"]
            )
            if not entry:
                if debug_log:
                    log(f"[JW] MAL {mal_id}: strict match not found (not cached).")
                if idx % FINALIZE_EVERY_N == 0:
                    log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                    finalize_jsons("justwatch")
                    save_mal_jw_map(mal_to_jw_map)
                    save_missing_cache()
                continue

            node_id = getattr(entry, "node_id", None) or getattr(entry, "entry_id", None) or getattr(entry, "id", None)
            if not node_id:
                if debug_log:
                    log(f"[JW] MAL {mal_id}: JW entry had no node/entry id.")
                if idx % FINALIZE_EVERY_N == 0:
                    log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                    finalize_jsons("justwatch")
                    save_mal_jw_map(mal_to_jw_map)
                    save_missing_cache()
                continue

            # Cache successful mapping and fetch fresh langs
            mal_to_jw_map[str(mal_id)] = node_id
            langs_codes = jw_union_audio_langs(node_id, JW_COVERAGE)
            for code in langs_codes:
                key = jw_lang_to_key(code)
                if key:
                    json_data[key].add(mal_id)

            time.sleep(0.03)

            if idx % FINALIZE_EVERY_N == 0:
                log(f"--- Updating files at MAL ID {mal_start + idx - 1} ---")
                finalize_jsons("justwatch")
                save_mal_jw_map(mal_to_jw_map)
                save_missing_cache()

    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error (JustWatch): {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("justwatch")
        save_mal_jw_map(mal_to_jw_map)
        save_missing_cache()
        print("Done (JustWatch).")


# ----------------------
# Main
# ----------------------
def main():
    global debug_log

    parser = argparse.ArgumentParser(
        description="Fetch dubbed languages per anime and save to JSON (append-only)."
    )
    parser.add_argument("--api", choices=["mal", "anilist", "ann", "justwatch"], default="mal", help="Which source to use.")
    parser.add_argument("--debug", default="false", help="Enable verbose logging (true/false).")

    # MAL-specific
    parser.add_argument("--client-id", help="MyAnimeList API Client ID (required for --api mal).")
    parser.add_argument("--mal-start", type=int, help="Start MAL ID (inclusive) for --api mal/ann/justwatch.")
    parser.add_argument("--mal-end", type=int, help="End MAL ID (inclusive) for --api mal/ann/justwatch.")

    # AniList-specific
    parser.add_argument("--anilist-start-page", type=int, help="AniList: start page number (1-based).")
    parser.add_argument("--anilist-end-page", type=int, help="AniList: end page number (inclusive).")
    parser.add_argument("--anilist-check-pages", default="false",
                        help="AniList: if true, prints total pages (perPage=50) and exits.")

    args = parser.parse_args()
    debug_log = str(args.debug).lower() == "true"

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

    else:  # justwatch
        if args.mal_start is None or args.mal_end is None:
            print("For --api justwatch you must provide --mal-start and --mal-end.")
            sys.exit(1)
        run_justwatch(args.mal_start, args.mal_end)


if __name__ == "__main__":
    main()
