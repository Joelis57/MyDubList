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

# ======================
# CONFIG
# ======================
MAL_BASE = "https://api.myanimelist.net/v2"
JIKAN_BASE = "https://api.jikan.moe/v4"
ANILIST_BASE = "https://graphql.anilist.co"

MAX_IN_MEMORY_CACHE = 5000
CALL_RETRIES = 4
RETRY_DELAYS = [10, 60, 120]
FINALIZE_EVERY_N = 100

# AniList paging (fixed by request)
ANILIST_PER_PAGE = 50
ANILIST_CHAR_PER_PAGE = 50
ANILIST_PAGE_SLEEP = 2  # seconds
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

    # Portuguese variants → "portuguese"
    if s.startswith("portuguese"):
        return "portuguese"

    # NEW: Mandarin → Chinese (unify MAL + AniList)
    if s.startswith("mandarin"):
        return "chinese"

    # Remove any (...) parenthetical chunk(s)
    s = re.sub(r"\(.*?\)", "", s).strip()

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)

    return s


def filename_for_lang(lang_key: str) -> str:
    """Turn normalized language key into a filename-friendly token."""
    return lang_key.replace(" ", "_")


# ----------------------
# MAL + Jikan helpers
# ----------------------
@lru_cache(maxsize=MAX_IN_MEMORY_CACHE)
def get_anime_roles_for_va_cached(person_id):
    log(f"    Fetching VA {person_id} from API")
    return jikan_get(f"/people/{person_id}/voices")


def mal_get(url, client_id):
    global mal_last_call, last_mal_404

    # Throttle MAL to <= 0.5 req/sec (2s min interval)
    now = time.time()
    to_wait = MAL_MIN_INTERVAL - (now - mal_last_call)
    if to_wait > 0:
        time.sleep(to_wait)

    headers = {"X-MAL-CLIENT-ID": client_id}
    last_exception = None
    for attempt in range(CALL_RETRIES):
        try:
            # record call time right before we hit the endpoint
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

    # If the characters call 404'd, short-circuit and report it
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
    media(type: ANIME, sort: ID) {  # you can change ID -> POPULARITY_DESC to sanity check
      id
      idMal
      characters(perPage: $charPerPage) {
        edges {
          node { id }  # optional, useful for debug
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
    media(type: ANIME, sort: ID) { id }  # trivial selection so Page is actually paging something
  }
}
"""


def anilist_post(query, variables):
    last_exception = None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(CALL_RETRIES):
        try:
            r = requests.post(ANILIST_BASE, json={"query": query, "variables": variables}, headers=headers)
            # AniList returns 200 with {"errors":[...]} for GraphQL errors. 400s usually mean malformed payload.
            if r.status_code >= 400:
                # show the raw body so you can see the GraphQL error text
                print(f"  AniList HTTP {r.status_code}: {r.text[:500]}")
                r.raise_for_status()
            data = r.json()
            if "errors" in data:
                # surface GraphQL errors
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

    # Local counters for this page
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
                    # Dump a sample edge (trimmed) to inspect structure
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

    # Update global stats
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

        updated_ids = existing_ids | set(new_ids)  # append-only

        added_ids = sorted(updated_ids - existing_ids)
        if added_ids:
            log(f"  Changes in {filename}:")
            log(f"    Added: {added_ids}")

        obj = {
            "_license": "This file is licensed under the MIT License. User visible attribution is required.",
            "_origin": "https://github.com/Joelis57/MyDubList",
            "language": lang_key.capitalize(),
            "dubbed": sorted(updated_ids),
        }

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        if debug_log:
            log(f"  wrote {filename} (total ids: {len(updated_ids)})")


# ----------------------
# Runners
# ----------------------
def run_mal(client_id: str, start_id: int, end_id: int):
    MAX_CONSECUTIVE_404 = 500
    consecutive_404 = 0
    try:
        for idx, mal_id in enumerate(range(start_id, end_id + 1), 1):
            was_404 = process_anime_mal(mal_id, client_id)

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
    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        finalize_jsons("mal")
        print("Done (MAL).")


def run_anilist(start_page: int | None, end_page: int | None):
    # If no start_page is given, default to 1
    page = start_page or 1
    total_processed = 0

    try:
        while True:
            log(f"AniList Page {page}")
            has_next, processed = process_anilist_page(page)
            total_processed += processed

            if page % 10 == 0:
                finalize_jsons("anilist")

            # Stop if we've reached the requested end_page
            if end_page is not None and page >= end_page:
                break

            # Otherwise continue as long as AniList has more pages
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


# ----------------------
# Main
# ----------------------
def main():
    global debug_log

    parser = argparse.ArgumentParser(
        description="Fetch dubbed languages per anime and save to JSON (append-only)."
    )
    parser.add_argument("--api", choices=["mal", "anilist"], default="mal", help="Which source to use.")
    parser.add_argument("--debug", default="false", help="Enable verbose logging (true/false).")

    # MAL-specific
    parser.add_argument("--client-id", help="MyAnimeList API Client ID (required for --api mal).")
    parser.add_argument("--mal-start", type=int, help="Start MAL ID (inclusive) for --api mal.")
    parser.add_argument("--mal-end", type=int, help="End MAL ID (inclusive) for --api mal.")

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

    else:  # anilist
        if str(args.anilist_check_pages).lower() == "true":
            try:
                pages = anilist_total_pages(ANILIST_PER_PAGE)
                print(pages)
                return
            except Exception as e:
                print(f"Failed to fetch AniList total pages: {e}")
                sys.exit(2)

        # Run with optional start/end page bounds
        run_anilist(args.anilist_start_page, args.anilist_end_page)


if __name__ == "__main__":
    main()
