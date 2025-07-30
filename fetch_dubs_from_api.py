import requests
import json
import time
import sys
import os
from collections import defaultdict
from functools import lru_cache

# ======================
# CONFIG
# ======================
MAL_BASE = "https://api.myanimelist.net/v2"
JIKAN_BASE = "https://api.jikan.moe/v4"
MAX_IN_MEMORY_CACHE = 5000
CALL_RETRIES = 3
FINALIZE_EVERY_N = 100
# ======================

# Globals
jikan_last_call = 0
json_data = defaultdict(set)

@lru_cache(maxsize=MAX_IN_MEMORY_CACHE)
def get_anime_roles_for_va_cached(person_id):
    print(f"    Fetching VA {person_id} from API")
    return jikan_get(f"/people/{person_id}/voices")

def mal_get(url, client_id):
    headers = {"X-MAL-CLIENT-ID": client_id}
    last_exception = None
    for _ in range(CALL_RETRIES):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 404:
                print("  404 Anime Not Found, skipping")
                return None
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_exception = e
            time.sleep(10)
    raise last_exception

def jikan_get(url):
    global jikan_last_call
    now = time.time()
    to_wait = 1.0 - (now - jikan_last_call)
    if to_wait > 0:
        time.sleep(to_wait)
    jikan_last_call = time.time()

    last_exception = None
    for _ in range(CALL_RETRIES):
        try:
            response = requests.get(JIKAN_BASE + url)
            if response.status_code == 404:
                print("  404 Not Found, skipping")
                return None
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_exception = e
            time.sleep(10)
    raise last_exception

def get_characters(mal_id, client_id):
    return mal_get(f"{MAL_BASE}/anime/{mal_id}/characters?limit=1", client_id)

def get_voice_actors(char_id):
    return jikan_get(f"/characters/{char_id}/voices")

def process_anime(mal_id, client_id):
    print(f"Processing MAL ID: {mal_id}")
    characters = get_characters(mal_id, client_id)
    if not characters or "data" not in characters or not characters["data"]:
        print(f"  No characters found, skipping")
        return

    first_char = characters["data"][0]["node"]
    char_id = first_char["id"]

    voice_actors = get_voice_actors(char_id)
    if not voice_actors or "data" not in voice_actors:
        return

    for va_entry in voice_actors["data"]:
        lang = va_entry["language"]
        person = va_entry["person"]
        person_id = person["mal_id"]

        print(f"  Processing voice actor: {person['name']} ({lang})")

        va_roles = get_anime_roles_for_va_cached(person_id)
        if not va_roles or "data" not in va_roles:
            continue

        for role_entry in va_roles["data"]:
            anime = role_entry.get("anime")
            if anime and anime.get("mal_id") == mal_id:
                json_data[lang].add(mal_id)
                break

def finalize_jsons(start_id, end_id):
    os.makedirs("automatic", exist_ok=True)
    processed_range = set(range(start_id, end_id + 1))

    for lang, new_ids in json_data.items():
        sanitized_lang = lang.lower().replace("(", "").replace(")", "").replace(" ", "_")
        filename = os.path.join("automatic", f"dubbed_{sanitized_lang}.json")

        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                    existing_ids = set(existing_data.get("dubbed", []))
                except Exception:
                    existing_ids = set()
        else:
            existing_ids = set()

        updated_ids = (existing_ids - processed_range) | new_ids

        obj = {
            "_license": "This file is licensed under the MIT License. User visible attribution is required.",
            "_origin": "https://github.com/Joelis57/MyDubList",
            "language": lang,
            "dubbed": sorted(updated_ids)
        }

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    if len(sys.argv) != 4:
        print("Usage: python fetchDubsFromApi.py <mal_client_id> <startMalId> <endMalId>")
        return

    client_id = sys.argv[1]
    start_id = int(sys.argv[2])
    end_id = int(sys.argv[3])

    try:
        for count, mal_id in enumerate(range(start_id, end_id + 1), 1):
            process_anime(mal_id, client_id)
            if count % FINALIZE_EVERY_N == 0:
                current_end = start_id + count - 1
                print(f"--- Updating files at MAL ID {current_end} ---")
                finalize_jsons(start_id, current_end)
    except KeyboardInterrupt:
        print("\nInterrupted. Finalizing data...")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

    finalize_jsons(start_id, end_id)
    print("Done.")

if __name__ == "__main__":
    main()
