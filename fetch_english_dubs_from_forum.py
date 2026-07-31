#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import re
import os
from bs4 import BeautifulSoup

FORUM_URL = "https://myanimelist.net/forum/?topicid=1692966"
KENNY_USERNAME = "Kenny_Stryker"
POST_COUNT = 11  # how many of Kenny's posts to read (most recent first)
# A 200 OK that isn't the expected page (a challenge/interstitial, a moved or
# locked topic, a markup change) parses fine and yields zero posts. Refuse to
# publish a result that small rather than overwriting a good file with it.
MIN_KEEP_FRACTION = 0.5

OUTPUT_DIR = os.path.join("dubs", "sources", "automatic_kenny")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "dubbed_english.json")

def fetch_posts():
    resp = requests.get(FORUM_URL, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    posts = soup.find_all("div", class_="forum-topic-message", attrs={"data-user": KENNY_USERNAME})
    return posts[:POST_COUNT]

def extract_mal_ids(post):
    content = post.find("div", class_="content")
    ids = set()
    if not content:
        return ids
    for a in content.find_all("a", href=True):
        href = a["href"]
        match = re.match(r"https://myanimelist\.net/anime/(\d+)", href)
        if match:
            ids.add(int(match.group(1)))
    return ids

def load_existing_ids(path):
    """Ids already published, or None when there is no file yet. A corrupt file
    raises rather than reading as empty, which would disable the check below."""
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return set(json.load(f).get("dubbed", []))


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def main():
    print(f"🔍 Fetching forum posts from {FORUM_URL} (user: {KENNY_USERNAME}) ...")
    posts = fetch_posts()

    all_ids = set()
    for idx, post in enumerate(posts, start=1):
        ids = extract_mal_ids(post)
        all_ids.update(ids)
        print(f"--- Post {idx}: found {len(ids)} anime IDs")

    print(f"Total unique IDs extracted: {len(all_ids)}")

    existing = load_existing_ids(OUTPUT_PATH)
    if existing and len(all_ids) < MIN_KEEP_FRACTION * len(existing):
        raise SystemExit(
            f"Refusing to overwrite {OUTPUT_PATH}: extracted {len(all_ids)} IDs, under "
            f"{MIN_KEEP_FRACTION:.0%} of the {len(existing)} already published. "
            "The response is not the expected forum page."
        )

    # Always (re)write the automatic_kenny file fresh — no reading old state.
    payload = {
        "_license": "CC BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
        "_attribution": "MyDubList - https://mydublist.com - (CC BY 4.0)",
        "_origin": FORUM_URL,
        "language": "English",
        "dubbed": sorted(all_ids),
    }

    save_json(OUTPUT_PATH, payload)
    print(f"✅ Wrote {len(all_ids)} IDs to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()