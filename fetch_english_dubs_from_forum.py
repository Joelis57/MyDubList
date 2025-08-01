import requests
import json
import re
import os
from bs4 import BeautifulSoup

FORUM_URL = "https://myanimelist.net/forum/?topicid=1692966"
MANUAL_PATH = os.path.join("manual", "dubbed_english.json")
FINAL_PATH = os.path.join("final", "dubbed_english.json")

def fetch_posts():
    resp = requests.get(FORUM_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    posts = soup.find_all("div", class_="forum-topic-message", attrs={"data-user": "Kenny_Stryker"})
    return posts[:5]

def extract_mal_ids(post):
    content = post.find("div", class_="content")
    ids = set()
    for a in content.find_all("a", href=True):
        href = a["href"]
        match = re.match(r"https://myanimelist\.net/anime/(\d+)", href)
        if match:
            ids.add(int(match.group(1)))
    return ids

def load_existing_ids(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {
                "dubbed": set(data.get("dubbed", [])),
                "incomplete": set(data.get("incomplete", [])),
                "not_dubbed": set(data.get("not_dubbed", []))
            }
    except FileNotFoundError:
        return {"dubbed": set(), "incomplete": set(), "not_dubbed": set()}
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return {"dubbed": set(), "incomplete": set(), "not_dubbed": set()}

def save_manual_file(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"Saved updated dubbed list to {path}")

def main():
    print("🔍 Fetching forum posts...")
    posts = fetch_posts()
    
    new_ids = set()
    for idx, post in enumerate(posts, start=1):
        ids = extract_mal_ids(post)
        new_ids.update(ids)
        print(f"--- Post {idx}: {len(ids)} anime IDs found")

    print(f"Total extracted IDs: {len(new_ids)}")

    manual_data = load_existing_ids(MANUAL_PATH)
    final_data = load_existing_ids(FINAL_PATH)

    already_present = (
        manual_data["dubbed"] |
        manual_data["incomplete"] |
        manual_data["not_dubbed"] |
        final_data["dubbed"] |
        final_data["incomplete"]
    )

    # Filter out existing ones
    filtered_new_ids = sorted(mid for mid in new_ids if mid not in already_present)
    print(f"New unique IDs to add: {len(filtered_new_ids)}")

    if not os.path.exists(MANUAL_PATH):
        print(f"File not found: {MANUAL_PATH}")
        return

    with open(MANUAL_PATH, "r", encoding="utf-8") as f:
        manual_json = json.load(f)

    updated_dubbed = set(manual_json.get("dubbed", []))
    updated_dubbed.update(filtered_new_ids)
    manual_json["dubbed"] = sorted(updated_dubbed)

    save_manual_file(MANUAL_PATH, manual_json)

if __name__ == "__main__":
    main()
