import asyncio
from curl_cffi.requests import AsyncSession
import json
import time

# --- CONFIGURATION ---
cookies = {
    "dsq__u": "ug28rv376gfab",
    "dsq__s": "ug28rv376gfab",
    "cf_clearance": "MYw6GiG8utmImCuiyJ3LE6VIFu8FhqWoIpf3b5lbcnc-1771593323-1.2.1.1-gkqGfq2RAb5z4xTpyLfija8w5W4gn3w.aYGTOqumS22Fj3om6ra1y5jkpntt9doBLHbQeQlQd3ugtCaBSOZSQiTRQvaiZbAWVtPk6bCtiwhB5rp_grs85fSXH3TKhrzpDOWwzbWAkeYin9xgUSKegkMpMc6Vc_YfeyR1lA._J.YWGoNBKpGOEIFvGNL6zs.KXHjzdYwxyArWYdf39M.nUeP1x1Ei3HRxQ6_WriwL4o4",
}

headers = {
    "accept": "*/*",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,ar;q=0.7",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

LANGUAGE_MAP = {
    "en-US": "english",
    "es-419": "spanish",
    "es-ES": "spanish",
    "fr-FR": "french",
    "it-IT": "italian",
    "pt-BR": "portuguese",
    "de-DE": "german",
    "ru-RU": "russian",
    "ar-SA": "arabic",
    "ja-JP": "japanese",
}


async def fetch_details_concurrently(session, content_id, semaphore):
    async with semaphore:
        url = f"https://anime.uniquestream.net/api/v1/series/{content_id}"
        try:
            response = await session.get(
                url, headers=headers, cookies=cookies, impersonate="edge101", timeout=15
            )
            if response.status_code == 200:
                data = response.json()
                title = data.get("title")

                mal_id = None
                if "seasons" in data and len(data["seasons"]) > 0:
                    mal_id = data["seasons"][0].get("mal_id")

                raw_audio = data.get("audio_locales", [])
                mapped_audio = list(
                    set([LANGUAGE_MAP.get(lang, lang) for lang in raw_audio])
                )

                return {"title": title, "mal_id": mal_id, "audio": mapped_audio}
        except Exception:
            pass
        return None


async def main():
    offset = 0
    all_shows = []
    start_time = time.monotonic()

    async with AsyncSession() as session:
        print("Phase 1: Scraping the Master List...")
        while True:
            list_url = (
                f"https://anime.uniquestream.net/api/v1/videos/browse?offset={offset}"
            )
            response = await session.get(
                list_url,
                headers=headers,
                cookies=cookies,
                impersonate="edge101",
                timeout=10,
            )

            if response.status_code != 200:
                break

            data = response.json()
            items = data.get("data", [])

            if not items:
                break

            for item in items:
                all_shows.append(item.get("content_id"))

            offset += len(items)
            print(f"Collected {len(all_shows)} total IDs so far...")

        print(f"\nPhase 1 Complete. Found {len(all_shows)} total shows.")
        print("Phase 2: Fetching details concurrently...\n")

        semaphore = asyncio.Semaphore(15)
        tasks = [
            fetch_details_concurrently(session, show_id, semaphore)
            for show_id in all_shows
        ]

        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r and r["mal_id"]]

        filename = "crunchyroll_data.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(valid_results, f, indent=4, ensure_ascii=False)

        print("-" * 50)
        print(f"Extraction complete! Saved {len(valid_results)} shows to {filename}.")
        print(f"Total Execution Time: {time.monotonic() - start_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
