# MyDubList

**MyDubList** is an open-source tool that builds and maintains a database of anime titles with multilingual dubs. It uses both the official MyAnimeList API and the Jikan API to automatically identify which anime titles have been dubbed in specific languages.

---

## 🚀 How It Works

1. **Automatic Dub Detection:**

   * For each anime (MAL ID), the script retrieves the list of characters using the MyAnimeList API.
   * It selects **only the first main character**.
   * Using the Jikan API, it fetches all voice actors for that character.
   * For each voice actor, it checks their full voice acting history to confirm whether they actually voiced the character **in that specific anime**.
   * If a match is found, the anime is recorded as dubbed in that voice actor's language.

2. **Manual Corrections:**

   * You can manually correct or override data using JSON files in the `manual/` folder.
   * These support fields for `dubbed`, `not_dubbed`, and `incomplete`.
   * Manual entries override automatic ones during the merge step.

3. **Final Output:**

   * Merged results are written to the `final/` folder.
   * Each language file contains two arrays: `dubbed` and `incomplete`.

---

## 📁 Folder Structure

* `automatic/` — automatically generated dubbed lists
* `manual/` — user-maintained overrides
* `final/` — merged, cleaned output files

---

## 🛠 Requirements

* Python 3.7+
* `requests` library

Install dependencies with:

```bash
pip install requests
```

---

## 💻 Running the Script

To fetch dubbed anime info for a range of MAL IDs:

```bash
python fetchDubsFromApi.py <mal_client_id> <startMalId> <endMalId>
```

To merge automatic and manual entries into the final folder:

```bash
python merge_manual_and_automatic.py
```

---

## ☕ Support the Project

If you find this project helpful, consider supporting its development:

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/joelis57)

---

## 📄 License

MIT License. User-visible attribution is required.

---

## 📫 Contributing

Pull requests are welcome! Feel free to open an issue or suggestion to fix mistakes or to help identify anime with incomplete dubs across different languages. Your contributions are highly appreciated!

---
