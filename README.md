# MyDubList

**MyDubList** is an open-source project that builds and maintains a multi-language database of anime with released dubs. The dataset is designed to be dependable, simple to consume, and permissively licensed (only requiring attribution) so anyone-from hobby scripts to large closed-source apps-can use it.

## Overview

- Continuously updated, language-keyed dataset of dubbed anime (by MyAnimeList ID).
- Final JSON files under `final/`, one per language (e.g. `final/dubbed_english.json`).
- Optional `incomplete` array tracks edge cases (partial/lost/unverified) when relevant.

## Database search website

[Click here to search](https://mydublist.com)

![MyDubList Search](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/mydublist.com.jpg)

## Browser extension for MyAnimeList

![MyDubList extension showcase](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/extension-showcase.gif)

[![Install for Chrome](https://img.shields.io/badge/Install-Chrome%20Web%20Store-4285F4?logo=google-chrome&logoColor=white)](https://chrome.google.com/webstore/detail/mydublist/hdpppphfhlhmehghmndopednfpbimkco)
[![Install for Firefox](https://img.shields.io/badge/Install-Firefox%20Add--ons-FF7139?logo=firefox-browser&logoColor=white)](https://addons.mozilla.org/en-US/firefox/addon/mydublist)

## Language statistics

<!-- LANG-STATS:START -->
| Language | Native name | Dubbed | Incomplete | File |
|---|---:|---:|---:|---|
| Japanese | 日本語 | 13524 | 0 | `final/dubbed_japanese.json` |
| English | English | 4937 | 66 | `final/dubbed_english.json` |
| Spanish | Español | 2792 | 0 | `final/dubbed_spanish.json` |
| German | Deutsch | 2245 | 0 | `final/dubbed_german.json` |
| French | Français | 2218 | 0 | `final/dubbed_french.json` |
| Italian | Italiano | 2080 | 0 | `final/dubbed_italian.json` |
| Portuguese | Português | 1669 | 0 | `final/dubbed_portuguese.json` |
| Korean | 한국어 | 1338 | 0 | `final/dubbed_korean.json` |
| Tagalog | Tagalog | 812 | 0 | `final/dubbed_tagalog.json` |
| Chinese | 中文 | 639 | 0 | `final/dubbed_chinese.json` |
| Arabic | العربية | 316 | 0 | `final/dubbed_arabic.json` |
| Polish | Polski | 269 | 0 | `final/dubbed_polish.json` |
| Hungarian | Magyar | 168 | 0 | `final/dubbed_hungarian.json` |
| Swedish | Svenska | 141 | 0 | `final/dubbed_swedish.json` |
| Norwegian | Norsk | 134 | 0 | `final/dubbed_norwegian.json` |
| Hebrew | עברית | 107 | 0 | `final/dubbed_hebrew.json` |
| Dutch | Nederlands | 102 | 0 | `final/dubbed_dutch.json` |
| Russian | Русский | 78 | 0 | `final/dubbed_russian.json` |
| Danish | Dansk | 62 | 0 | `final/dubbed_danish.json` |
| Indonesian | Bahasa Indonesia | 59 | 0 | `final/dubbed_indonesian.json` |
| Thai | ไทย | 39 | 0 | `final/dubbed_thai.json` |
| Hindi | हिन्दी | 19 | 0 | `final/dubbed_hindi.json` |
| Finnish | Suomi | 16 | 0 | `final/dubbed_finnish.json` |
| Turkish | Türkçe | 10 | 0 | `final/dubbed_turkish.json` |
| Catalan | Català | 3 | 0 | `final/dubbed_catalan.json` |
| Vietnamese | Tiếng Việt | 1 | 0 | `final/dubbed_vietnamese.json` |
<!-- LANG-STATS:END -->

## App integrations

### DailyAL
![DailyAL integration](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/DailyAL.jpg)

With enough interest, support may be added to MoeList and other iOS/Android clients.

## Data sources (automatic)

The database is automatically assembled and constantly refreshed from multiple sources:

- MyAnimeList API
- AniList API
- Kenny Stryker's ("Mr. Dub McQueen") community forum post on MAL
- Select NSFW dub sources (where applicable)

All sources are ingested, normalized, de-duplicated, and merged by the merge_manual_and_automatic script to keep the dataset current. If something looks wrong or missing for your language, please open an issue.

## Output format

Each language file in `final/` contains two arrays of MAL IDs:

```json
{
  "dubbed": [16498, 40028, 38524],
  "incomplete": [50197]
}
```

## ☕ Support the Project

If you find this project helpful, consider supporting its development.

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/joelis)

---

## 📄 License

- **Code:** MIT © MyDubList. See [LICENSE](./LICENSE).
- **Dataset (JSON files)**: **Creative Commons Attribution 4.0 International (CC BY 4.0)**.  
  See [DATA-LICENSE](./DATA-LICENSE) and [NOTICE](./NOTICE).

### Required attribution for dataset (CC BY 4.0)

When you display or distribute the dataset (or substantial portions/derivatives), provide public attribution that is reasonable to the medium, including:
- **Name:** MyDubList
- **Link to source:** https://mydublist.com
- **License:** CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/)
- **Changes:** note if you modified the data

**Preferred credit line:**
> "Dub data © MyDubList - https://mydublist.com - (CC BY 4.0)"

**Reasonable placement:** About screen, settings, footer, or results header.

### Requested (not required) UI links

I kindly ask integrators who display dub info to include:
- A small "**Powered by MyDubList**" credit linking to https://mydublist.com  
- **Report inaccurate dubs** → https://github.com/Joelis57/MyDubList/issues/new/choose  
- **Support MyDubList** → https://ko-fi.com/joelis

---

## 📫 Contributing

Your contributions are highly appreciated!

- **Report issues**: please include the anime link, language, what’s missing/incorrect, and a source (streaming page, official post, etc.).
- **Manual overrides**: see manual/ for the override format. Manual entries supersede automated results during merge.
- **Pull requests**: welcome for new sources, language keys, validation, and tooling.

---
