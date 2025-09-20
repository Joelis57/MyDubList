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
| Language | Native name | Dubbed |
|---|---:|---:|
| Japanese | 日本語 | 14965 |
| English | English | 5228 |
| Spanish | Español | 2951 |
| German | Deutsch | 2908 |
| French | Français | 2396 |
| Italian | Italiano | 2272 |
| Portuguese | Português | 1686 |
| Korean | 한국어 | 1369 |
| Tagalog | Tagalog | 810 |
| Chinese | 中文 | 695 |
| Arabic | العربية | 316 |
| Polish | Polski | 274 |
| Hungarian | Magyar | 185 |
| Swedish | Svenska | 142 |
| Norwegian | Norsk | 135 |
| Hebrew | עברית | 120 |
| Dutch | Nederlands | 103 |
| Russian | Русский | 78 |
| Danish | Dansk | 62 |
| Indonesian | Bahasa Indonesia | 60 |
| Thai | ไทย | 39 |
| Hindi | हिन्दी | 19 |
| Finnish | Suomi | 16 |
| Turkish | Türkçe | 10 |
| Catalan | Català | 3 |
| Vietnamese | Tiếng Việt | 1 |
| Lithuanian | Lietuvių | 0 |
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
