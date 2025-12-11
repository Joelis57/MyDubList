# MyDubList

MyDubList is an open-source project that builds and maintains a **multi‑language** database of anime **dubs**.
The JSON datasets are licensed under **CC BY 4.0** (only requiring attribution) so anyone-from hobby scripts to large closed-source apps-can use it.


## Database search website

[Click here to search](https://mydublist.com)

![MyDubList Search](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/mydublist.com.jpg)

## Browser extension for MyAnimeList

![MyDubList extension showcase](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/extension-showcase.gif)

[![Install for Chrome](https://img.shields.io/badge/Install-Chrome%20Web%20Store-4285F4?logo=google-chrome&logoColor=white)](https://chrome.google.com/webstore/detail/mydublist/hdpppphfhlhmehghmndopednfpbimkco)
[![Install for Firefox](https://img.shields.io/badge/Install-Firefox%20Add--ons-FF7139?logo=firefox-browser&logoColor=white)](https://addons.mozilla.org/en-US/firefox/addon/mydublist)

## Sources

The dataset aggregates information from multiple sources, including:

- **MyAnimeList** (official API) and **Jikan** (community API)
- **AniList** (official API)
- **Anime News Network** (official API)
- **aniSearch** (custom API for MyDubList)
- **Kitsu** (official API)
- **HiAnime** (community API)
- Curated community lists (e.g., *Kenny Stryker’s English dubs list* on MAL forums)
- Manual overrides by MyDubList

If you have an authoritative source to add, please open an issue/PR.

## Language statistics

<!-- LANG-STATS:START -->
| Language | Native name | Dubbed |
|---|---:|---:|
| English | English | 5275 |
| Spanish | Español | 2988 |
| German | Deutsch | 2918 |
| French | Français | 2405 |
| Italian | Italiano | 2292 |
| Portuguese | Português | 1718 |
| Korean | 한국어 | 1377 |
| Chinese | 中文 | 828 |
| Tagalog | Tagalog | 813 |
| Arabic | العربية | 315 |
| Polish | Polski | 276 |
| Hungarian | Magyar | 193 |
| Swedish | Svenska | 142 |
| Norwegian | Norsk | 135 |
| Hebrew | עברית | 122 |
| Dutch | Nederlands | 104 |
| Russian | Русский | 78 |
| Indonesian | Bahasa Indonesia | 64 |
| Danish | Dansk | 63 |
| Thai | ไทย | 43 |
| Hindi | हिन्दी | 20 |
| Finnish | Suomi | 16 |
| Turkish | Türkçe | 10 |
| Catalan | Català | 3 |
| Vietnamese | Tiếng Việt | 1 |
| Lithuanian | Lietuvių | 0 |
<!-- LANG-STATS:END -->

## Data layout

```
dubs/
  confidence/
    <confidence>/
      dubbed_<lang>.json
  counts/
    dubbed_<lang>.json       # { "<mal_id>": <num_sources>, ... }
  sources/
    <source>/
      dubbed_<lang>.json     # { "dubbed": [<mal_id>, <mal_id>, ...] }
  mappings/
    mappings_<source>.jsonl  # { "mal_id":<mal_id>,"<source>_id":<source_id>, ... }

cache/
  missing_mal_ids.json       # List of MAL IDs returning 404

final/                       # DEPRECATED: legacy per‑language JSONs
```

- `<lang>` is a lowercase language key (e.g., `english`, `french`, `spanish`, `german`, `italian`, `japanese`, `korean`, `mandarin`, `hebrew`, `hungarian`, `portuguese_br`, etc.).
- `<confidence>` can be `low` (≥1 source), `normal` (≥2 sources), `high` (≥3 sources) or `very-high` (≥4 sources).

## App integrations

### DailyAL
![DailyAL integration](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/DailyAL.jpg)

### Seanime
![Seanime integration](https://raw.githubusercontent.com/Joelis57/MyDubList/main/images/Seanime.jpg)

[Seanime plugin](https://github.com/Bas1874/MyDubList-Seanime) by [Bas1874](https://github.com/Bas1874).

## Contributing

- **Issues:** Please include the anime link (MAL preferred), the **language**, what’s missing/incorrect, and a verifying **source URL** (streaming page, official news/announcement, etc.).
- **Pull Requests:** Welcome for new sources, data corrections, language keys, validation logic, and tooling improvements.
- Be mindful of rate limits and terms of the upstream sources.

## Attribution & License

**Dataset** (all JSON files inside the repository): Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).  

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
If you need a different data license for a specific use case, open an issue to discuss.

## ☕ Support

If this project helps you, consider supporting: https://ko-fi.com/joelis
