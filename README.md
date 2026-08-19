# ADH African Country Inflation Database (v2)

The IMF publishes an excellent CPI/inflation database, but it depends on countries
reporting to the IMF, so it is often a month or more behind. **This repository keeps an
African inflation database up to date** by taking the IMF's African CPI series as a base
and overlaying more recent figures taken directly from national statistics agencies as
soon as they publish.

The result powers:

* **CKAN dataset** — <https://ckan.africadatahub.org/dataset/imf-africa-inflation-database>
* **ADH Inflation Observer** — <https://www.africadatahub.org/dashboards/inflation-observer>

---

## 1. What the process actually is

This is a **monthly, human-driven pipeline with Python assistance** — not an automated
job. Roughly half the work is manual: checking country websites, downloading bulletins,
reading numbers out of PDFs, and pasting them into per-country CSVs. The scripts automate
the parts that can be automated.

```
                       +------------------------------------------+
  IMF CPI bulk CSV --> | adh_prep_imf_New25.py                    |
  (data/imf/raw/)      |  filter to Africa + YoY % + monthly CPI  |
                       +--------------------+---------------------+
                                            |  outputs/imf/CPI_*_NewtimeSeries_africa.xlsx
                                            v
  National statistics    +--------------------------+     +---------------------------+
  offices (PDF/XLSX) --> | adh_prep_<country>.py    | --> | data/<country>/csv/       |
  (data/<c>/raw/)        |   (23 countries)         |     |   <file>.csv  (one month) |
                         +--------------------------+     +-------------+-------------+
                                                                        |
                                                    copy/paste last column
  National statistics                                                   v
  offices (manual) ----------------------------------->  data/<country>/csv/
                                                            <country>_output.csv
                                                                        |
                       +------------------------------------------------+
                       v
              +------------------------------+
              | adh_create_inflation_db.py   |     previous CKAN file
              |  base = last CKAN output     | <-- outputs/ckan/*.csv
              |  + IMF overlay (last 12 mo)  |
              |  + country overlay           |
              +--------------+---------------+
                             v
        outputs/ckan/  <date>_combined_imf_database.csv   (wide: one row per country+indicator)
                       <date>_reshaped_imf_database.csv   (one row per country+date)
                       power_BI.csv                       (melted: country/indicator/date/value)
                             |
                             v
                  Manual upload to CKAN (Step 6)
```

**Key idea:** the database is *cumulative*. Every run reads the previous month's
`*_combined_imf_database.csv` as its starting point and overwrites cells in it. Nothing is
rebuilt from scratch. If you start from the wrong base file you silently lose history —
see [§7 Gotchas](#7-gotchas-and-landmines).

### Indicators tracked

13 COICOP divisions plus the headline index, all as **year-on-year % change, period
average, monthly frequency**:

| Indicator.Code | Indicator.Name |
|---|---|
| `PCPI_PC_CP_A_PT` | Consumer Price Index, All items |
| `PCPIF_PC_CP_A_PT` | Food and non-alcoholic beverages |
| `PCPIFBT_PC_CP_A_PT` | Alcoholic Beverages, Tobacco, and Narcotics |
| `PCPIA_PC_CP_A_PT` | Clothing and footwear |
| `PCPIH_PC_CP_A_PT` | Housing, Water, Electricity, Gas and Other Fuels |
| `PCPIHO_PC_CP_A_PT` | Furnishings, household equipment and routine household maintenance |
| `PCPIM_PC_CP_A_PT` | Health |
| `PCPIT_PC_CP_A_PT` | Transport |
| `PCPIEC_PC_CP_A_PT` | Communication |
| `PCPIR_PC_CP_A_PT` | Recreation and culture |
| `PCPIED_PC_CP_A_PT` | Education |
| `PCPIRE_PC_CP_A_PT` | Restaurants and hotels |
| `PCPIO_PC_CP_A_PT` | Miscellaneous goods and services |

A 14th row, *Insurance and Financial Services*, exists in the master template
(`outputs/ckan/bk/template.csv`) but is **dropped** by `adh_create_inflation_db.py`.

**53 African countries** are in the published database. Only **28** are actively
maintained by ADH (they have a `data/<country>/` folder); the other 25 come straight from
the IMF overlay.

---

## 2. Prerequisites

| Requirement | Notes |
|---|---|
| **Python 3.8+** with **pandas 2.x** | Developed and run in **Spyder** (Anaconda). The scripts are written as Spyder cell scripts (`#%%`) and are meant to be run **with the repo root as the working directory**. |
| **Java (JRE 8+)** | Required by `tabula-py`, which does the PDF table extraction for most countries. Without Java those scripts fail. |
| **Internet access** | The `translate` package calls the MyMemory web API for the French/Portuguese countries. It is rate-limited and can fail quietly — see §7. |
| **LibreOffice Calc or Excel** | For editing `Notes.ods` and hand-editing `<country>_output.csv`. |
| **CKAN publisher rights** | Needed for Step 6. |

### Python packages

There is no `requirements.txt` in the repo. The imports across all scripts are:

```
pandas  numpy  openpyxl  xlrd            # xlrd needed for the .xls files (Nigeria, Tanzania)
tabula-py                                # + a working Java install
translate                                # French/Portuguese indicator names
fuzzywuzzy  python-Levenshtein           # Chad indicator matching
matplotlib  altair  altair_viewer        # optional: adh_quality_check.py only
```

---

## 3. Repository layout

```
adh_inflation_database_v2/
├── adh_prep_imf_New25.py          Step 2 — current IMF extractor (2025 IMF portal format)
├── adh_prep_imf.py                legacy IMF extractor (old bulk .zip) — superseded
├── adh_prep_<country>.py          Step 4 — one per automated country (23 of them)
├── adh_prep_data.py               optional: runs many adh_prep_<country>.py scripts in a loop
├── adh_create_inflation_db.py     Step 5 — combines everything into the CKAN outputs
├── adh_create_inflation_db_back_up.py   older copy of the above (superseded, reference only)
├── adh_quality_check.py           optional: Altair bar chart to eyeball the combined output
├── adh_check_update.py            debug: check whether pandas .update() overwrites with NaN
├── adh_extract_from_ckan.py       debug: rebuild one country's _output.csv from the CKAN base
├── adh_explore_imf.py             debug: exploratory version of the legacy IMF extractor
├── adh_inzpect_zimbabwe.py        one-off: Zimbabwe 2019 vs 2020 rebase investigation
├── adh_dynamic_template.py        one-off: prototype of the multi-line-label joiner
├── ah_reshape.py                  one-off: standalone version of the reshape/compare block
├── fuzzy_translate.py             one-off: fuzzy matching helper for translated indicator names
├── Notes.ods                      THE monthly worksheet — see Step 3
├── data/
│   ├── codeList.csv               legacy Indicator.Name -> Indicator.Code map (comma-separated)
│   ├── codeList_n.csv             *** REQUIRED by adh_prep_imf_New25.py but NOT IN THIS REPO ***
│   ├── imf_country_codes.pdf      reference
│   ├── inflationdatacountrylist.csv  reference: source links per country/region
│   ├── imf/
│   │   ├── raw/                   put the single IMF bulk download here
│   │   ├── imf_country_codes.csv  imf_code,iso_code,country — all IMF countries
│   │   ├── map_imf_iso_code.csv   the 54 African countries — this is the Africa filter
│   │   ├── african_country_iso_codes.csv  reference (56 rows; not used by the pipeline)
│   │   └── combined_imf_template.csv      reference
│   └── <country>/
│       ├── raw/                   downloaded source PDFs / XLSX / CSV
│       │   └── data_log.txt       ledger of already-processed raw files (see §7)
│       ├── csv/
│       │   ├── <country>_output.csv      *** the ADH system of record for that country ***
│       │   ├── <country>_template.csv    indicator skeleton for that country
│       │   ├── translation_template.csv  (Chad, Mozambique, Niger, Togo only)
│       │   └── bk/                       per-month intermediate CSVs, archived by Step 5
│       ├── Notes.txt / website.txt       country-specific hints
│       └── tabula-*.sh                   recorded tabula-java extraction geometry
└── outputs/
    ├── imf/                       CPI_<date>_NewtimeSeries_africa.xlsx (current), bk/ (previous)
    └── ckan/                      the three publishable files (current), bk/ (previous + fixtures)
        └── bk/
            ├── template.csv       master 53 countries x 14 indicators skeleton — READ BY MOST SCRIPTS
            ├── africadata3.csv    authoritative Country <-> iso_code map used to repair names
            └── africainflationdata.csv, reshape_template.csv   comparison fixtures
```

`outputs/ckan/bk/template.csv` and `outputs/ckan/bk/africadata3.csv` are **inputs**, not
backups, despite living in `bk/`. Do not delete them.

---

## 4. Monthly run — step by step

### Step 0 — Set up the working copy

Clone or download the repo so that you have `...\adh_inflation_database_v2\` with `data\`
and `outputs\` beneath it. The scripts use **relative paths from the repo root**, so your
Spyder working directory must be the repo root. Four lines still contain absolute paths
and must be edited once (see Step 5).

### Step 1 — Download the IMF CPI bulk file

1. Go to the IMF CPI dataset: <https://data.imf.org/en/datasets/IMF.STA:CPI>
2. Click **Download**. You get a ~347 MB CSV named something like
   `dataset_2025-11-24T08_30_48.624811811Z_DEFAULT_INTEGRATION_IMF.STA_CPI_5.0.0.csv`
3. Save it to `data\imf\raw\` and **delete every other file in that folder**. The script
   takes `glob('./data/imf/raw/*')[0]`, so a leftover file will be picked up instead.
4. Record the download date in `Notes.ods`.

### Step 2 — Extract the African CPI time series

Run **`adh_prep_imf_New25.py`** from the repo root.

It filters the bulk file to `TYPE_OF_TRANSFORMATION = 'Period average, Year-over-year
(YOY) percent change'`, `INDEX_TYPE = 'Consumer price index (CPI)'` and
`FREQUENCY = 'Monthly'`; keeps only the 54 African ISO codes listed in
`data/imf/map_imf_iso_code.csv`; keeps date columns from `2008M1` onwards; and normalises
IMF country and indicator names to the ADH spellings.

* Moves any existing `outputs/imf/*.xlsx` to `outputs/imf/bk/`
* Writes `outputs/imf/CPI_<MM-DD-YYYY HH-MM-SS>_NewtimeSeries_africa.xlsx`
  (the date/time comes from the IMF filename, not from today)

> **This script needs `data/codeList_n.csv`, which is not committed to this repo.** It
> must be a **semicolon-separated** file with columns `Indicator.Name;Indicator.Code`,
> keyed on the **raw IMF `COICOP_1999` labels** (e.g. `All Items`, `Housing, water,
> electricity, gas and other fuels`), because the merge happens *before* the name-tidying
> step. See `HANDOVER_QUESTIONS.md`.

`adh_prep_imf.py` is the **old** extractor for the previous IMF bulk `.zip` format. It is
kept for reference only; do not run it against the new download.

### Step 3 — Work out which countries need an update

Open **`Notes.ods`**. This is the operational worksheet for the month, and it is where the
human judgement lives.

Columns (current working version):
`Country | Language | Analysis Method | Publication_Order | latest IMF data |
latest ADH country data | Country publication day | Date site checked |
data on date checked | analysed | checked | Comments | Source`

How to use it:

1. Fill **latest IMF data** for each country from
   `outputs/imf/CPI_*_NewtimeSeries_africa.xlsx` — the last month that has a value.
2. **Country publication day** is the day of the month that country normally publishes.
   Do not bother checking before that date.
3. Open the **Source** link and see whether the country has published something more
   recent than the IMF has.
4. Flags use `1 = yes`, `0 = no`:
   * **data on date checked** — was there newer data on the site?
   * **analysed** — has it been entered manually, or run through the script?
   * **checked** — has `<country>_output.csv` been verified against the source document?
5. **Comments** tells you *where in the document* the numbers are. Read it before you
   start hunting.

Only countries listed in `Notes.ods` publish on the web; everything else is IMF-only.

> ⚠️ The `Notes.ods` committed to this repo was last updated in **March 2023** and still
> has the older, shorter column set (`Country, Language, Analysis Method, latest data,
> data this month, analysed, checked, Comments, Source`). The version described above
> lives on the original developer's machine.

### Step 4 — Update each country

For every country flagged as having new data, download the source document into
`data\<country>\raw\`. Then:

#### 4a. Countries with a script

Run `adh_prep_<country>.py` from the repo root. The script:

1. Globs `data/<country>/raw/*.pdf` (or `*.xlsx` / `*.xls` / `*.csv`)
2. Skips any file already listed in `data/<country>/raw/data_log.txt`
3. Extracts the table, maps the local indicator names onto the ADH indicator names using
   `outputs/ckan/bk/template.csv`, and writes `data/<country>/csv/<same filename>.csv`
4. Appends the processed filename to `data_log.txt`

**You then copy the last column of that CSV and paste it as a new last column of
`data/<country>/csv/<country>_output.csv`.** This step is manual — no script does it.
Save and close the file afterwards.

*Worked example (Namibia):* download `Namibia-CPI-<Month>-<Year>-Excel-Tables-.xlsx` from
<https://nsa.org.na/publications/> → save to `data\namibia\raw\` → run
`adh_prep_namibia.py` → open the produced
`data\namibia\csv\Namibia-CPI-<Month>-<Year>-Excel-Tables-.csv` → copy its last column
into `data\namibia\csv\namibia_output.csv`.

`adh_prep_data.py` loops over most of the country scripts in one go. It excludes
`burundi, eswatini, malawi, senegal, sierra_leone, togo, zambia, zimbabwe`, and will
report a harmless failure for `angola` and `sudan` because no script exists for them.

#### 4b. Manual countries

There is no script; open the source document and type the values into
`data\<country>\csv\<country>_output.csv` directly. Use the **Comments** column of
`Notes.ods` to find the right table.

*Worked example (Algeria):* download the monthly IPC PDF from <https://www.ons.dz/> →
save to `data\algeria\raw\` → find the CPI table (see `data/algeria/Notes.txt`: Algeria
lumps recreation and education together) → type the values into
`data\algeria\csv\algeria_output.csv` → save and close.

#### Country reference

| Country | Language | Method | Input | Notes |
|---|---|---|---|---|
| Algeria | French | manual | PDF | Recreation and education are reported together |
| Angola | Portuguese | manual | PDF | One line of data, table around p.10; pull the rest from IMF first |
| Burkina Faso | French | script (tabula) | PDF | Multi-line labels rejoined by a template function |
| Burundi | French | script, run alone | PDF | Excluded from `adh_prep_data.py` |
| Chad | French | script (tabula + fuzzy) | PDF | Month/year read from **fixed character offsets** in the path — see §7 |
| Eswatini | English | manual | PDF | Central Bank Economic Review & Inflation Report |
| Ethiopia | English | script (tabula) | PDF | Site has certificate problems |
| Gabon | French | script (tabula) | PDF | Publishes irregularly |
| Gambia | English | script | **XLSX** | Use `adh_prep_gambia_NEW_25xl.py` (2025 format). `adh_prep_gambia.py` is the old PDF version |
| Ghana | English | script (tabula) | PDF | Flagged "Manual" in Notes.ods but a script exists — confirm which is current |
| Kenya | English | script (tabula) | PDF | |
| Liberia | English | script (tabula) | PDF | CBL statistical report, REAL SECTOR 2.2c (~p.37) |
| Malawi | English | manual | PDF | One line of data, "stats flash" |
| Mozambique | Portuguese | script + translate | XLSX | Quadro 11 *Homólogas*. Ignore Oct–Dec 2022: cities only |
| Namibia | English | script | XLSX | Sheet **"Tab 4"**; filename must contain `Jan`…`Dec` plus a 4-digit year |
| Niger | French | script + translate | PDF | Technical note |
| Nigeria | English | script | XLS/XLSX | **Computes YoY from the index** with hard-coded row slices per year — see §7 |
| Rwanda | English | script | PDF | Last table, 3rd-last column |
| Senegal | French | script, run alone | PDF/DOCX | Table 1, "12 mois" column. Excluded from `adh_prep_data.py` |
| Sierra Leone | English | script, run alone | PDF | Excluded from `adh_prep_data.py` |
| South Africa | English | script (tabula) | PDF | Download the **additional tables** PDF only |
| Sudan | English/Arabic | manual | PDF | One line of data, CBoS periodicals |
| Tanzania | English | script | **XLS** | Needs `xlrd`; calculation required from the summary |
| Togo | French | script + translate, run alone | PDF | Excluded from `adh_prep_data.py` |
| Tunisia | French/English | script | **CSV** (`;`-separated) | INS publishes CSV directly; script has no `data_log` and processes only the last file it finds |
| Uganda | English | script | XLSX | |
| Zambia | English | script, run alone | PDF | Monthly bulletin, "Annual Inflation Trends by CPI Main Groups" |
| Zimbabwe | English | manual | PDF | **Use the 2019-rebase series**, Table 9.2 (RBZ Monthly Economic Review) |

Source links live in the `Source` column of `Notes.ods` and in
`data/inflationdatacountrylist.csv`.

### Step 5 — Build the combined database

Before the first run on a new machine, edit **`adh_create_inflation_db.py`** and replace
the four hard-coded absolute paths (they still point at
`C:\Users\heiko\Documents\Work\OCL\ADH\Inflation\...`):

| Line | Variable |
|---|---|
| 33 | `bk_folder` |
| 39 | `files = glob.glob(...)` |
| 188 | `full_path` |
| 189 | `full_path_bk` |

Then check the preconditions:

* `outputs/ckan/` contains **exactly one generation** of output files — the previous
  month's `<date>_combined_imf_database.csv` plus its reshaped/power_BI siblings. The
  script reads `glob('./outputs/ckan/*.csv')[0]`, which is the **alphabetically first**
  file, not the newest.
* `outputs/imf/` contains **exactly one** `.xlsx` — this month's Step 2 output.
* Every folder in `data/` except `imf` has a `<country>_output.csv`.
* `data/` contains no loose files other than `codeList.csv`, `imf_country_codes.pdf` and
  `inflationdatacountrylist.csv`. The script removes exactly those three names from its
  country list; anything else is treated as a country folder and crashes the loop.

Run `adh_create_inflation_db.py`. It:

1. Loads the previous CKAN combined file as the base and drops *Insurance and Financial
   Services*
2. Repairs `Country` ↔ `Geography` (iso_code) using `outputs/ckan/bk/africadata3.csv`
3. Loads the IMF xlsx, converts `2025M1`-style headers to end-of-month dates, and overlays
   **only the last 12 months**
4. Aborts with `"There is an issue with names"` if the IMF file contains a country name
   the base does not know — fix the mapping in `adh_prep_imf_New25.py` and re-run Step 2
5. Loops over every `data/<country>/`, overlays `<country>_output.csv`, and moves that
   country's intermediate CSVs into `data/<country>/csv/bk/`
6. Archives the previous `outputs/ckan/*.csv` into `outputs/ckan/bk/` and writes the three
   new files

Outputs, all in `outputs/ckan/`, all the same data in different shapes:

| File | Shape | Used by |
|---|---|---|
| `<today>_combined_imf_database.csv` | wide — one row per country × indicator, one column per month | CKAN, and next month's base |
| `<today>_reshaped_imf_database.csv` | one row per country × date, one column per indicator code | CKAN |
| `power_BI.csv` | melted — `country, indicator, iso_code, date, value` | Power BI |

Optional check: set `check = True` near the bottom of the script (or run
`adh_quality_check.py`) to compare against the previous version and write
`<today>_errors.csv`.

### Step 6 — Publish to CKAN

Upload the three files to
<https://ckan.africadatahub.org/dataset/imf-africa-inflation-database>. You need
management rights on the dataset.

For each resource: click the file name under **Data and Resources** → **Manage / Edit
Resource** → **Clear Upload** → upload the new file from `outputs\ckan\` → save.

Then update the `checked` flags in `Notes.ods` and commit the run to git.

---

## 5. Which countries come from where

| | Count | |
|---|---|---|
| Countries published in the database | 53 | from `outputs/ckan/bk/template.csv` |
| Of which actively maintained by ADH | 28 | have a `data/<country>/` folder |
| Of which have an extraction script | 23 | `adh_prep_<country>.py` |
| Of which are fully manual | 5 | Angola, Eswatini, Malawi, Sudan, Zimbabwe |
| Remaining, IMF-only | 25 | updated solely by the Step 5 IMF overlay |

`data/imf/map_imf_iso_code.csv` lists 54 African countries; the CKAN base has 53.
**Eritrea (ERI)** is in the filter but has no row in the template, so any Eritrean data in
the IMF file is silently discarded by `DataFrame.update()`.

---

## 6. Verification before publishing

* Row count of the combined file = `53 countries × 13 indicators = 689` data rows.
* Date columns run contiguously from `2008-01-31` to the latest month, **in date order**.
* No column exists for a month that has not happened yet.
* Spot-check two or three of the countries you updated against the source PDF.
* `adh_quality_check.py` renders an Altair bar chart with country and date dropdowns for
  eyeballing a single country-month.

---

## 7. Gotchas and landmines

These are the things that will bite you. Most are not visible from the code.

1. **The base file is chosen by alphabetical order, not by date.**
   `adh_create_inflation_db.py` uses `glob('./outputs/ckan/*.csv')[0]`. With this repo's
   current contents that resolves to `2023-03-13_combined_imf_database.csv`, so a run from
   a fresh clone would rebuild on top of March 2023 and discard everything since.
   **Always confirm which file the script picked up before trusting the output.**

2. **`data_log.txt` suppresses re-processing.** If you re-download a file with the same
   name (for example because the agency corrected it), the script will skip it. Delete the
   line from `data/<country>/raw/data_log.txt` to force a re-run. The Gambia log already
   contains duplicate entries from repeated runs — duplicates are harmless.

3. **`adh_prep_gambia_NEW_25xl.py` writes Windows-style paths into `data_log.txt`** while
   the historical entries use forward slashes, so previously-processed files can be
   reprocessed. Noisy but harmless.

4. **Chad parses the month and year from fixed character positions**
   (`data_path[30:32]` and `data_path[33:]`). This only works if the path is exactly
   `./data/chad/raw/Bulletin-INPC-MM-YYYY`. Renaming the file, or running from a different
   working directory, produces wrong dates or a crash.

5. **Nigeria's script has year-specific row slices** (`df.iloc[324:336]` for 2022,
   `df.iloc[336:]` for 2023) and derives YoY inflation from the index itself. It needs
   editing every year and is almost certainly stale now.

6. **Gambia (2025) depends on exact worksheet geometry** — sheet
   `"NChained Link Series to Publish"`, rows `[14,15,28,31,37,41,46,47,48,49,52,53,54]`.
   Any layout change at GBoS breaks it quietly.

7. **New date columns are appended at the end, not inserted in date order.** And if the
   base file is more than 5 months behind the IMF file, the IMF overlay step only compares
   the last 5 columns of each and never creates the intervening months. The committed
   `2025-11-24` output shows both symptoms: `2025-05-31 … 2025-07-31` sit *after*
   `2025-12-31`, and there is a gap between `2025-04-30` and `2025-08-31`.

8. **Empty future columns can appear.** The IMF wide file carries columns for the whole
   calendar year, so `2025-11-30` and `2025-12-31` were created with no data in them. Drop
   them before publishing.

9. **Translation is a live web call.** `translate.Translator` hits the MyMemory API and is
   rate-limited. On failure the script falls back to the untranslated string, which then
   fails to match the template and produces a blank row — check the French/Portuguese
   country outputs for empty values. `translation_template.csv` (Chad, Mozambique, Niger,
   Togo) caches known translations and is tried first.

10. **`tabula` needs Java.** If PDF countries all fail at once, check `java -version`
    before debugging the scripts.

11. **`outputs/ckan/bk/` is git-ignored** (see `.gitignore`) but four files in it are
    tracked and are pipeline **inputs**: `template.csv`, `africadata3.csv`,
    `africainflationdata.csv`, `reshape_template.csv`. New backups written there will not
    be committed.

12. **Country name spellings are load-bearing.** The mapping in `adh_prep_imf_New25.py`
    (`Gambia, The` → `The Gambia`, `Congo, Republic of` → `Republic of Congo`, and so on)
    must match `outputs/ckan/bk/template.csv` exactly. When the IMF renames a country,
    Step 5 stops with `"There is an issue with names"`.

13. **`prep()` assumes the first five columns** of `<country>_output.csv` are
    `Indicator.Name, Indicator.Code, Country, _id, Geography`. Do not reorder them or add
    columns when hand-editing.

14. **Close the file before running.** LibreOffice/Excel lock files (`.~lock.<name>#`) do
    not break the scripts, but an unsaved edit will silently be missed.

15. **Adding `codeList_n.csv` to `data/` breaks Step 5** unless you also add
    `countries.remove('codeList_n.csv')` alongside the other `countries.remove(...)` lines
    in `adh_create_inflation_db.py` — see §4 Step 5 preconditions.

---

## 8. Known problems with the current repository state

| Issue | Impact |
|---|---|
| `data/codeList_n.csv` is missing | `adh_prep_imf_New25.py` (Step 2) cannot run from a clean clone |
| `outputs/ckan/2025-11-24_combined_imf_database.csv` contains **git merge-conflict markers** (`<<<<<<<<` on line 1, `========` on line 692, `>>>>>>>>` on line 1383) — it is the 2023 file concatenated with the 2025 file | The most recent committed output is corrupt; the real 2025 data is lines 693–1382 |
| `outputs/ckan/` holds two generations of output (2023-03-13 and 2025-11-24) | Step 5 would pick the 2023 file as its base (§7.1) |
| `data/imf/raw/` still holds the **legacy** `CPI_..._timeSeries.zip` | Must be deleted before Step 1 |
| `outputs/imf/` still holds the **legacy-format** 2023 xlsx | Step 5 would use it if Step 2 were skipped |
| All `data/<country>/csv/<country>_output.csv` files stop in early 2023 | The 2025 run demonstrably used newer local copies that were never committed |
| `Notes.ods` is the March 2023 version with the old column set | The workflow described in Step 3 cannot be followed with the committed file |
| No `requirements.txt` or environment file | The environment must be reconstructed by hand |

---

## 9. Handover

Open questions for the original developer are collected in
**[`HANDOVER_QUESTIONS.md`](HANDOVER_QUESTIONS.md)**. Several of them block a clean
end-to-end run.
