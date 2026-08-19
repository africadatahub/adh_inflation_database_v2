# Handover questions for the original developer

Compiled from a full read of the repository (all 38 Python scripts, `Notes.ods`, the
reference CSVs, the committed outputs and the git history) on 2026-08-19.

The pipeline is only about half code. The other half is hand-work — checking country
sites, reading numbers out of PDFs, pasting columns, editing the worksheet — and much of
that lives on the original developer's machine, not in git. The questions below are what
a new maintainer cannot answer from the repository alone.

Ordered: **blockers** first (you cannot complete a run without an answer), then
**correctness**, then **process**, then **nice-to-know**.

---

## A. Blockers — a clean clone cannot run end-to-end without these

### A1. `data/codeList_n.csv` is missing

`adh_prep_imf_New25.py:68` reads `./data/codeList_n.csv` with `sep=';'`. The file is not
in the repository and never has been (`git log` shows only `data/codeList.csv`).

It is the map from the IMF's `COICOP_1999` label to `Indicator.Code`, and the merge on
line 84 happens **before** the name-tidying replacements on lines 112–116 — so it must be
keyed on the *raw* IMF labels (`All Items`, `Housing, water, electricity, gas and other
fuels`, `Alcoholic beverages, tobacco and narcotics`), not the cleaned ADH ones.

* **Can you send the actual file?** We can reconstruct 13 rows from the
  `Indicator.Name`/`Indicator.Code` pairs in the 2025-11-24 output, but we would be
  guessing at the exact raw-label spellings, and a wrong guess silently produces `NaN`
  indicator codes rather than an error.
* Was `codeList_n.csv` intentionally kept out of git, or just missed?
* Does the IMF ever add or rename a `COICOP_1999` category, and if so how do you notice?

### A2. Adding `codeList_n.csv` to `data/` appears to break Step 5

`adh_create_inflation_db.py:158-164` does `os.listdir('./data/')` and then explicitly
removes `imf`, `codeList.csv`, `imf_country_codes.pdf` and `inflationdatacountrylist.csv`.
Anything else in `data/` is treated as a country folder, so `codeList_n.csv` would make
the loop try to open `./data/codeList_n.csv/csv/codeList_n.csv_output.csv`.

* Do you keep `codeList_n.csv` somewhere else, or is your local copy of
  `adh_create_inflation_db.py` patched with an extra `countries.remove(...)`?
* Is your local `adh_create_inflation_db.py` otherwise the same as the committed one? The
  committed version was last changed in March 2023, but a run clearly happened in
  November 2025.

### A3. `Notes.ods` in the repo is two years out of date

The committed `Notes.ods` was last updated in commit `17bedbf` ("March Week 2", 2023) and
has columns `Country, Language, Analysis Method, latest data, data this month, analysed,
checked, Comments, Source`.

The README describes a richer sheet: `Country, Language, Analysis Method,
Publication_Order, latest IMF data, latest ADH country data, Country publication day,
Date site checked, data on date checked, analysed, checked, Comments, Source`.

* **Please send the current `Notes.ods`.** Without it we do not have: the publication day
  of the month per country, the publication order, current source URLs, or the current
  per-country "manual vs script" designation.
* What does **Publication_Order** drive — is it just the order you work through countries,
  or does something depend on it?
* Should `Notes.ods` be committed each month going forward, or do you deliberately keep it
  out of git?

### A4. The country `_output.csv` files in git stop in early 2023

Every `data/<country>/csv/<country>_output.csv` in the repo ends around January–February
2023. But the 2025-11-24 output contains columns for `2025-05-31`, `2025-06-30` and
`2025-07-31` that could only have come from country outputs — so your local copies run to
at least mid-2025.

* **Can you send the current `data/` tree**, or at least the 28 `<country>_output.csv`
  files? These are the ADH system of record and they are not recoverable from anywhere
  else.
* Was there a reason data stopped being committed after March 2023 (repo size, git-lfs,
  workflow change)?

### A5. What is the true current CKAN base file?

`adh_create_inflation_db.py:87-90` picks `glob('./outputs/ckan/*.csv')[0]` — the
**alphabetically first** file, not the newest. The repo now has both a 2023-03-13 and a
2025-11-24 generation, so a fresh run would build on top of March 2023.

* Which file is the true current base? Is it the 2025-11-24 one, or has there been a run
  since?
* Do you clear `outputs/ckan/` down to a single generation before each run, or is the
  alphabetical ordering something you have been relying on?

---

## B. Correctness — things that look wrong in the committed output

### B1. `2025-11-24_combined_imf_database.csv` contains git conflict markers

The committed file has:

```
line    1: <<<<<<<< Updated upstream:outputs/ckan/2023-03-13_combined_imf_database.csv
line  692: ========
line 1383: >>>>>>>> Stashed changes:outputs/ckan/2025-11-24_combined_imf_database.csv
```

Lines 2–691 are the 2023 file; lines 693–1382 are the real 2025 data. It looks like a
`git stash pop` conflict was committed unresolved.

* Was the **clean** version of this file the one uploaded to CKAN, or did the corrupt one
  go up too?
* Do you have the clean file so we can replace what is in git?

### B2. Out-of-order and empty date columns in the 2025 output

In the 2025 section, the date columns end:

```
… 2025-01-31, 2025-02-28, 2025-03-31, 2025-04-30, 2025-08-31, 2025-09-30,
   2025-10-31, 2025-11-30, 2025-12-31, 2025-05-31, 2025-06-30, 2025-07-31
```

Two separate problems:

* `2025-05/06/07` were appended after `2025-12-31` — new columns are added with
  `df_ckan[col] = np.nan` / `= ''`, which always appends at the end.
* `2025-11-30` and `2025-12-31` exist but are **100% empty** (0 non-null values in 689
  rows) — the IMF wide file carries a column for every month of the calendar year.

Additionally, `2025-05` through `2025-07` were only created by the *country* loop, not by
the IMF overlay: the IMF step only compares `ckan_cols[-5:]` against `df_cols[-5:]`, so if
the base is more than five months behind, intervening months are never created.

* Do you re-sort the columns and drop the empty future ones by hand before uploading to
  CKAN? If so, in what tool, and is that step recorded anywhere?
* Do the CKAN consumers (the Inflation Observer, Power BI) care about column order, or do
  they read by column name?
* Has the "more than 5 months behind" gap ever bitten you, and if so how did you patch it?

### B3. Eritrea is filtered in but has nowhere to land

`data/imf/map_imf_iso_code.csv` lists 54 African countries; `outputs/ckan/bk/template.csv`
has 53. **ERI (Eritrea)** is in the IMF filter but has no template row, so
`df_ckan.update(df_imf)` silently drops it.

* Is that deliberate (the IMF has no usable Eritrea CPI), or an oversight?
* Same question for `ESH` and `SHN`, which appear in
  `data/imf/african_country_iso_codes.csv` (56 rows) but not in `map_imf_iso_code.csv`.
  Is `african_country_iso_codes.csv` dead reference data, or does something use it?

### B4. Is `_id` still meaningful?

`_id` in `outputs/ckan/bk/template.csv` is populated for some indicators (5.0, 1.0, 7.0,
2.0, 3.0, 6.0, 9.0, 4.0) and `NaN` for others (Alcoholic Beverages, Communication,
Education, …). It is carried through to the published files.

* What is `_id` — a CKAN row id, a legacy display-order field, something else?
* Does anything downstream depend on it? Can it be dropped?

### B5. Zimbabwe rebase

`adh_inzpect_zimbabwe.py` and `Notes.ods` both say "be sure to use rebase 2019 data".
The script header notes Zimbabwe rebased 2019 → 2020 and that RBZ data is "incorrectly
columned from Jan–July 2020, so just ignore".

* Is the 2019-rebase rule still current in 2025/2026, or has it been superseded?
* Which source is authoritative now — RBZ Monthly Economic Review, or ZIMSTAT?

---

## C. Process — the hand-work that is not in the code

### C1. The copy-paste step

Every scripted country produces `data/<country>/csv/<file>.csv`, and someone then copies
its last column into `<country>_output.csv`. Nothing automates this.

* Do you do this in LibreOffice or Excel?
* Do you paste **values only**, and do you ever have to re-round or re-sign anything?
* What do you do when the script's mapping leaves a blank cell — leave it blank, or fill
  from the IMF?

### C2. Which countries are actually manual right now?

Three sources disagree:

| Country | `Notes.ods` says | `adh_prep_data.py` excludes | `adh_prep_<c>.py` exists |
|---|---|---|---|
| Ghana | Manual | no | **yes** |
| Burundi | Manual | yes | yes |
| Senegal | Manual | yes | yes |
| Zambia | Manual | yes | yes |
| Sierra Leone | — | **yes** | yes |
| Togo | — | **yes** | yes |
| Angola | Manual | no | **no** (loop will fail) |
| Sudan | — | no | **no** (loop will fail) |

* Which is right? For Ghana in particular — is `adh_prep_ghana.py` still used, or has it
  been abandoned in favour of manual entry?
* For Burundi / Senegal / Sierra Leone / Togo / Zambia: are those scripts excluded from
  `adh_prep_data.py` because they are broken, because they need per-file supervision, or
  for another reason?
* Do you actually use `adh_prep_data.py`, or run each country script individually?

### C3. Country scripts that have almost certainly rotted

Several scripts encode the exact layout of a 2022-era publication:

* **Nigeria** — `df.iloc[324:336]` = 2022, `df.iloc[336:]` = 2023, and it computes YoY
  from the index. This must have needed editing for 2024 and 2025.
* **Gambia** — `adh_prep_gambia_NEW_25xl.py` hard-codes sheet
  `"NChained Link Series to Publish"` and rows `[14,15,28,31,37,41,46,47,48,49,52,53,54]`.
* **Chad** — parses month/year from character offsets `data_path[30:32]` / `[33:]`.
* **Ghana** — has a `special = [...]` list of 2021 filenames with different handling.

Questions:

* **Which country scripts still work as of your last run?** A simple pass/fail list would
  save a lot of time.
* Which ones do you edit each year (Nigeria at minimum) and what exactly do you change?
* Are there newer local versions of any of these that were never pushed — as
  `adh_prep_imf_New25.py` and `adh_prep_gambia_NEW_25xl.py` were, in a single "Add files
  via upload" commit?

### C4. Manual-country extraction recipes

For Angola, Eswatini, Malawi, Sudan and Zimbabwe there is no code at all — just short
hints in the `Comments` column of `Notes.ods` ("one line of data — table around pg 10",
"stats flash", "Table 9.2").

* Can you write out, per country, **which table, which row, which column** you read? Even
  a screenshot of a marked-up PDF page would do. This is the single largest piece of
  undocumented knowledge in the project.
* Same for the calculated ones — Nigeria and Tanzania are both flagged "calculation
  required" in `Notes.ods`. What is the calculation, and does it differ from what the
  scripts do?

### C5. Verification / QA

`adh_create_inflation_db.py` has a `check = False` block that diffs against
`outputs/ckan/bk/reshape_template.csv` and writes `<date>_errors.csv`. There is one
committed example (`2022-10-25_errors_checked.csv`).

* Do you still run that check? The comment says *"change to previous db once this has been
  run for a month"* — was that ever done, or does it still point at the 2022 template?
* What is the actual sign-off before uploading to CKAN? The `checked` column in
  `Notes.ods` implies a per-country visual check against the source document — is that
  done every month, or only for countries you updated?
* Is `adh_quality_check.py` still usable? It uses `alt.selection_single`, which was
  removed in Altair 5.

### C6. CKAN upload

The README says: Edit Resource → Clear Upload → upload.

* Are there exactly three resources on the dataset (combined, reshaped, power_BI), and do
  the CKAN resource names have to stay the same? The uploaded files have a date in the
  filename, which changes every month.
* Does anything need to be updated on the dataset besides the three files — a "last
  updated" field, a description, a data dictionary?
* Does the Inflation Observer dashboard need any action after the upload (cache clear,
  rebuild), or does it pick the new data up automatically?
* Who else holds CKAN management rights?

### C7. Cadence and ownership

* Is this monthly, or "whenever the IMF updates"? The git history shows roughly weekly
  runs in late 2022 / early 2023, then nothing until November 2025.
* Roughly how long does a full run take, end to end?
* Was anything else agreed with ADH about the schedule?

---

## D. Environment and tooling

* **Which Python / pandas version do you run?** We have verified the scripts against
  pandas 2.0.3. All 23 country scripts use the chained-assignment idiom
  `df['col'][mask] = value`, which is a hard error in pandas 3.
* Which Java version is installed for `tabula-py`?
* `fuzzywuzzy` is deprecated in favour of `thefuzz`, and the `translate` package's
  MyMemory backend is rate-limited and anonymous. Have you hit rate limits, and is there
  an API key configured anywhere locally?
* Two scripts (`adh_prep_imf_New25.py`, `adh_prep_gambia_NEW_25xl.py`) were exported from
  **Google Colab**. Are the Colab notebooks the master copies? If so, can we get the links
  / a copy, since the exported `.py` files have Colab scaffolding commented out and may
  have drifted?
* `adh_prep_imf_New25.py:31` does `file[0].split('\\')[1]` — Windows-only path splitting.
  Is the pipeline expected to stay Windows-only, or would you like it made portable?

---

## E. Nice-to-know / possible dead code

Confirm whether these can be deleted, so the next maintainer is not misled:

| File | Our reading |
|---|---|
| `adh_prep_imf.py` | superseded by `adh_prep_imf_New25.py` |
| `adh_create_inflation_db_back_up.py` | older copy of `adh_create_inflation_db.py` |
| `adh_prep_gambia.py` | superseded by `adh_prep_gambia_NEW_25xl.py` |
| `adh_explore_imf.py` | exploratory version of the legacy IMF extractor |
| `ah_reshape.py` | standalone copy of the reshape block already inside `adh_create_inflation_db.py` |
| `adh_dynamic_template.py` | prototype of the multi-line-label joiner now inlined in the country scripts |
| `fuzzy_translate.py` | only Chad imports `fuzzywuzzy`, and it does so directly |
| `adh_check_update.py` | debugging aid, hard-codes a 2022 IMF filename |
| `adh_inzpect_zimbabwe.py` | one-off rebase investigation |
| `data/imf/combined_imf_template.csv` | reference only — nothing reads it |
| `data/inflationdatacountrylist.csv`, `data/imf_country_codes.pdf` | reference only — but Step 5 requires the filenames to exist (`countries.remove(...)` raises if they are gone) |
| `outputs/ckan/bk/africainflationdata.csv` | only read by `adh_extract_from_ckan.py` |
| `data/*/tabula-*.sh` | recorded tabula-java geometry — still useful for re-deriving extraction areas? |

Also: `outputs/ckan/bk/` is listed in `.gitignore` but four files inside it are tracked
and are pipeline **inputs** (`template.csv`, `africadata3.csv`, `reshape_template.csv`,
`africainflationdata.csv`). Was that intentional, and is `template.csv` ever regenerated —
`adh_prep_ckan.py` writes it, but its own comment says it exists to fill in indicators
that some countries lack.
