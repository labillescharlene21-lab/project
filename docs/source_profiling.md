# Source Profiling Report
 
**Version:** 1.0 · **Code:** `src/analysis/profile_sources.py` · **Related:** [`source_inventory.md`](source_inventory.md), [`business_rules.md`](business_rules.md), [`data_dictionary.md`](data_dictionary.md), `config/mappings.yaml`
 
What each source contains, what its raw data actually looks like, the quality issues we found, how the pipeline handles each one, and what limitations remain. Full provider details (URLs, licences, access dates) are in [`source_inventory.md`](source_inventory.md); this page is about the data itself.
 
Every number here comes from a real run: either the ticket where it was first observed (in brackets, e.g. ING-3) or the profiling outputs in `outputs/profiling/`. Cells marked **FILL** must be copied from `outputs/profiling/profiling_summary.md` after the final run (section 2), never estimated.
 
---
 
## 1. Source summary
 
| # | source_code | Provider | Access | Raw format | Role | Coverage used | Size at source |
|---|---|---|---|---|---|---|---|
| 1 | `owq_gemstat` | UNEP GEMS/Water via Open Water Quality | Scripted export (GET) | CSV with 7 `#` comment lines | Observations: Africa, Asia, Latin America strata | Global, 2015–2025 | ~400K observations (all years) |
| 2 | `owq_eionet` | EEA / Eionet bathing water via Open Water Quality | Scripted export (GET) | CSV with 7 `#` comment lines | Observations: Europe stratum; main marine source | Europe, 2015–2025 | ~2.4M observations (all years) |
| 3a | `wqp_summary` | Water Quality Portal (USGS, EPA, NWQMC) | REST API, one request per state | CSV (zip) | Per-site annual sample counts, used **only for sampling** | US | ~6.9M fecal indicator observations nationally |
| 3b | `wqp_results` | Water Quality Portal | REST API, POST, 50 sites per request | CSV (zip), 63 columns | Observations: US stratum | Sampled US sites, 2015–2025 | as 3a |
| 4 | `open_meteo` | Open-Meteo (ERA5 reanalysis) | REST API, one request per 0.25° cell | JSON | Daily rainfall and temperature | Cells of sampled sites, 2015–2025 | Global, 1940 onward |
| 5 | `natural_earth` | Natural Earth | File download | Shapefile (zip) | Admin-1 regions for aggregation | Global | 4,596 features (ING-5) |
 
The Open Water Quality compilation behind sources 1 and 2 holds about 11.1 million observations from nine public sources ([`problem_statement.md`](problem_statement.md)).
 
## 2. How profiling was done
 
Profiling runs on the **raw** layer, before any cleaning, so it shows the data as delivered. It is read-only and makes no network calls.
 
```powershell
docker compose run --rm -T pipeline python -m src.analysis.profile_sources | Out-File -FilePath outputs\doc2_profiling_log.txt -Encoding utf8
```
 
For each source it reads the latest successful raw batch, only the files named by `data_files` in `config/schemas/{source_code}.yaml`, and writes `outputs/profiling/{source_code}.json` plus `outputs/profiling/profiling_summary.md`.
 
| Measure | Tabular sources (WQP, OWQ) | Open-Meteo | Natural Earth |
|---|---|---|---|
| Volume | rows, files, columns | cells, cell-days | features |
| Completeness | blank share per column | missing precipitation / temperature share | features with `iso_a2 = -99` |
| Coverage | distinct sites, date range, rows per year | days per cell (min / max) | countries |
| Vocabulary | top values of indicator, unit, detection condition, activity type, media, status, water body, region | n/a | CRS, invalid geometries |
| Values | numeric share, non-numeric texts, negatives, quantiles, most repeated values | max precipitation, dry-day share | n/a |
| Uniqueness | duplicate (site, date, indicator) rows | n/a | n/a |
 
Copy `outputs/profiling/profiling_summary.md` to `docs/evidence/doc2_profiling_summary.md` to commit it (`outputs/` is git-ignored).
 
### Profiling results (final run)
 
| source_code | batch_id | rows | distinct sites | date range | blank share (key columns) |
|---|---|---|---|---|---|
| `owq_gemstat` | FILL | FILL | FILL | FILL | FILL |
| `owq_eionet` | `owq_eionet_e86ef04d7716` (ING-1) | FILL | FILL | FILL | FILL |
| `wqp_summary` | FILL | FILL | FILL | n/a (yearly) | FILL |
| `wqp_results` | FILL | FILL | FILL | FILL | FILL |
| `open_meteo` | FILL | FILL cell-days | FILL cells | FILL | FILL |
| `natural_earth` | FILL | 4,596 features | n/a | n/a | FILL |
 
## 3. Findings per source
 
### 3.1 GEMStat (`owq_gemstat`)
 
| Finding | Evidence | Handling |
|---|---|---|
| Responses capped at 100,000 rows | `X-Row-Count` / `X-Row-Capped` headers (ING-2 spike) | Extractor chunks by year, then splits by `loc_type` when a chunk is capped |
| 7 `#` attribution lines before the header | STG-1 | Readers skip comment lines (`comment_prefix: "#"` in the schema file) |
| No site ID or record ID column; 9 columns only (`date`, `indicator`, `value`, `unit`, `latitude`, `longitude`, `source`, `region`, `water_body`) | ING-1 | Site ID = coordinates rounded to 5 decimals; record ID = SHA-1 of site, date, indicator (`mappings.yaml › record_id`) |
| Already harmonized by OWQ: MPN treated as CFU, censored values pre-filled at half the limit, cross-source duplicates removed at ~1 km | [`source_inventory.md`](source_inventory.md) | Accepted; the same censoring rule is applied to WQP so all sources match |
| Values cluster at **24,196** and **2,419,600** MPN/100 mL (lab reporting maximums, "at least this much") | README §15 | Exceedance and hotspot flags unaffected; severity understated for Latin America. Stated as a limitation |
| `region` can contain non-target countries (e.g. "Canada") | ING-2 | Strata assigned from the spatially joined country; US and CA excluded from `gemstat_latin_america` |
| **No African sites** with fecal indicator data; **Asia is coliform-only** | STG-1 | `gemstat_africa` and `gemstat_asia` strata are empty (coliforms are never scored). Reported as a data-availability finding |
| Sparse, irregular sampling in many countries | [`source_inventory.md`](source_inventory.md) | Eligibility filter: ≥ 24 primary-indicator samples across ≥ 3 years |
 
### 3.2 Eionet bathing water (`owq_eionet`)
 
| Finding | Evidence | Handling |
|---|---|---|
| Same layout, comment lines and row cap as GEMStat | ING-1 / ING-2 | Same extractor and readers |
| **Zero** fecal or total coliform rows in 2015–2025 | ING-1 | Expected (the bathing-water dataset covers E. coli and enterococci only); logged as expected-empty, not an error |
| **Zero E. coli / enterococci rows from 2022 onward** | ING-1 | Cause unconfirmed (reporting lag vs dataset cutoff). Persistence uses each site's most recent 5 *classified* years, so Europe effectively covers 2015–2021. Flagged VERIFY |
| `water_body = "other"` is **65%** of Eionet rows | STG-1 | Excluded: cannot be reliably classified as fresh or marine, and the wrong realm means the wrong threshold. Europe marine still has 224 eligible sites for 60 slots |
| Bathing-season sampling only | [`source_inventory.md`](source_inventory.md) | Winter under-represented; stated as a limitation |
| French overseas regions inside "Europe" | STG-1 | Excluded from the Europe stratum (tropical rainfall would distort it) |
 
### 3.3 Water Quality Portal summary (`wqp_summary`)
 
| Finding | Evidence | Handling |
|---|---|---|
| A national summary request times out | ING-2 | One request per US state; files named `summary_US-XX` (`:` is invalid in Windows file names) |
| Eligible pools: **9,052** freshwater and **1,484** marine sites | ING-2 | Far above K=60, so sampling (seed 42) keeps the US from dominating |
| `Enterococci` spelling returned **0** summary rows (`Enterococcus` is the name used) | ING-2 | Both spellings mapped, the second kept defensively |
| Site types outside scope (BEACH Program, canals, Great Lakes, stream subtypes), **~10,455 sites** | ING-2 | Excluded; only rivers/streams, lakes/reservoirs, estuaries, ocean |
 
### 3.4 Water Quality Portal results (`wqp_results`)
 
| Finding | Evidence | Handling |
|---|---|---|
| 63 columns; no result ID column | ING-3 | Record ID = SHA-1 of activity, characteristic, fraction, value, unit |
| POST search ignores date filters in the query string | ING-3 | Dates sent in the JSON body |
| Units seen: `#/100ml` (867 results), `MPN/100ml`, `cfu/100ml`. **Unmapped:** `MPN` (968), `CFU` (76), `count` (13) with no volume; `None` (278), `hours` (135) | ING-3 full run, 2026-10-03 | Volume-less and non-concentration units dropped as `unit_unmapped`: **1,449 WQP rows (~11%)**. Open question for PM (STG-2 follow-up) |
| Censoring texts: `Below Detection Limit` (9), `Not Detected at Reporting Limit` (6), value `*Non-detect` (160); unusable: `Not Reported` (19), `Detected Not Quantified` (1) | ING-3 | Mapped to `<` with limit / 2; unusable rows dropped as `non_numeric_value`. Censored share in WQP: **5–13%** (README §15) |
| Quality-control activity types (blanks, field replicates) | ING-3 | Blanks dropped (`qc_blank`); field replicates kept and combined per day (geometric mean) |
| `Rejected` result status, non-water media | ING-3 | Dropped (`result_rejected`, `media_not_water`) |
| Thousands separators in values, e.g. `5,794.0` | STG-2 | Parsed as numbers (`harmonize.to_number`) |
| 400+ submitting organizations with inconsistent names and units | [`source_inventory.md`](source_inventory.md) | Full harmonization in staging; every dropped row counted in `_drop_log.parquet` |
 
### 3.5 Open-Meteo (`open_meteo`)
 
| Finding | Evidence | Handling |
|---|---|---|
| Long requests are weighted: locations × days/14 × variables/10. One cell for 2015–2025 with 2 variables ≈ **57 calls** | ING-4 | Hourly / daily budgets in `sources.yaml` (4,500 / 9,000); the run stops as `partial` and resumes next run (~156 cells/day) |
| One request per 0.25° cell, not per site | ING-4 | Sites in the same cell share weather (`site_cells.csv`) |
| Daily values are UTC days; sample dates are local | ING-4 / CUR-1 | The 48 h window absorbs most of the offset; stated as a limitation |
| Reanalysis, not station rainfall | [`source_inventory.md`](source_inventory.md) | Local convective rain may be missed; stated as a limitation |
| Missing days | profiling `precipitation_missing_share`: FILL | A window with any missing day gives null rain, never a partial sum; null weather counts as neither wet nor dry |
 
### 3.6 Natural Earth admin-1 (`natural_earth`)
 
| Finding | Evidence | Handling |
|---|---|---|
| **4,596** features, EPSG:4326 | ING-5 | Used as-is |
| `iso_a2 = "-99"` for some territories | STG-0 | Fall back to `adm0_a3` |
| Coastal and marine sites fall just outside land polygons | STG-1 | Nearest polygon within 25 km (`region_match = nearest`), else `none` |
| Some countries only at top-level regions (e.g. France); theme marked beta | [`source_inventory.md`](source_inventory.md) | Accepted; administrative, not watershed, boundaries |
 
## 4. Quality issues across sources
 
| # | Issue | Dimension | Sources | Impact if ignored | Pipeline handling | Check |
|---|---|---|---|---|---|---|
| Q1 | Different indicator names (`Escherichia coli`, `E. coli`, `Enterococcus`, `Enterococci`) | Consistency | all | Same indicator counted as different ones | `mappings.yaml › indicator_names` | VAL-2 `accepted_values` |
| Q2 | Different or missing units | Consistency, validity | WQP | Values on different scales compared | Unit factor table; unmapped rows dropped and counted | STG-2 drop log, VAL-3 `reconciliation` |
| Q3 | Censored values (`<10`, `Not Detected`) | Validity | WQP, OWQ | Non-numeric values lost or treated as exact | `<x` → x/2, `>x` → x (same as OWQ) | `is_censored`, `censor_direction` columns |
| Q4 | Lab upper-limit values | Accuracy | GEMStat | Severity understated | Documented limitation | — |
| Q5 | No record or site IDs | Uniqueness | OWQ | Duplicates, no lineage key | Deterministic SHA-1 keys, rounded-coordinate sites | VAL-2 `obs_key_unique` |
| Q6 | Same-day replicates | Uniqueness | WQP | One event counted several times | Collapsed to one sample-day (geometric mean) in MART-1 | — |
| Q7 | QC blanks, rejected results | Validity | WQP | Non-environmental values scored | Dropped with reasons | drop log |
| Q8 | Unclassifiable water body (`other`, 65% of Eionet) | Completeness | Eionet | Wrong threshold applied | Excluded | `water_body_excluded` in drop log |
| Q9 | Data gap from 2022 | Timeliness | Eionet | Recent years look clean | Most recent *classified* years used; flagged | VAL-2 `stratum_coverage` |
| Q10 | Empty strata (Africa, Asia) | Coverage | GEMStat | Rankings look global but aren't | Reported as a finding | VAL-2 `stratum_coverage` (warning) |
| Q11 | Missing or invalid coordinates | Validity | all | Site cannot be placed or matched to weather | Dropped as `bad_coordinates` | VAL-2 `coordinates_valid` |
| Q12 | Row cap / timeouts at source | Completeness | OWQ, WQP | Silent truncation | Chunking; manifest row counts | VAL-1 `row_count_matches` |
| Q13 | Rate limits | Completeness | Open-Meteo | Missing weather | Budgeted, resumable extract | VAL-3 `weather_coverage` (≥ 90%) |
 
## 5. Sampling outcome
 
| Stratum | Source | Freshwater sites | Marine sites | Note |
|---|---|---|---|---|
| `us` | WQP | FILL | FILL | 120 in total; eligible pools 9,052 / 1,484 |
| `europe` | Eionet | FILL | FILL | 120 in total; 224 eligible marine |
| `gemstat_latin_america` | GEMStat | FILL | FILL | 60 in total |
| `gemstat_africa` | GEMStat | 0 | 0 | No fecal indicator sites |
| `gemstat_asia` | GEMStat | 0 | 0 | Coliform-only |
| **Total** | | | | **300 sites** ([`business_rules.md`](business_rules.md) §6) |
 
The per-realm split is in `data/staging/sampled_sites/_manifest.json` (STG-1 sampling summary).
 
## 6. Limitations
 
1. **Geographic imbalance.** Openly shared fecal indicator data is concentrated in the US and Europe; Africa and Asia are absent from the scored sample. This reflects data sharing, not pollution levels.
2. **Different monitoring designs.** GEMStat river stations, Eionet bathing sites and WQP mixed networks sample differently, so cross-stratum comparisons are partly about design.
3. **Pre-harmonized OWQ data.** We cannot see OWQ's original censoring or units, so GEMStat/Eionet censored shares are unknown.
4. **Lab upper limits** understate severity in Latin America.
5. **Eionet gap after 2021** and bathing-season-only sampling.
6. **Dropped WQP units** (~11% of WQP rows) may hold usable data if their volume could be confirmed.
7. **Weather** is 0.25° reanalysis in UTC days, not station rainfall at the sampling time.
8. **Administrative regions**, not watersheds, so a polluted river can span several regions.
9. **MPN = CFU** is an approximation (the same one OWQ makes).
