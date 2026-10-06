# Data Dictionary
 
**Version:** 1.0· **Related:** [`data_contract.md`](data_contract.md), [`erd.md`](erd.md), `config/staging_schema.yaml`, `sql/init/01_schema.sql`
 
Every table and column the pipeline produces, from staging to the published outputs. Raw files keep their source columns unchanged (required columns per source are listed in [`data_contract.md`](data_contract.md) §3); section 5 shows how they map to staging.
 
**Authoritative specs:** if this page and a spec disagree, the spec wins and this page must be fixed. Staging = `config/staging_schema.yaml` (section 4 is generated from it). Warehouse = `sql/init/01_schema.sql` / [`erd.md`](erd.md).
 
---
 
## Contents
 
1. [Conventions](#1-conventions)
2. [Controlled vocabularies](#2-controlled-vocabularies)
3. [Warehouse tables (curated layer, PostgreSQL)](#3-warehouse-tables-curated-layer-postgresql)
4. [Staging tables (Parquet)](#4-staging-tables-parquet)
5. [Source-to-staging field mapping](#5-source-to-staging-field-mapping)
6. [Output files](#6-output-files)
## 1. Conventions
 
| Item | Convention |
|---|---|
| Names | `snake_case`; tables prefixed `dim_`, `fact_`, `mart_`; units in the name (`_mm`, `_c`, `_cfu_100ml`) |
| Keys | Natural strings: `site_key = {source_code}:{source_site_id}`, `obs_key = {source_code}:{source_record_id}`, `cell_id = {lat:.2f}_{lon:.2f}` |
| Null | Unknown or not applicable. Never `0`, `-99` or empty string |
| Concentrations | CFU/100 mL (MPN/100 mL treated as equal) |
| Rates | Fractions in [0, 1], not percentages |
| Dates | `date` = calendar day; `timestamptz` = UTC |
| Types | Warehouse: PostgreSQL types. Staging: Arrow/pandas types (`string`, `float64`, `int16`, `int32`, `bool`, `date32`) |
| **PK** / **FK** | Primary key / foreign key (target in brackets) |
 
## 2. Controlled vocabularies
 
| Field | Allowed values | Defined in |
|---|---|---|
| `source_code` (observations, sites) | `wqp`, `owq_gemstat`, `owq_eionet` | `staging_schema.yaml` |
| `source_code` (raw batches, `dq_results`) | `wqp_summary`, `wqp_results`, `owq_gemstat`, `owq_eionet`, `open_meteo`, `natural_earth` | AGENTS §2 |
| `indicator_code` | `e_coli`, `enterococci` (scored); `fecal_coliform`, `total_coliform` (supplementary) | `mappings.yaml › indicator_names` |
| `realm` | `freshwater` (primary indicator `e_coli`, threshold 410), `marine` (primary `enterococci`, threshold 130) | `sampling.yaml › realms` |
| `stratum` | `us`, `europe`, `gemstat_africa`, `gemstat_asia`, `gemstat_latin_america` | `sampling.yaml › strata` |
| `region_match` | `within` (inside an admin-1 polygon), `nearest` (closest polygon within 25 km), `none` | `sites.assign_regions` |
| `censor_direction` | `<` (left-censored), `>` (right-censored), null | `mappings.yaml › censoring` |
| `access_method` | `api`, `export`, `download` | `source_catalog.yaml`, DDL |
| `mart_region_priority.status` | `ranked`, `insufficient_sites` | `marts.py`, DDL |
| `etl_batch_log.stage` | `extract`, `staging`, `curated`, `marts`, `load` | DDL |
| `etl_batch_log.status` | `success`, `failed` | DDL |
| `dq_results.stage` / `severity` / `status` | `raw`, `staging`, `curated` / `critical`, `warning` / `pass`, `fail` | DDL |
| `drop_log.reason` | `site_not_sampled`, `date_out_of_period`, `indicator_unmapped`, `media_not_water`, `qc_blank`, `result_rejected`, `non_numeric_value`, `unit_unmapped`, `negative_value`, `duplicate_obs_key`, `water_body_excluded`, `bad_coordinates`, `no_stratum` | `mappings.yaml › drop_reasons` |
 
## 3. Warehouse tables (curated layer, PostgreSQL)
 
Also written as `data/curated/{table}.parquet` by CUR-1 / MART-1 (without `load_batch_id`, which LOAD-1 adds).
 
### `dim_source`
 
Data sources of observations. **Grain:** one row per source. **Built by:** CUR-1 from `config/source_catalog.yaml`.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `source_code` | varchar | no | **PK** | `wqp`, `owq_gemstat`, `owq_eionet` |
| `source_name` | varchar | yes | | Display name, e.g. "Water Quality Portal" |
| `provider` | varchar | yes | | Organization(s) behind the data |
| `access_method` | varchar | yes | | `api`, `export`, `download` |
| `url` | text | yes | | Source home page |
 
### `dim_indicator`
 
Fecal indicators. **Grain:** one row per indicator. **Built by:** CUR-1 from `sampling.yaml` + `source_catalog.yaml`.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `indicator_code` | varchar | no | **PK** | See vocabularies |
| `indicator_name` | varchar | yes | | e.g. "Escherichia coli", "Intestinal enterococci" |
| `realm` | varchar | yes | | Realm the indicator is scored for; null for supplementary indicators |
| `threshold_cfu_100ml` | numeric | yes | | EPA 2012 single-sample threshold: 410 (`e_coli`), 130 (`enterococci`); null for supplementary |
| `is_scored` | boolean | no | | True for `e_coli` and `enterococci` only |
 
### `dim_region`
 
Natural Earth admin-1 regions (states, provinces) that contain at least one sampled site. **Grain:** one row per region. **Built by:** CUR-1 from the Natural Earth attribute table.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `region_code` | varchar | no | **PK** | Natural Earth `adm1_code` |
| `admin1_name` | varchar | yes | | Region name (Natural Earth `name`) |
| `country_iso` | varchar | yes | | ISO 3166-1 alpha-2 (`iso_a2`; `adm0_a3` when `iso_a2` is `-99`) |
| `country_name` | varchar | yes | | Natural Earth `admin` |
| `continent` | varchar | yes | | From `country_iso` (pycountry-convert) |
 
### `dim_grid_cell`
 
0.25° weather grid cells; one Open-Meteo request per cell. **Grain:** one row per cell used by a sampled site. **Built by:** CUR-1 from ING-4 `grid_cells.csv`.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `cell_id` | varchar | no | **PK** | `{cell_lat:.2f}_{cell_lon:.2f}`, e.g. `14.50_121.00` |
| `cell_lat` | numeric | no | | Cell-centre latitude (multiple of 0.25) |
| `cell_lon` | numeric | no | | Cell-centre longitude (multiple of 0.25) |
 
### `dim_site`
 
Sampled monitoring sites. **Grain:** one row per sampled site. **Built by:** CUR-1 from staging `sites` (`is_sampled = true`) + ING-4 `site_cells.csv`.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `site_key` | varchar | no | **PK** | `{source_code}:{source_site_id}`, e.g. `wqp:USGS-01649190` |
| `source_code` | varchar | no | **FK** [dim_source] | Dataset the site comes from |
| `source_site_id` | varchar | no | UNIQUE with `source_code` | WQP `MonitoringLocationIdentifier`; OWQ: coordinates rounded to 5 decimals, `{lat}_{lon}` |
| `region_code` | varchar | yes | **FK** [dim_region] | Null when `region_match = none` |
| `cell_id` | varchar | yes | **FK** [dim_grid_cell] | Weather cell of the site |
| `site_name` | varchar | yes | | Name if the source gives one; currently null for all sources (OWQ has none, WQP names are not pulled by ING-2) |
| `water_body_type` | varchar | yes | | As reported (WQP `MonitoringLocationTypeName`, OWQ `water_body`) |
| `realm` | varchar | yes | | `freshwater` or `marine`; decides the scored indicator and threshold |
| `stratum` | varchar | yes | | Sampling stratum |
| `latitude` | numeric | no | | WGS84 |
| `longitude` | numeric | no | | WGS84 |
| `region_match` | varchar | no | | How `region_code` was assigned: `within`, `nearest`, `none` |
 
### `fact_observation`
 
**Grain:** one harmonized fecal-indicator result. **Built by:** CUR-1 from staging `observations` (sampled sites only) + weather. **Loaded by:** LOAD-1, UPSERT on `obs_key`.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `obs_key` | varchar | no | **PK** | `{source_code}:{source_record_id}` |
| `site_key` | varchar | no | **FK** [dim_site] | |
| `indicator_code` | varchar | no | **FK** [dim_indicator] | |
| `source_record_id` | varchar | no | | Source result ID; SHA-1 of identifying fields when the source has none (`mappings.yaml › record_id`) |
| `activity_id` | varchar | yes | | Sampling event (WQP `ActivityIdentifier`); null for OWQ |
| `activity_type` | varchar | yes | | e.g. `Sample-Routine`, `Quality Control Sample-Field Replicate`; null for OWQ |
| `sample_date` | date | no | | Local collection date as reported |
| `value_cfu_100ml` | numeric | yes | | Harmonized value. Censored: `<x` → x/2, `>x` → x. CHECK ≥ 0 |
| `original_value` | varchar | yes | | Value text exactly as reported |
| `original_unit` | varchar | yes | | Unit exactly as reported |
| `is_censored` | boolean | no | | Value was below/above a detection or quantification limit |
| `censor_direction` | varchar | yes | | `<`, `>` or null |
| `detection_limit` | numeric | yes | | Reported limit in CFU/100 mL |
| `exceeds_threshold` | boolean | yes | | `value_cfu_100ml > dim_indicator.threshold_cfu_100ml`; null for supplementary indicators or null values |
| `rain_48h_mm` | numeric | yes | | Precipitation on the sample day + previous day (site's grid cell); null if any day is missing |
| `rain_72h_mm` | numeric | yes | | `rain_48h_mm` + the day before; null if any day is missing |
| `temp_mean_c` | numeric | yes | | Mean 2 m air temperature on the sample day, °C |
| `is_wet` | boolean | yes | | `rain_48h_mm >= 10` (`business_rules.yaml › rainfall.wet_threshold_mm_48h`); null if no weather |
| `raw_batch_id` | varchar | no | | Raw batch the row came from (lineage) |
| `raw_file` | varchar | no | | File inside that batch, e.g. `chunk_0002.csv` (lineage) |
| `load_batch_id` | varchar | yes | **FK** [etl_batch_log] | Run that loaded the row |
 
### `fact_weather_daily`
 
**Grain:** one row per grid cell per day. **Built by:** CUR-1 from ING-4 `cell_*.json`. Source of truth for weather; `fact_observation` holds copies for speed.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `cell_id` | varchar | no | **PK**, **FK** [dim_grid_cell] | |
| `weather_date` | date | no | **PK** | UTC day |
| `precipitation_mm` | numeric | yes | | Open-Meteo `precipitation_sum` (ERA5). CHECK ≥ 0 |
| `temp_mean_c` | numeric | yes | | Open-Meteo `temperature_2m_mean`, °C |
| `raw_batch_id` | varchar | yes | | ING-4 batch (lineage) |
 
### `mart_site_hotspot`
 
**Grain:** one row per (site, scored indicator); each site is scored only on its realm's primary indicator. **Built by:** MART-1. Rule values: [`business_rules.md`](business_rules.md).
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `site_key` | varchar | no | **PK**, **FK** [dim_site] | |
| `indicator_code` | varchar | no | **PK**, **FK** [dim_indicator] | `e_coli` (freshwater) or `enterococci` (marine) |
| `region_code` | varchar | yes | | Copied from `dim_site` |
| `realm` | varchar | yes | | Copied from `dim_site` |
| `n_observations` | integer | yes | | Rows in `fact_observation` for this site and indicator |
| `n_samples` | integer | yes | | Sample-days (same-day replicates collapsed by shifted geometric mean) with a value |
| `years_monitored` | smallint | yes | | Classified years (≥ 5 sample-days) |
| `insufficient_years` | smallint | yes | | Years with < 5 sample-days (ignored) |
| `poor_years` | smallint | yes | | Poor years (> 10% of sample-days exceed) among the most recent 5 classified years |
| `is_persistent_hotspot` | boolean | yes | | `poor_years >= 3` |
| `exceedance_rate` | numeric | yes | | Share of all sample-days above the threshold, whole period. [0, 1] |
| `median_ratio_to_threshold` | numeric | yes | | Median of sample-day value ÷ threshold (1.0 = at threshold) |
| `trend_slope` | numeric | yes | | Theil-Sen slope of annual exceedance rate per year (positive = worsening); null if < 3 classified years |
| `wet_exceedance_rate` | numeric | yes | | Exceedance rate on wet sample-days; null if none |
| `dry_exceedance_rate` | numeric | yes | | Exceedance rate on dry sample-days (unknown weather excluded from both) |
| `batch_id` | varchar | yes | | MART-1 batch id |
| `load_batch_id` | varchar | yes | **FK** [etl_batch_log] | |
 
### `mart_region_priority`
 
**Grain:** one row per admin-1 region with at least one judged site. **Built by:** MART-1.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `region_code` | varchar | no | **PK**, **FK** [dim_region] | |
| `admin1_name`, `country_iso`, `country_name` | varchar | yes | | Copied from `dim_region` |
| `status` | varchar | no | | `ranked` if `n_sites >= 3`, else `insufficient_sites` |
| `n_sites` | integer | yes | | Judged sites in the region (`years_monitored > 0`) |
| `n_hotspots` | integer | yes | | Of those, persistent hotspots |
| `persistence_raw` | numeric | yes | | `n_hotspots / n_sites` |
| `severity_raw` | numeric | yes | | Median of sites' `median_ratio_to_threshold` |
| `trend_raw` | numeric | yes | | Median of sites' `trend_slope` |
| `rain_sensitivity_raw` | numeric | yes | | Median of sites' `wet_exceedance_rate / dry_exceedance_rate` (sites with dry rate > 0) |
| `persistence_score`, `severity_score`, `trend_score`, `rain_sensitivity_score` | numeric | yes | | Raw component min-max scaled to [0, 1] across ranked regions; 0 if missing (see `notes`); null if unranked |
| `priority_score` | numeric | yes | | 0.40 × persistence + 0.30 × severity + 0.15 × trend + 0.15 × rain sensitivity; null if unranked |
| `priority_rank` | integer | yes | | 1 = highest priority; ties → more hotspots, then `region_code`; null if unranked |
| `rank_in_country` | integer | yes | | Rank among ranked regions of the same country |
| `notes` | text | yes | | e.g. `trend missing (scored 0);` |
| `batch_id` | varchar | yes | | MART-1 batch id |
| `load_batch_id` | varchar | yes | **FK** [etl_batch_log] | |
 
### `etl_batch_log`
 
**Grain:** one row per pipeline batch (per stage). **Written by:** LOAD-1 / ING-6.
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `batch_id` | varchar | no | **PK** | Deterministic batch id |
| `dag_run_id` | varchar | yes | | Airflow run id |
| `stage` | varchar | no | | `extract`, `staging`, `curated`, `marts`, `load` |
| `source_code` | varchar | yes | | Source, if the stage is per source |
| `started_at`, `finished_at` | timestamptz | yes | | UTC |
| `status` | varchar | no | | `success`, `failed` |
| `rows_in`, `rows_out` | integer | yes | | Row counts (≥ 0) |
| `params` | jsonb | yes | | Parameters that determined the batch id |
 
### `dq_results`
 
**Grain:** one row per check per run. **Written by:** VAL-1/2/3 (`src/validation/core.py`).
 
| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `dq_result_id` | bigint identity | no | **PK** | |
| `run_id` | varchar | yes | | Airflow run id or `manual` |
| `batch_id` | varchar | yes | | Batch checked (no FK: raw batch ids are not in `etl_batch_log`) |
| `check_name` | varchar | no | | e.g. `checksum_matches`, `obs_key_unique`, `reconciliation` |
| `stage` | varchar | yes | | `raw`, `staging`, `curated` |
| `source_code` | varchar | yes | | |
| `severity` | varchar | no | | `critical` (stops the run) or `warning` |
| `status` | varchar | no | | `pass`, `fail` |
| `failed_count` | integer | yes | | Number of problems found |
| `details` | text | yes | | Human-readable explanation |
| `checked_at` | timestamptz | no | | Default `now()` |
 
## 4. Staging tables (Parquet)
 
Generated from `config/staging_schema.yaml` (version 1). Partition columns live in folder names, not inside the files.
 
### `observations`
 
**Path:** `data/staging/observations/source_code={source_code}/year={year}/part-0000.parquet` · **Primary key:** `obs_key` · **Partitioned by:** `source_code, year`
 
| Column | Type | Null | Description |
|---|---|---|---|
| `obs_key` | string | no | Unique observation key: source_code + ':' + source_record_id |
| `source_code` | string | no | Dataset: wqp \| owq_gemstat \| owq_eionet |
| `source_record_id` | string | no | Stable ID within the source. WQP has no result ID column, so it is a SHA-1 hash (see mappings.yaml › record_id) |
| `site_key` | string | no | Site key: source_code + ':' + source_site_id; must exist in sites |
| `source_site_id` | string | no | Site ID in the source. WQP: MonitoringLocationIdentifier. OWQ has no site ID: rounded coordinates 'lat_lon' (mappings.yaml › record_id) |
| `activity_id` | string | yes | Sampling event ID in the source (WQP: ActivityIdentifier); one activity has several results |
| `activity_type` | string | yes | Sample type as reported (WQP: ActivityTypeCode), e.g. 'Sample-Routine', 'Quality Control Sample-Field Replicate' |
| `indicator_code` | string | no | e_coli \| enterococci \| fecal_coliform \| total_coliform (mappings.yaml › indicator_names) |
| `sample_date` | date32 | no | Date the sample was collected (local date as reported) |
| `year` | int16 | no | Year of sample_date (partition column) |
| `value_cfu_100ml` | float64 | yes | Harmonized value in CFU/100 mL (MPN treated as CFU). Censored values: '<x' -> x/2, '>x' -> x |
| `original_value` | string | yes | Value exactly as reported (text), kept for lineage |
| `original_unit` | string | yes | Unit exactly as reported |
| `is_censored` | bool | no | True if the value was below or above a detection/quantification limit |
| `censor_direction` | string | yes | '<' (left-censored), '>' (right-censored) or null |
| `detection_limit` | float64 | yes | Reported detection/quantification limit in CFU/100 mL, if any |
| `result_status` | string | yes | Status as reported (WQP: ResultStatusIdentifier); 'Rejected' rows are dropped |
| `raw_batch_id` | string | no | Raw batch the row came from (lineage) |
| `raw_file` | string | no | Raw file name within the batch, e.g. chunk_0002.csv (lineage) |
 
### `sites`
 
**Path:** `data/staging/sites/sites.parquet` · **Primary key:** `site_key`
 
| Column | Type | Null | Description |
|---|---|---|---|
| `site_key` | string | no | source_code + ':' + source_site_id |
| `source_code` | string | no | wqp \| owq_gemstat \| owq_eionet |
| `source_site_id` | string | no | Site ID in the source (OWQ: rounded coordinates, see mappings.yaml) |
| `site_name` | string | yes | Site name if the source provides one (OWQ: none) |
| `source_region` | string | yes | Country as labelled by the source (OWQ 'region', e.g. 'Hungary'); country_iso comes from the spatial join |
| `water_body_type` | string | yes | Type as reported (WQP: MonitoringLocationTypeName) |
| `realm` | string | yes | freshwater \| marine (sampling.yaml › water_body_realm); null if excluded/unmapped |
| `latitude` | float64 | no | WGS84 decimal degrees |
| `longitude` | float64 | no | WGS84 decimal degrees |
| `country_iso` | string | yes | ISO 3166-1 alpha-2 from Natural Earth iso_a2 (fallback adm0_a3 when iso_a2 is '-99') |
| `continent` | string | yes | From country_iso (mappings.yaml › continents) |
| `region_code` | string | yes | Natural Earth adm1_code; null if no region within 25 km |
| `region_match` | string | no | within \| nearest \| none |
| `stratum` | string | yes | us \| europe \| gemstat_africa \| gemstat_asia \| gemstat_latin_america; null if none applies |
| `n_samples` | int32 | no | Samples of the realm's primary indicator in the period (eligibility) |
| `n_years` | int16 | no | Distinct years with at least one such sample |
| `is_eligible` | bool | no | Meets sampling.yaml › eligibility |
| `is_sampled` | bool | no | Selected by stratified_sample (fixed seed) |
 
### `sampled_sites`
 
**Path:** `data/staging/sampled_sites/sampled_sites.parquet` · **Primary key:** `site_key`
 
| Column | Type | Null | Description |
|---|---|---|---|
| `site_key` | string | no | As in sites |
| `source_code` | string | no | As in sites |
| `latitude` | float64 | no | As in sites |
| `longitude` | float64 | no | As in sites |
| `stratum` | string | no | As in sites |
| `realm` | string | no | As in sites |
 
### `drop_log`
 
**Path:** `data/staging/_drop_log.parquet` · **Primary key:** `stage, source_code, reason, batch_id`
 
| Column | Type | Null | Description |
|---|---|---|---|
| `stage` | string | no | sites \| observations |
| `source_code` | string | no | wqp \| owq_gemstat \| owq_eionet |
| `reason` | string | no | One of mappings.yaml › drop_reasons |
| `row_count` | int32 | no | Rows dropped for this reason |
| `batch_id` | string | no | Staging batch id |
 
## 5. Source-to-staging field mapping
 
| Staging column | WQP results (`wqp_results`) | OWQ (`owq_gemstat`, `owq_eionet`) |
|---|---|---|
| `source_site_id` | `MonitoringLocationIdentifier` | `latitude`, `longitude` rounded to 5 decimals → `{lat}_{lon}` |
| `source_record_id` | SHA-1 of `ActivityIdentifier`, `CharacteristicName`, `ResultSampleFractionText`, `ResultMeasureValue`, `ResultMeasure/MeasureUnitCode` | SHA-1 of `source_site_id`, `date`, `indicator` |
| `activity_id` | `ActivityIdentifier` | null |
| `activity_type` | `ActivityTypeCode` (blanks dropped) | null |
| `indicator_code` | `CharacteristicName` via `mappings.yaml` | `indicator` via `mappings.yaml` |
| `sample_date` | `ActivityStartDate` | `date` |
| `value_cfu_100ml` | `ResultMeasureValue` × unit factor, censoring applied | `value` (already harmonized by OWQ) |
| `original_value` / `original_unit` | `ResultMeasureValue` / `ResultMeasure/MeasureUnitCode` | `value` / `unit` |
| `is_censored`, `censor_direction` | value prefix, `ResultDetectionConditionText`, `*Non-detect` | value prefix only (OWQ pre-fills censored values) |
| `detection_limit` | `DetectionQuantitationLimitMeasure/MeasureValue`, else the limit in a `<x` / `>x` value | the limit in a `<x` / `>x` value, else null |
| `result_status` | `ResultStatusIdentifier` (`Rejected` dropped) | null |
| `water_body_type` (sites) | `MonitoringLocationTypeName` | most frequent `water_body` at the site (ties → alphabetical) |
| `site_name`, `source_region` (sites) | null, null | null, `region` |
| `latitude`, `longitude` (sites) | `MonitoringLocationLatitude`, `MonitoringLocationLongitude` (summary) | `latitude`, `longitude` |
| Filter only | `ActivityMediaName` = `Water` | |
 
Weather (`open_meteo`) maps `daily.time` → `weather_date`, `daily.precipitation_sum` → `precipitation_mm`, `daily.temperature_2m_mean` → `temp_mean_c`. Boundaries (`natural_earth`) map `adm1_code` → `region_code`, `name` → `admin1_name`, `iso_a2` / `adm0_a3` → `country_iso`, `admin` → `country_name`.
 
## 6. Output files
 
All under `outputs/` (git-ignored; copy evidence to `docs/evidence/` to commit it).
 
| File | Written by | Grain | Columns / content |
|---|---|---|---|
| `priority_ranking.csv` | MART-1 | one row per ranked region | `priority_rank`, `region_code`, `admin1_name`, `country_name`, `country_iso`, `n_sites`, `n_hotspots`, `priority_score`, `rank_in_country` |
| `reconciliation.csv` | VAL-3 | one row per source | `source_code`, `raw_rows`, `dropped_{reason}` (one column per reason seen), `dropped_total`, `staging_rows`, `curated_rows`, `raw_to_staging_ok`, `staging_to_curated_ok` |
| `dq/dq_report_{stage}_{run_id}.json` | VAL-1/2/3 | one file per stage and run | `stage`, `run_id`, `checks`, `passed`, `failed_critical`, `failed_warning`, `results[]` (same fields as `dq_results`) |
| `benchmark/format_benchmark.csv` | BENCH-1 | one row per format | `format`, `rows`, `size_bytes`, `size_mb`, `size_vs_csv`, `write_s`, `read_all_s`, `read_cols_s`, `query_s`, `query_result`, `types_preserved`, `roundtrip_equal` (see [`format_benchmark.md`](format_benchmark.md)) |
| `benchmark/format_benchmark.md`, `benchmark/_manifest.json` | BENCH-1 | one per run | Same table for the report + environment and batch id |
 
