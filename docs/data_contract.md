# Data Cntract

**Version:** 1.0 · **Related:** [`data_dictionary.md`](data_dictionary.md), [`erd.md`](erd.md), [`business_rules.md`](business_rules.md), `config/staging_schema.yaml`
 
A data contract is the agreement between whoever **produces** a dataset and whoever **consumes** it: what the dataset contains, what is guaranteed about it, how that is checked, and how it may change. This pipeline has one contract per layer hand-off, plus one for the published data product.
 
Column-level definitions are in [`data_dictionary.md`](data_dictionary.md); this page states the guarantees.
 
---
 
## 1. Contracts at a glance
 
| # | Contract | Producer | Consumer | Machine-readable spec | Enforced by |
|---|---|---|---|---|---|
| C1 | Raw batch | Extractors (ING-1…5) | STG-1, STG-2, CUR-1 | `config/schemas/{source_code}.yaml` + manifest format (AGENTS §5.3) | VAL-1 `raw_checks.py` |
| C2 | Sampled sites | STG-1 | ING-4 (weather) | `staging_schema.yaml › sampled_sites` | VAL-2, ING-4 input check |
| C3 | Staging | STG-1, STG-2 | CUR-1 | `staging_schema.yaml › observations, sites, drop_log` | VAL-2 `staging_checks.py` |
| C4 | Curated / warehouse | CUR-1, MART-1, LOAD-1 | Analysts, SQL, report, map | [`erd.md`](erd.md), `sql/init/01_schema.sql` | DDL constraints + VAL-3 `curated_checks.py` |
| C5 | Published data product | MART-1 | Rehabilitation planners, regulators | this page §6 | VAL-3 `mart_rank_consistency` |
 
## 2. General terms (apply to every contract)
 
| Term | Guarantee |
|---|---|
| **Units** | Every concentration is CFU/100 mL. MPN/100 mL is treated as CFU/100 mL (factor 1.0, documented assumption) |
| **Indicator vocabulary** | `e_coli`, `enterococci` (scored); `fecal_coliform`, `total_coliform` (supplementary, never scored) |
| **Dates** | `sample_date` is the local date as reported by the source; weather dates are UTC days |
| **Coordinates** | WGS84 (EPSG:4326) decimal degrees |
| **Keys** | Natural, deterministic strings (`source_code:id`). The same input always produces the same keys |
| **Period** | 2015-01-01 to 2025-12-31 (`config/sampling.yaml › period`) |
| **Determinism** | Same configuration + same raw batches → identical rows, keys and ranks (AGENTS §7) |
| **Freshness** | Batch, on demand (one Airflow run). No real-time guarantee. The data reflects each source as of its `retrieved_at_utc` in the raw manifest |
| **Lineage** | Every observation carries `raw_batch_id` and `raw_file`; every warehouse fact/mart row carries `load_batch_id` |
| **Failure policy** | A **critical** check failure stops the run before the next layer is written. A **warning** is recorded in `dq_results` and the DQ report, and the run continues |
 
## 3. C1: Raw batch contract
 
**Location:** `data/raw/{source_code}/batch_id={batch_id}/`
 
| Guarantee | Detail | Check (severity) |
|---|---|---|
| Files are exactly as received | No renaming of columns, filtering or value fixes | by design (review) |
| Manifest present and complete | `manifest.json` with `source_code`, `batch_id`, `status`, `retrieved_at_utc`, `params`, `requests`, `files` (name, size, sha256, row_count) | `manifest_status_success` (critical) |
| Status | Consumers read only batches with `status: success`; `partial` means resumable, never read | `manifest_status_success` (critical) |
| Integrity | SHA-256 of every file matches the manifest | `checksum_matches` (critical) |
| Completeness | Data rows on disk = manifest `row_count` | `row_count_matches` (critical) |
| Schema | Data files contain every column/key in `config/schemas/{source_code}.yaml` (`data_files` glob decides which files) | `required_columns_present`, `json_shape` (critical) |
| Identity | `batch_id` is derived from request parameters only; same params → same folder | by design |
 
Per-source required columns:
 
| source_code | Format | Required columns / keys |
|---|---|---|
| `wqp_summary` | CSV | `MonitoringLocationIdentifier`, `YearSummarized`, `CharacteristicName`, `ActivityCount`, `ResultCount`, `MonitoringLocationTypeName`, `MonitoringLocationLatitude`, `MonitoringLocationLongitude` |
| `wqp_results` | CSV | `MonitoringLocationIdentifier`, `ActivityIdentifier`, `ActivityTypeCode`, `ActivityMediaName`, `ActivityStartDate`, `CharacteristicName`, `ResultSampleFractionText`, `ResultMeasureValue`, `ResultMeasure/MeasureUnitCode`, `ResultDetectionConditionText`, `DetectionQuantitationLimitMeasure/MeasureValue`, `DetectionQuantitationLimitMeasure/MeasureUnitCode`, `ResultStatusIdentifier`, `ProviderName` |
| `owq_gemstat`, `owq_eionet` | CSV (7 leading `#` comment lines) | `date`, `indicator`, `value`, `unit`, `latitude`, `longitude`, `source`, `region`, `water_body` |
| `open_meteo` | JSON | `daily.time`, `daily.precipitation_sum`, `daily.temperature_2m_mean`, `daily_units`; every daily series as long as `daily.time` |
| `natural_earth` | Shapefile, EPSG:4326 | `adm1_code`, `name`, `iso_a2`, `adm0_a3`, `admin` |
 
## 4. C2: Sampled sites contract (STG-1 → ING-4)
 
**Location:** `data/staging/sampled_sites/sampled_sites.parquet`
 
| Guarantee | Detail |
|---|---|
| Columns | Exactly `site_key`, `source_code`, `latitude`, `longitude`, `stratum`, `realm`, in that order, none nullable |
| Grain | One row per sampled site; `site_key` unique |
| Selection | Only eligible sites (≥ 24 primary-indicator samples across ≥ 3 years), up to K=60 per stratum × realm, seed 42 |
| Stability | Same config → same sites. Changing K, seed, period or eligibility changes the site list **and** the Open-Meteo batch, so it requires PM approval (weather pull is rate-limited) |
 
## 5. C3: Staging contract (STG-1/STG-2 → CUR-1)
 
**Location:** `data/staging/` · **Spec:** `config/staging_schema.yaml` (version 1)
 
| Guarantee | Detail | Check (severity) |
|---|---|---|
| Schema | Columns, order and dtypes exactly as in `staging_schema.yaml`. Partition columns may read back as int32 | `schema_matches` (critical) |
| Required fields | Columns declared `nullable: false` have no nulls | `keys_not_null` (critical) |
| Grain and keys | `observations`: one row per `obs_key`, unique across partitions. `sites`: one row per `site_key` | `obs_key_unique` (critical) |
| Vocabularies | `indicator_code`, `source_code`, `censor_direction`, sampled `realm` only use allowed values | `accepted_values` (critical) |
| Value range | `value_cfu_100ml >= 0`; values > 1,000,000 flagged for review | `value_range` (warning) |
| Period | Every `sample_date` within the period and equal to its `year` partition | `date_in_period` (critical) |
| Coordinates | Latitude in [-90, 90], longitude in [-180, 180] | `coordinates_valid` (critical) |
| Referential | Every observation's `site_key` exists in `sites.parquet` | `observation_site_exists` (critical) |
| Coverage | Each stratum × realm has ≥ `min_sites_to_keep` (10) sampled sites | `stratum_coverage` (warning) |
| Accountability | Every row removed after raw is counted in `_drop_log.parquet` with a reason from `mappings.yaml › drop_reasons`; STG-2 fails if rows in ≠ rows out + drops | STG-2 internal assertion |
| Layout | Partitioned `source_code=` / `year=`; the whole dataset is replaced atomically on every run | by design |
 
## 6. C4: Curated / warehouse contract
 
**Location:** `data/curated/*.parquet` and PostgreSQL `water_quality` · **Spec:** [`erd.md`](erd.md), `sql/init/01_schema.sql`
 
| Guarantee | Detail | Check (severity) |
|---|---|---|
| Tables | All 11 tables exist | `tables_present` (critical) |
| Keys | Primary keys as in the ERD; no duplicates | DDL + `pk_unique_in_db` (critical) |
| Relationships | Every FK resolves (0 orphans) | DDL + `referential_integrity` (critical) |
| Domain rules | `value_cfu_100ml >= 0`, `realm IN (freshwater, marine)`, rates in [0, 1], `region_match IN (within, nearest, none)`, etc. | DDL CHECK constraints |
| Reconciliation | Per source: raw rows − staging drops = staging rows = `fact_observation` rows | `reconciliation` (critical), `outputs/reconciliation.csv` |
| Weather coverage | ≥ 90% of observations have `rain_48h_mm` | `weather_coverage` (warning) |
| Rerun safety | Loads use `INSERT … ON CONFLICT (pk) DO UPDATE`; reruns never duplicate rows | LOAD-1, rerun evidence in `docs/evidence/` |
| Scope | Supplementary indicators are in `fact_observation` but never in marts | MART-1 design |
 
## 7. C5: Published data product
 
The consumer-facing outputs. These are what stakeholders use, so their meaning must not change silently.
 
| Dataset | Grain | Location | Guarantees |
|---|---|---|---|
| `mart_site_hotspot` | one row per (site, scored indicator); each site scored on its realm's primary indicator only | PostgreSQL, `data/curated/mart_site_hotspot.parquet` | `is_persistent_hotspot` follows `business_rules.yaml` exactly; rates in [0, 1] |
| `mart_region_priority` | one row per admin-1 region with ≥ 1 judged site | PostgreSQL, `data/curated/mart_region_priority.parquet` | `status = ranked` iff ≥ 3 sites; ranked regions have `priority_rank` 1…n with no gaps and `rank_in_country` 1…n per country; unranked regions have null ranks (`mart_rank_consistency`, critical). Ties broken by more hotspots, then `region_code` |
| `priority_ranking.csv` | ranked regions only | `outputs/priority_ranking.csv` | Columns: `priority_rank`, `region_code`, `admin1_name`, `country_name`, `country_iso`, `n_sites`, `n_hotspots`, `priority_score`, `rank_in_country` |
 
**Interpretation limits that consumers must be told** (also README §15):
 
- Rankings compare **sampled** sites, not every monitored water body.
- Cross-stratum differences partly reflect monitoring design (GEMStat rivers vs Eionet bathing sites vs WQP mixed networks).
- Asia and Africa strata are empty (data availability, not absence of pollution).
- GEMStat values cluster at lab upper limits ("at least this much"), so severity is understated for Latin America; exceedance and hotspot flags are unaffected.
## 8. Change management
 
| Change type | Examples | Allowed how |
|---|---|---|
| Non-breaking | Add a nullable column at the end; add a mapping value seen in real data; add a warning check | PR with ticket ID; update `data_dictionary.md` in the same PR |
| Breaking | Rename/remove/retype a column; change a key format; change a threshold, weight, K, seed or period | PM approval first; bump `version` in `staging_schema.yaml` (staging) or update `erd.md` + `01_schema.sql` together (warehouse); reset the DB (`docker compose down -v`); record in `business_rules.md` change log if a rule changed |
| Rule values | Anything in `business_rules.yaml` / `sampling.yaml` | Config change only (no code), justified in `business_rules.md` |
 
Rules for both: config and docs change in the **same PR** as the code; the PR states which contract (C1–C5) it touches.
 
## 9. Terms of use and attribution
 
| Source | Terms |
|---|---|
| Open Water Quality (GEMStat, Eionet) | Observations remain the property of the contributing programmes; cite on reuse; attribution file bundled with each export |
| Water Quality Portal | US federal public data (citation terms: VERIFY, see [`source_inventory.md`](source_inventory.md)) |
| Open-Meteo | Free for non-commercial use; attribution required (VERIFY) |
| Natural Earth | Public domain |
 
No personal data is collected. No credentials are stored in the repository.
 
## 10. Contacts
 
| Area | Owner |
|---|---|
| Contract and rules, validation, curated, documentation | Labilles|
| Raw layer, extractors, Airflow | Cuyo |
| Staging, platform, load | Vicente |
