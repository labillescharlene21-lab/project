# Database Design (ERD)

**Owner:** DE2 · **Status:** Updated for STG-0 (natural keys) and MART-1 (mart columns). `sql/init/01_schema.sql` (DB-1) must match this exactly.

```mermaid
erDiagram
    dim_source ||--o{ dim_site : "provides"
    dim_region ||--o{ dim_site : "contains"
    dim_grid_cell ||--o{ dim_site : "covers"
    dim_grid_cell ||--o{ fact_weather_daily : "has"
    dim_site ||--o{ fact_observation : "sampled at"
    dim_indicator ||--o{ fact_observation : "measures"
    dim_site ||--o{ mart_site_hotspot : "scored in"
    dim_indicator ||--o{ mart_site_hotspot : "scored on"
    dim_region ||--o| mart_region_priority : "ranked in"
    etl_batch_log ||--o{ fact_observation : "loaded by"
    etl_batch_log ||--o{ mart_site_hotspot : "loaded by"
    etl_batch_log ||--o{ mart_region_priority : "loaded by"

    dim_source {
        varchar source_code PK "wqp, owq_gemstat, owq_eionet"
        varchar source_name
        varchar provider
        varchar access_method "api, export, download"
        text url
    }

    dim_indicator {
        varchar indicator_code PK "e_coli, enterococci, fecal_coliform, total_coliform"
        varchar indicator_name
        varchar realm "freshwater, marine; null if supplementary"
        numeric threshold_cfu_100ml "410, 130; null if supplementary"
        boolean is_scored
    }

    dim_region {
        varchar region_code PK "Natural Earth adm1_code"
        varchar admin1_name
        varchar country_iso "ISO alpha-2"
        varchar country_name
        varchar continent
    }

    dim_grid_cell {
        varchar cell_id PK "e.g. 14.50_121.00"
        numeric cell_lat "0.25 degree centre"
        numeric cell_lon
    }

    dim_site {
        varchar site_key PK "source_code:source_site_id"
        varchar source_code FK
        varchar source_site_id "UNIQUE with source_code"
        varchar region_code FK "null if no region within 25 km"
        varchar cell_id FK
        varchar site_name
        varchar water_body_type
        varchar realm "freshwater, marine"
        varchar stratum "us, europe, gemstat_africa, ..."
        numeric latitude
        numeric longitude
        varchar region_match "within, nearest, none"
    }

    fact_observation {
        varchar obs_key PK "source_code:source_record_id"
        varchar site_key FK
        varchar indicator_code FK
        varchar source_record_id
        varchar activity_id
        varchar activity_type
        date sample_date
        numeric value_cfu_100ml "CHECK >= 0"
        varchar original_value
        varchar original_unit
        boolean is_censored
        varchar censor_direction "<, > or null"
        numeric detection_limit
        boolean exceeds_threshold "null if supplementary"
        numeric rain_48h_mm
        numeric rain_72h_mm
        numeric temp_mean_c
        boolean is_wet "null if no weather"
        varchar raw_batch_id "lineage"
        varchar raw_file "lineage"
        varchar load_batch_id FK
    }

    fact_weather_daily {
        varchar cell_id PK, FK
        date weather_date PK
        numeric precipitation_mm
        numeric temp_mean_c
        varchar raw_batch_id "lineage"
    }

    mart_site_hotspot {
        varchar site_key PK, FK
        varchar indicator_code PK, FK
        varchar region_code
        varchar realm
        int n_observations
        int n_samples "sample-days"
        smallint years_monitored
        smallint insufficient_years
        smallint poor_years
        boolean is_persistent_hotspot
        numeric exceedance_rate
        numeric median_ratio_to_threshold
        numeric trend_slope
        numeric wet_exceedance_rate
        numeric dry_exceedance_rate
        varchar batch_id "mart batch"
        varchar load_batch_id FK
    }

    mart_region_priority {
        varchar region_code PK, FK
        varchar admin1_name
        varchar country_iso
        varchar country_name
        varchar status "ranked, insufficient_sites"
        int n_sites
        int n_hotspots
        numeric persistence_raw
        numeric severity_raw
        numeric trend_raw
        numeric rain_sensitivity_raw
        numeric persistence_score
        numeric severity_score
        numeric trend_score
        numeric rain_sensitivity_score
        numeric priority_score
        int priority_rank "null if unranked"
        int rank_in_country "null if unranked"
        text notes
        varchar batch_id "mart batch"
        varchar load_batch_id FK
    }

    etl_batch_log {
        varchar batch_id PK
        varchar dag_run_id
        varchar stage "extract, staging, curated, marts, load"
        varchar source_code
        timestamptz started_at
        timestamptz finished_at
        varchar status "success, failed"
        int rows_in
        int rows_out
        jsonb params
    }

    dq_results {
        bigint dq_result_id PK
        varchar run_id "Airflow run id"
        varchar batch_id "no FK: raw batch ids are not in etl_batch_log"
        varchar check_name
        varchar stage "raw, staging, curated"
        varchar source_code
        varchar severity "critical, warning"
        varchar status "pass, fail"
        int failed_count
        text details
        timestamptz checked_at
    }
```

## Design notes

- **Natural keys everywhere.** `site_key`, `obs_key`, `region_code`, `cell_id`, `source_code`, `indicator_code` are built from the data itself, so every rerun produces the same keys. Loads use `INSERT ... ON CONFLICT (pk) DO UPDATE`, so reruns never duplicate rows (rerun safety).
- **Staging → warehouse.** `fact_observation` and `dim_site` carry the columns of `config/staging_schema.yaml` (observations, sites) plus curated additions: weather (`rain_48h_mm`, `rain_72h_mm`, `temp_mean_c`, `is_wet`) and `exceeds_threshold`.
- **Weather is copied onto observations** in the curated layer for analysis speed. `fact_weather_daily` stays the source of truth, reachable via `dim_site.cell_id` + date.
- **Marts** match `SITE_COLUMNS` / `REGION_COLUMNS` in `src/transform/marts.py` (MART-1), plus `load_batch_id` set by LOAD-1.
- **Supplementary indicators** (fecal, total coliform) are stored in `fact_observation` with `dim_indicator.is_scored = false` and never appear in the marts.
- **Lineage:** any curated row traces back through `raw_batch_id` + `raw_file` to the raw file and its manifest; `load_batch_id` → `etl_batch_log` shows which run loaded it.
- **`dq_results` has no FK to `etl_batch_log`** because raw batch ids come from the extractors, not from load runs.
