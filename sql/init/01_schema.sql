-- Warehouse schema (DB-1). Implements docs/erd.md exactly.
--
-- Idempotent: safe to run more than once (CREATE ... IF NOT EXISTS). It never changes a
-- table that already exists, so after editing a column here, reset the database:
--     docker compose down -v && docker compose up -d
--
-- Runs automatically when the postgres volume is first created (sql/init is mounted to
-- /docker-entrypoint-initdb.d), and manually with:  python -m src.load.init_db
--
-- Order: run log and dimensions first, then facts, then marts (foreign keys need their
-- target table to exist). Everything runs in one transaction: all tables or none.

BEGIN;

-- ---------------------------------------------------------------- run log

CREATE TABLE IF NOT EXISTS etl_batch_log (
    batch_id     varchar PRIMARY KEY,
    dag_run_id   varchar,
    stage        varchar NOT NULL,
    source_code  varchar,
    started_at   timestamptz,
    finished_at  timestamptz,
    status       varchar NOT NULL,
    rows_in      integer,
    rows_out     integer,
    params       jsonb,
    CONSTRAINT ck_etl_batch_log_stage  CHECK (stage IN ('extract', 'staging', 'curated', 'marts', 'load')),
    CONSTRAINT ck_etl_batch_log_status CHECK (status IN ('success', 'failed')),
    CONSTRAINT ck_etl_batch_log_rows   CHECK (rows_in >= 0 AND rows_out >= 0)
);
COMMENT ON TABLE etl_batch_log IS 'One row per pipeline batch; facts and marts point here through load_batch_id.';

-- ---------------------------------------------------------------- dimensions

CREATE TABLE IF NOT EXISTS dim_source (
    source_code    varchar PRIMARY KEY,
    source_name    varchar,
    provider       varchar,
    access_method  varchar,
    url            text,
    CONSTRAINT ck_dim_source_access_method CHECK (access_method IN ('api', 'export', 'download'))
);
COMMENT ON TABLE dim_source IS 'Data sources: wqp, owq_gemstat, owq_eionet.';

CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_code       varchar PRIMARY KEY,
    indicator_name       varchar,
    realm                varchar,
    threshold_cfu_100ml  numeric,
    is_scored            boolean NOT NULL,
    CONSTRAINT ck_dim_indicator_realm     CHECK (realm IN ('freshwater', 'marine')),
    CONSTRAINT ck_dim_indicator_threshold CHECK (threshold_cfu_100ml > 0)
);
COMMENT ON TABLE dim_indicator IS 'Fecal indicators; scored ones carry their realm and EPA 2012 threshold, supplementary ones have nulls.';

CREATE TABLE IF NOT EXISTS dim_region (
    region_code   varchar PRIMARY KEY,
    admin1_name   varchar,
    country_iso   varchar,
    country_name  varchar,
    continent     varchar
);
COMMENT ON TABLE dim_region IS 'Natural Earth admin-1 regions (states, provinces) used for aggregation.';

CREATE TABLE IF NOT EXISTS dim_grid_cell (
    cell_id   varchar PRIMARY KEY,
    cell_lat  numeric NOT NULL,
    cell_lon  numeric NOT NULL,
    CONSTRAINT ck_dim_grid_cell_coords CHECK (cell_lat BETWEEN -90 AND 90 AND cell_lon BETWEEN -180 AND 180)
);
COMMENT ON TABLE dim_grid_cell IS '0.25 degree weather grid cells (Open-Meteo requests are made per cell).';

CREATE TABLE IF NOT EXISTS dim_site (
    site_key         varchar PRIMARY KEY,
    source_code      varchar NOT NULL,
    source_site_id   varchar NOT NULL,
    region_code      varchar,
    cell_id          varchar,
    site_name        varchar,
    water_body_type  varchar,
    realm            varchar,
    stratum          varchar,
    latitude         numeric NOT NULL,
    longitude        numeric NOT NULL,
    region_match     varchar NOT NULL,
    CONSTRAINT uq_dim_site_source_site   UNIQUE (source_code, source_site_id),
    CONSTRAINT fk_dim_site_source        FOREIGN KEY (source_code) REFERENCES dim_source (source_code),
    CONSTRAINT fk_dim_site_region        FOREIGN KEY (region_code) REFERENCES dim_region (region_code),
    CONSTRAINT fk_dim_site_cell          FOREIGN KEY (cell_id)     REFERENCES dim_grid_cell (cell_id),
    CONSTRAINT ck_dim_site_realm         CHECK (realm IN ('freshwater', 'marine')),
    CONSTRAINT ck_dim_site_region_match  CHECK (region_match IN ('within', 'nearest', 'none')),
    CONSTRAINT ck_dim_site_coords        CHECK (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
);
COMMENT ON TABLE dim_site IS 'Sampled monitoring sites with region, weather cell, realm and stratum.';

-- ---------------------------------------------------------------- facts

CREATE TABLE IF NOT EXISTS fact_weather_daily (
    cell_id           varchar NOT NULL,
    weather_date      date NOT NULL,
    precipitation_mm  numeric,
    temp_mean_c       numeric,
    raw_batch_id      varchar,
    CONSTRAINT pk_fact_weather_daily       PRIMARY KEY (cell_id, weather_date),
    CONSTRAINT fk_fact_weather_daily_cell  FOREIGN KEY (cell_id) REFERENCES dim_grid_cell (cell_id),
    CONSTRAINT ck_fact_weather_daily_precip CHECK (precipitation_mm >= 0)
);
COMMENT ON TABLE fact_weather_daily IS 'Daily rainfall and mean temperature per grid cell (Open-Meteo, ERA5).';

CREATE TABLE IF NOT EXISTS fact_observation (
    obs_key            varchar PRIMARY KEY,
    site_key           varchar NOT NULL,
    indicator_code     varchar NOT NULL,
    source_record_id   varchar NOT NULL,
    activity_id        varchar,
    activity_type      varchar,
    sample_date        date NOT NULL,
    value_cfu_100ml    numeric,
    original_value     varchar,
    original_unit      varchar,
    is_censored        boolean NOT NULL DEFAULT false,
    censor_direction   varchar,
    detection_limit    numeric,
    exceeds_threshold  boolean,
    rain_48h_mm        numeric,
    rain_72h_mm        numeric,
    temp_mean_c        numeric,
    is_wet             boolean,
    raw_batch_id       varchar NOT NULL,
    raw_file           varchar NOT NULL,
    load_batch_id      varchar,
    CONSTRAINT fk_fact_observation_site       FOREIGN KEY (site_key)       REFERENCES dim_site (site_key),
    CONSTRAINT fk_fact_observation_indicator  FOREIGN KEY (indicator_code) REFERENCES dim_indicator (indicator_code),
    CONSTRAINT fk_fact_observation_load_batch FOREIGN KEY (load_batch_id)  REFERENCES etl_batch_log (batch_id),
    CONSTRAINT ck_fact_observation_value      CHECK (value_cfu_100ml >= 0),
    CONSTRAINT ck_fact_observation_censor     CHECK (censor_direction IN ('<', '>')),
    CONSTRAINT ck_fact_observation_limit      CHECK (detection_limit >= 0),
    CONSTRAINT ck_fact_observation_rain       CHECK (rain_48h_mm >= 0 AND rain_72h_mm >= 0)
);
COMMENT ON TABLE fact_observation IS 'One harmonized fecal-indicator result in CFU/100 mL, with exceedance and antecedent weather.';

-- ---------------------------------------------------------------- marts

CREATE TABLE IF NOT EXISTS mart_site_hotspot (
    site_key                   varchar NOT NULL,
    indicator_code             varchar NOT NULL,
    region_code                varchar,
    realm                      varchar,
    n_observations             integer,
    n_samples                  integer,
    years_monitored            smallint,
    insufficient_years         smallint,
    poor_years                 smallint,
    is_persistent_hotspot      boolean,
    exceedance_rate            numeric,
    median_ratio_to_threshold  numeric,
    trend_slope                numeric,
    wet_exceedance_rate        numeric,
    dry_exceedance_rate        numeric,
    batch_id                   varchar,
    load_batch_id              varchar,
    CONSTRAINT pk_mart_site_hotspot            PRIMARY KEY (site_key, indicator_code),
    CONSTRAINT fk_mart_site_hotspot_site       FOREIGN KEY (site_key)       REFERENCES dim_site (site_key),
    CONSTRAINT fk_mart_site_hotspot_indicator  FOREIGN KEY (indicator_code) REFERENCES dim_indicator (indicator_code),
    CONSTRAINT fk_mart_site_hotspot_load_batch FOREIGN KEY (load_batch_id)  REFERENCES etl_batch_log (batch_id),
    CONSTRAINT ck_mart_site_hotspot_realm      CHECK (realm IN ('freshwater', 'marine')),
    CONSTRAINT ck_mart_site_hotspot_rates      CHECK (exceedance_rate BETWEEN 0 AND 1
                                                      AND wet_exceedance_rate BETWEEN 0 AND 1
                                                      AND dry_exceedance_rate BETWEEN 0 AND 1)
);
COMMENT ON TABLE mart_site_hotspot IS 'Per site and scored indicator: persistence, severity, trend and wet vs dry exceedance.';

CREATE TABLE IF NOT EXISTS mart_region_priority (
    region_code             varchar PRIMARY KEY,
    admin1_name             varchar,
    country_iso             varchar,
    country_name            varchar,
    status                  varchar NOT NULL,
    n_sites                 integer,
    n_hotspots              integer,
    persistence_raw         numeric,
    severity_raw            numeric,
    trend_raw               numeric,
    rain_sensitivity_raw    numeric,
    persistence_score       numeric,
    severity_score          numeric,
    trend_score             numeric,
    rain_sensitivity_score  numeric,
    priority_score          numeric,
    priority_rank           integer,
    rank_in_country         integer,
    notes                   text,
    batch_id                varchar,
    load_batch_id           varchar,
    CONSTRAINT fk_mart_region_priority_region     FOREIGN KEY (region_code)   REFERENCES dim_region (region_code),
    CONSTRAINT fk_mart_region_priority_load_batch FOREIGN KEY (load_batch_id) REFERENCES etl_batch_log (batch_id),
    CONSTRAINT ck_mart_region_priority_status     CHECK (status IN ('ranked', 'insufficient_sites')),
    CONSTRAINT ck_mart_region_priority_rank       CHECK (priority_rank >= 1 AND rank_in_country >= 1)
);
COMMENT ON TABLE mart_region_priority IS 'Admin-1 regions ranked by the weighted rehabilitation priority score.';

-- ---------------------------------------------------------------- data quality

CREATE TABLE IF NOT EXISTS dq_results (
    dq_result_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        varchar,
    batch_id      varchar,  -- no FK: raw batch ids come from the extractors, not etl_batch_log
    check_name    varchar NOT NULL,
    stage         varchar,
    source_code   varchar,
    severity      varchar NOT NULL,
    status        varchar NOT NULL,
    failed_count  integer,
    details       text,
    checked_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_dq_results_stage    CHECK (stage IN ('raw', 'staging', 'curated')),
    CONSTRAINT ck_dq_results_severity CHECK (severity IN ('critical', 'warning')),
    CONSTRAINT ck_dq_results_status   CHECK (status IN ('pass', 'fail'))
);
COMMENT ON TABLE dq_results IS 'Result of every data quality check, per run, stage and source.';

-- ---------------------------------------------------------------- indexes

CREATE INDEX IF NOT EXISTS ix_fact_observation_site_date ON fact_observation (site_key, sample_date);
CREATE INDEX IF NOT EXISTS ix_fact_observation_indicator ON fact_observation (indicator_code);
CREATE INDEX IF NOT EXISTS ix_dim_site_region            ON dim_site (region_code);

COMMIT;