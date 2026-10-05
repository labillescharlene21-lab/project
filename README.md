# Global Water Quality Hotspot Pipeline

**Identifying Persistent Fecal-Indicator Pollution Hotspots for Water Rehabilitation Planning: A Global Multi-Source Water Quality Data Pipeline**

---

## Contents

1. [Project Overview and Problem Statement](#1-project-overview-and-problem-statement)
2. [Objectives and Scope](#2-objectives-and-scope)
3. [Team Members and Roles](#3-team-members-and-roles)
4. [Data Source Inventory](#4-data-source-inventory)
5. [Architecture and Technology Stack](#5-architecture-and-technology-stack)
6. [Repository Structure](#6-repository-structure)
7. [Installation and Prerequisites](#7-installation-and-prerequisites)
8. [Configuration and Environment Variables](#8-configuration-and-environment-variables)
9. [Starting the Docker Services](#9-starting-the-docker-services)
10. [Initializing PostgreSQL](#10-initializing-postgresql)
11. [Running the Pipeline](#11-running-the-pipeline)
12. [Running and Inspecting Airflow](#12-running-and-inspecting-airflow)
13. [Data Quality and Validation Approach](#13-data-quality-and-validation-approach)
14. [Expected Outputs](#14-expected-outputs)
15. [Known Limitations and Assumptions](#15-known-limitations-and-assumptions)
16. [Troubleshooting](#16-troubleshooting)
17. [Future Improvements](#17-future-improvements)

---

## 1. Project Overview and Problem Statement

Water pollution data is usually obtained from many different monitoring stations and organizations, which makes it hard to consolidate and analyze this data as well as find the locations with recurring water pollution problems. The main goal of this research is to create an effective data pipeline for integrating large water quality data with environmental data like precipitation and temperature to detect pollution hot spots.

Whether a water source is considered safe is usually decided by counting fecal indicator bacteria: *Escherichia coli* in fresh water and intestinal enterococci in marine water. 

Unfortunately the data is fragmented, thus planners have difficulty answering **which places have pollution problems that keep coming back, year after year?** A single high reading may be a one-off event. A site that exceeds safety thresholds in most years is a structural problem that needs rehabilitation. 

**Why a pipeline rather than a one-time analysis.** This project developed a data pipeline that combined water-quality observation data with the relevant environmental factors to detect pollution hotspots for decision making regarding water rehabilitation measures. Water quality observation data will be analyzed geographically and chronologically to find the regions where the water quality is poor constantly and find out what factors contribute to water pollution. Environmental factors like rainfall and temperature has been incorporated to analyze the influence of environmental conditions on water quality. **(tentative) Finally, analytics was be utilized to develop pollution hotspots map and a rehabilitation prioritization framework.**

**Key questions the data product answers**

1. Which monitoring sites exceed the single-sample safety threshold repeatedly across years?
2. Which administrative regions contain the most persistent hotspots?
3. Are exceedance rates higher after rainfall than in dry conditions?
4. Which regions should be prioritized for rehabilitation, and why?

**Stakeholders**

| Stakeholder | How they use the data product |
|---|---|
| Water rehabilitation planners / river basin organizations (primary) | Use the regional priority ranking to decide where to invest first |
| National and regional environmental regulators | Identify sites with persistent exceedances for compliance follow-up |
| Public health agencies | Identify recreational waters with recurring risk, and whether risk rises after rainfall |
| Local government units | See which hotspots fall within their administrative region |
| Researchers and NGOs | Reuse the harmonized, documented curated dataset |

**Expected data product**

- **Site hotspot table** (`mart_site_hotspot`): per-site persistence, severity, trend, and wet vs. dry exceedance rates
- **Regional priority ranking** (`mart_region_priority`, also exported as CSV): regions ranked by a documented, weighted priority score
- **Hotspot map** and supporting analytics

Full problem statement: [`docs/problem_statement.md`](docs/problem_statement.md)

## 2. Objectives and Scope

**Objectives**

1. Automatically ingest fecal indicator, weather, and boundary data from 5 independent sources in multiple formats.
2. Harmonize observations into one schema, one indicator vocabulary, and one unit (CFU/100 mL).
3. Apply automated data-quality checks at the raw, staging, and curated layers.
4. Store the curated data model in PostgreSQL with keys and relationships.
5. Orchestrate the full flow with Apache Airflow in a Dockerized environment.
6. Identify persistent hotspot sites and rank regions for rehabilitation priority.
7. Measure whether rainfall is associated with higher exceedance rates at hotspot sites.

**Scope**

| Item | Decision |
|---|---|
| Period | 2015–2025 |
| Scored indicators | *E. coli* (freshwater), enterococci (marine) |
| Supplementary indicators | Fecal coliform, total coliform: ingested and reported, not scored |
| Safety thresholds | EPA 2012 Recreational Water Quality Criteria, single-sample values: **410 CFU/100 mL** (*E. coli*), **130 CFU/100 mL** (enterococci) |
| Water bodies | Surface water: rivers, streams, lakes, reservoirs, estuaries, coastal/marine |
| Geography | Stratified global sample across 5 strata: United States, Europe, and GEMStat Africa, Asia, and Latin America |
| Sampling | Up to K sites per stratum and realm; sites need ≥ 24 samples of their primary indicator across ≥ 3 distinct years; fixed random seed. All settings in [`config/sampling.yaml`](config/sampling.yaml) |

**Why sample instead of ingesting everything.** Openly shared fecal indicator data is heavily concentrated in the United States and Europe. Taking the same number of sites from each stratum keeps well-documented regions from dominating the rankings, keeps the pipeline fast enough to rerun, and still exceeds the volume requirement. Sampling runs inside the pipeline with a fixed seed, so the same configuration always selects the same sites.

**Out of scope:** groundwater, drinking-water compliance, chemical pollutants and nutrients, real-time alerting, and data before 2015.

## 3. Team Members and Roles

| Name | Role | Responsibilities |
|---|---|---|
| Labilles | Project Manager | Problem definition, Validation, source inventory, profiling, diagrams, data dictionary and contract, report, slides, analytics |
| Cuyo | Data Engineer 1: Ingestion & Platform | Extractors, WQP site sampling, ingestion metadata, Docker environment, Airflow DAG |
| Vicente | Data Engineer 2: Transform & Storage | Staging and curated layers, PostgreSQL model and loads, partitioning, format benchmark |

## 4. Data Source Inventory

| # | Source | Provider | Access method | Format | Role in pipeline |
|---|---|---|---|---|---|
| 1 | GEMStat (via Open Water Quality export) | UNEP GEMS/Water; harmonized by UNU-INWEH | Open Water Quality export | CSV, GeoJSON | Global coverage: Africa, Asia, Latin America |
| 2 | Eionet bathing water (via Open Water Quality export) | European Environment Agency; harmonized by UNU-INWEH | Open Water Quality export | CSV, GeoJSON | Europe; main source of marine / enterococci data |
| 3 | Water Quality Portal | USGS, US EPA, NWQMC | REST API | CSV | United States; raw, unharmonized observations |
| 4 | Open-Meteo Historical Weather | Open-Meteo (ERA5 reanalysis) | REST API | JSON | Daily rainfall and temperature per site |
| 5 | Natural Earth Admin-1 (10m) | Natural Earth | Automated file download | Shapefile (zip) | Administrative regions for aggregation |

**Links**

- Open Water Quality: https://openwaterquality.org/
- Water Quality Portal web services: https://www.waterqualitydata.us/webservices_documentation/
- Open-Meteo Historical Weather API: https://open-meteo.com/en/docs/historical-weather-api
- Natural Earth Admin-1: https://www.naturalearthdata.com/?p=480

**Excluded:** WPdx (too few repeat samples per site to assess persistence). UK Open WIMS, DataStream, Hub'Eau, NMMP, and LAWA are listed as future improvements.

Full details for each source (access dates, update frequency, license, known limitations): [`docs/source_inventory.md`](docs/source_inventory.md)
Profiling results: [`docs/source_profiling.md`](docs/source_profiling.md)

## 5. Architecture and Technology Stack

![Architecture diagram](docs/architecture.png)

### Data flow

```
config/  (.env, sources.yaml, sampling.yaml)
   │
1. EXTRACT ────────────────────────────── data/raw/        untouched source files + manifest.json per batch
   ├─ Open Water Quality export: GEMStat, Eionet (CSV/GeoJSON)
   ├─ WQP site catalog → stratified sampling → WQP results (CSV)
   ├─ Open-Meteo, one request per 0.25° grid cell (JSON)
   └─ Natural Earth admin-1 boundaries (shapefile zip)
   │
2. VALIDATE RAW        files present, checksums, row counts, required columns
   │
3. STAGING ────────────────────────────── data/staging/    Parquet, partitioned by source= / year=
   harmonize schema, indicator names, units (CFU/100 mL), censored values,
   realm (fresh/marine), deduplicate, sample OWQ sites, spatial join to admin-1
   │
4. VALIDATE STAGING    types, ranges, accepted units, dates, coordinates, uniqueness
   │
5. CURATED ────────────────────────────── data/curated/
   join antecedent rainfall and temperature, exceedance flags, wet/dry flag,
   site-year metrics, persistent hotspot flag, rehabilitation priority score
   │
6. LOAD POSTGRESQL     UPSERT on natural keys (dimensions, facts, marts, run logs)
   │
7. VALIDATE CURATED    referential integrity, raw → staging → curated reconciliation
   │
8. CONSUME             SQL queries, priority ranking CSV, hotspot map
```

Apache Airflow orchestrates steps 1–7 as one DAG. Docker Compose runs PostgreSQL, Airflow, and the pipeline environment.

### Layer rules

| Layer | Allowed | Not allowed |
|---|---|---|
| **Raw** | Save files exactly as received; add a manifest (source, request, timestamp, row count, checksum) | Renaming columns, filtering rows, fixing values |
| **Staging** | Type casting, unit and name standardization, censored-value handling, deduplication, filtering to scope, site sampling, spatial join | Aggregation, business scoring |
| **Curated** | Joins across sources, derived metrics, hotspot and priority rules | Changing staging values without a documented rule |

### Technology stack

| Component | Tool | Why this tool |
|---|---|---|
| Language | Python | Mature libraries for APIs, tabular data, and geospatial work; required stack |
| Data processing | pandas + PyArrow | Dataset fits comfortably in memory after sampling; PyArrow provides Parquet read/write |
| Geospatial | GeoPandas | Spatial join of monitoring sites to administrative regions |
| Raw formats | CSV, JSON, GeoJSON, Shapefile | Kept as delivered by each source, preserving traceability |
| Staging format | Parquet (partitioned) | Columnar, compressed, preserves data types; partitions allow reading only the needed source/year |
| Database | PostgreSQL | Relational model with primary and foreign keys; `ON CONFLICT` UPSERT supports rerun safety |
| Orchestration | Apache Airflow | Task dependencies, retries, parameters, scheduling, and per-task logs for diagnosing failures |
| Environment | Docker, Docker Compose | Same services and versions on every machine |
| Version control | Git, GitHub | Change history and per-member contribution tracking |

Versions are pinned in `requirements.txt` and `docker-compose.yml`.

Data flow / lineage diagram: [`docs/data_flow.png`](docs/data_flow.png) · ERD: [`docs/erd.png`](docs/erd.png)

## 6. Repository Structure

```
wq-hotspot-pipeline/
├── README.md
├── requirements.txt            # Python dependencies (pinned)
├── .gitignore
├── .env.example                # configuration template; copy to .env (never committed)
├── Dockerfile                  # pipeline image
├── docker-compose.yml          # PostgreSQL, Airflow, pipeline services
│
├── config/
│   ├── sources.yaml            # endpoints, request settings, retry policy
│   ├── sampling.yaml           # strata, eligibility rules, K, random seed
│   └── schemas/                # required columns per source (used by validation)
│
├── dags/
│   └── wq_hotspot_pipeline.py  # Airflow DAG
│
├── data/                       # contents ignored by Git; folders kept with .gitkeep
│   ├── landing/owq/            # only used if the OWQ export cannot be scripted
│   ├── raw/                    # source-faithful files + manifests, by source and batch
│   ├── staging/                # harmonized Parquet, partitioned by source= / year=
│   └── curated/                # analysis-ready outputs
│
├── docs/
│   ├── problem_statement.md
│   ├── source_inventory.md
│   ├── source_profiling.md
│   ├── architecture.*          # architecture diagram
│   ├── data_flow.*             # lineage diagram
│   ├── erd.*                   # database schema diagram
│   ├── data_dictionary.md
│   ├── data_contract.md
│   └── evidence/               # screenshots of DAG runs, failure logs, rerun tests
│
├── notebooks/                  # profiling and analytics only; no production logic
│
├── src/
│   ├── extract/                # owq, wqp_sites, wqp_results, weather, boundaries
│   ├── transform/              # staging, curated
│   ├── load/                   # postgres (UPSERT loads)
│   ├── validation/             # raw_checks, staging_checks, curated_checks
│   └── utils/                  # config, log, http, manifest, paths
│
├── sql/
│   ├── init/
│   │   └── 01_schema.sql       # DDL: tables, keys, constraints (runs on first DB start)
│   └── queries.sql             # representative queries (run manually)
├── tests/                      # unit tests (e.g. sampling reproducibility)
└── outputs/                    # DQ reports, benchmarks, priority ranking, maps
```

---

<!--
============================================================
SECTIONS 7–17: TO BE WRITTEN BY THE DATA ENGINEERS
Rules:
- Write your section in the same PR as the code it describes.
- Every command must be copy-pasteable and tested on a fresh clone.
- Names (DAG id, table names, file paths) must match the code exactly.
- Delete the guidance comments and TODO lines before submission.
============================================================
-->

## 7. Installation and Prerequisites
**Required software**

| Software | Version | Notes |
|---|---|---|
| Docker Desktop (Windows/Mac) or Docker Engine (Linux) | Any recent version; tested with Docker 29.7.2 | Must be running before any `docker` command |
| Docker Compose | v2.24 or newer; tested with v5.4.0 | Included with Docker Desktop. Check with `docker compose version` |
| Git | Any recent version | To clone the repository |

Everything else (Python 3.11, Apache Airflow 3.3.2, PostgreSQL 16.15, and all Python packages) runs inside Docker, so it does **not** need to be installed on your machine.

**Resources**

- **Memory:** Docker must be allowed at least **4 GB** of memory (tested with 7.7 GB). In Docker Desktop: Settings → Resources → Memory. Check the current value with `docker info | grep -i "total memory"`.
- **Disk:** about **10 GB** free for the Docker images and databases.
- **Internet:** required for the first start (downloading the Docker images and Python packages) and when the pipeline calls the source APIs.
- **Ports:** `8080` (Airflow UI) and `5432` (PostgreSQL) must be free. If `5432` is taken by a local PostgreSQL, set `POSTGRES_PORT` to another port in `.env` (see §8).

**Get the project**

```bash
git clone https://github.com/labillescharlene21-lab/project.git
cd project
```

**Optional: create a `.env` file**

The project runs **without** a `.env` file; every setting has a default. Create one only if you want to change a setting:

```bash
cp .env.example .env
```

On Windows (Command Prompt), use `copy .env.example .env` instead.

**Linux only:** set `AIRFLOW_UID` in `.env` to your user ID (the output of `id -u`); otherwise files created by the containers in `data/` and `outputs/` may not be writable by you.

<!-- Include: required software and versions (Docker Desktop / Compose v2, Git), minimum Docker memory, disk space, internet access, and the git clone + cp .env.example .env steps. -->

## 8. Configuration and Environment Variables


<!-- Include: table of every variable in .env.example (name, example value, description); what lives in config/*.yaml vs .env; statement that no secrets are committed. -->

## 9. Starting the Docker Services

**Start everything**

```bash
docker compose up -d
```

The **first start takes several minutes**: Docker downloads the images and builds the pipeline image from the `Dockerfile`. Later starts take under a minute.

**Check that everything is healthy**

Wait 1–2 minutes, then run:

```bash
docker compose ps
```

Every service should show `Up ... (healthy)`. If some show `(health: starting)`, wait a little and run it again. `airflow-init` is not listed: it runs once to set up Airflow, then exits.

**Services and ports**

| Service | What it is | Port on your machine |
|---|---|---|
| `postgres` | Project warehouse database (`water_quality`) | `5432` (or `POSTGRES_PORT`) |
| `airflow-db` | Airflow's own metadata database | not exposed |
| `airflow-apiserver` | Airflow web UI and API | `8080`: http://localhost:8080 |
| `airflow-scheduler` | Schedules and runs DAG tasks (LocalExecutor) | none |
| `airflow-dag-processor` | Reads and parses the DAG files in `dags/` | none |
| `airflow-init` | One-time setup: database migration and admin user | none (exits when done) |
| `pipeline` | Tools container for one-off commands, not started by `up` | none |

Log in to Airflow at http://localhost:8080 with `admin` / `change_me` (or the values of `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`).

**Run the tests**

```bash
docker compose run --rm pipeline python -m pytest -q
```

**Check the warehouse database**

```bash
docker compose exec postgres psql -U wq_user -d water_quality -c "select 1"
```

**Stop and reset**

| Command | What it does |
|---|---|
| `docker compose down` | Stops and removes the containers. **Data is kept.** |
| `docker compose down -v` | Stops everything **and deletes all data** (both databases). Use for a clean start. |

A full reset also re-runs `sql/init/01_schema.sql` (it only runs when the database is created):
```bash
docker compose down -v && docker compose up -d
```

**After changing `requirements.txt` or the `Dockerfile`**, rebuild the image:

```bash
docker compose up -d --build
```


<!-- Include: docker compose up -d / ps / down / down -v; list of services and ports; how to confirm everything is healthy. -->

## 10. Initializing PostgreSQL

**How the schema is created**

The warehouse schema is in [`sql/init/01_schema.sql`](sql/init/01_schema.sql) and implements [`docs/erd.md`](docs/erd.md). It is applied in two ways:

1. **Automatically, on the first start.** `docker-compose.yml` mounts `sql/init/` into the `postgres` container's `/docker-entrypoint-initdb.d/`, so the DDL runs once, when the `warehouse-data` volume is created. Only `sql/init/` runs at startup; `sql/queries.sql` is for manual use.
2. **Manually, at any time:**

```bash
   docker compose run --rm pipeline python -m src.load.init_db
```

   It applies the same file and prints the table list. It is **idempotent** (`CREATE TABLE IF NOT EXISTS`), so running it again changes nothing.

`CREATE TABLE IF NOT EXISTS` never changes a table that already exists. **After a column is added or changed in `01_schema.sql`, reset the database** so the new schema is created (this deletes all loaded data):

```bash
docker compose down -v && docker compose up -d --wait
```

**Tables created (11)**

| Table | Type | Contents |
|---|---|---|
| `dim_source` | dimension | Data sources (`wqp`, `owq_gemstat`, `owq_eionet`) |
| `dim_indicator` | dimension | Fecal indicators with realm, EPA 2012 threshold and whether they are scored |
| `dim_region` | dimension | Natural Earth admin-1 regions |
| `dim_grid_cell` | dimension | 0.25° weather grid cells |
| `dim_site` | dimension | Sampled monitoring sites with region, grid cell, realm and stratum |
| `fact_observation` | fact | One harmonized fecal-indicator result (CFU/100 mL) with exceedance and antecedent weather |
| `fact_weather_daily` | fact | Daily rainfall and mean temperature per grid cell |
| `mart_site_hotspot` | mart | Per site and scored indicator: persistence, severity, trend, wet vs dry exceedance |
| `mart_region_priority` | mart | Admin-1 regions ranked by the rehabilitation priority score |
| `etl_batch_log` | log | One row per pipeline batch; facts and marts link to it via `load_batch_id` |
| `dq_results` | log | Result of every data quality check |

Keys are natural keys (e.g. `site_key`, `obs_key`) so reruns produce the same keys. Foreign keys enforce the links in the ERD, and CHECK constraints reject impossible values (e.g. `value_cfu_100ml >= 0`, `realm IN ('freshwater', 'marine')`).

**Check the database with psql**

```bash
# List the tables
docker compose exec postgres psql -U wq_user -d water_quality -c '\dt'

# Columns, primary key, foreign keys and CHECK constraints of one table
docker compose exec postgres psql -U wq_user -d water_quality -P pager=off -c '\d fact_observation'

# Interactive session (type \q to quit)
docker compose exec postgres psql -U wq_user -d water_quality
```

To connect from a SQL client on your machine instead, use host `localhost`, port `5432` (or `POSTGRES_PORT` from `.env`), database `water_quality`, user `wq_user` and the password from `.env` (default `change_me`).


<!-- Include: how the DDL runs (automatic on first start, or manual command); list of tables created; how to connect with psql to check. -->

## 11. Running the Pipeline


<!-- Include: trigger command with defaults and with parameters (start_year, end_year, k, seed); expected runtime; rerun strategy (deterministic batch IDs + UPSERT) and link to rerun evidence in docs/evidence/. -->

## 12. Running and Inspecting Airflow


<!-- Include: UI URL and where credentials come from; DAG id; task order; retries/backoff; schedule; step-by-step how to find and read a failed task's log. -->

## 13. Data Quality and Validation Approach

Validation runs automatically at three stages of the pipeline. Every check produces a pass/fail result; **critical** failures stop the Airflow task, **warnings** are recorded but don't stop the run. Each stage writes a JSON report to `outputs/dq/dq_report_{stage}_{run_id}.json` and, when Postgres is available, inserts the results into the `dq_results` table.

### Raw stage (VAL-1, `src/validation/raw_checks.py`)

Runs on every extractor's `manifest.json` before staging reads the data.

| Check | What it tests | Severity |
|---|---|---|
| `manifest_status_success` | The extraction finished (`status == "success"`) | Critical |
| `files_exist_non_empty` | Every file listed in the manifest exists and is not empty | Critical |
| `checksum_matches` | Each file's SHA-256 still matches the manifest (nothing changed after download) | Critical |
| `row_count_matches` | CSV data rows on disk (header and `#` comment lines excluded) equal the manifest's count | Critical |
| `required_columns_present` | Data files contain every column in `config/schemas/{source_code}.yaml` (CSV headers; shapefile fields) | Critical |
| `json_shape` | Open-Meteo files contain the required keys, and every daily series is as long as `daily.time` | Critical |

Which files each check applies to is set by `data_files` in the source's schema file, so helper files (e.g. `sampled_sites.csv`, `grid_cells.csv`) aren't checked against the source's data columns.

Run manually:
```
python -m src.validation.raw_checks --all
python -m src.validation.raw_checks --manifest data/raw/<source>/<batch>/manifest.json
```

### Staging stage (VAL-2) and curated stage (VAL-3)

TODO (VAL-2 / VAL-3): schema and type checks, keys, accepted values, ranges, referential integrity, raw → staging → curated reconciliation.

## 14. Expected Outputs


<!-- Include: table of outputs (raw, staging Parquet, curated tables, marts, priority CSV, DQ reports, format benchmark, maps) with location and description; hotspot and priority score rules with the final thresholds and weights; link to sql/queries.sql. -->

## 15. Known Limitations and Assumptions
- **Lab upper limits:** GEMStat values cluster at 24,196 and 2,419,600 MPN/100 mL (reporting maximums, "at least this much"), so severity is understated for Latin America; exceedance/hotspots unaffected. New column `pct_at_lab_upper_limit` shows the share.
- **Censoring only visible for WQP** (5–13%); OWQ values arrive pre-filled.
- **~55% nulls** in `activity_id`, `activity_type`, `result_status` = OWQ rows (OWQ has no such fields).
- **1,449 WQP rows (~11%) dropped as unmapped units**, mostly `MPN`/`CFU` without volume (STG-2 follow-up).
- **Cross-stratum differences reflect monitoring design** (GEMStat river stations vs Eionet bathing sites vs WQP mixed networks), not only pollution.

## 16. Troubleshooting


<!-- Include: problems actually hit during development and their fixes, e.g. ports in use, Airflow memory, Linux file permissions (AIRFLOW_UID), API rate limits (HTTP 429), missing OWQ landing file, full reset procedure. -->

## 17. Future Improvements


<!-- Include: e.g. native API ingestion for Hub'Eau / DataStream / UK Open WIMS, incremental loading by date watermark, storm-event analysis, watershed-based regions (HydroBASINS). -->

---

## Acknowledgements

Data from the Open Water Quality project (UNU-INWEH, Colorado State University, University of Colorado Boulder), the Water Quality Portal (USGS, US EPA, NWQMC), Open-Meteo, and Natural Earth. Observations remain the property of the contributing monitoring programmes; see the attribution file bundled with each Open Water Quality export.
