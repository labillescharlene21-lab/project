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

<img width="4327" height="1967" alt="image" src="https://github.com/user-attachments/assets/0c4f6625-78e9-491f-8799-45961e88c957" />

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

Settings live in two places:
 
| Where | What goes there | Committed to Git? |
|---|---|---|
| `.env` (copied from `.env.example`) | Machine and deployment settings: ports, passwords, user IDs, pipeline mode | **No** (`.gitignore`). Optional: every variable has a default |
| `config/*.yaml` | Everything that decides **what** the pipeline does: endpoints, sampling, mappings, rules | Yes |
 
**Environment variables (`.env.example`)**
 
| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `water_quality` | Warehouse database name, created on first start |
| `POSTGRES_USER` | `wq_user` | Warehouse user, created on first start |
| `POSTGRES_PASSWORD` | `change_me` | Warehouse password. Required by `src/load/db.py` (no code default); change it beyond local use |
| `POSTGRES_PORT` | `5432` | Port on **your machine**. Change it if a local PostgreSQL already uses 5432; inside Docker it is always 5432 |
| `POSTGRES_HOST` | `postgres` | Warehouse host name; inside Docker always `postgres` |
| `PIPELINE_MODE` | `snapshot` | `snapshot` = run from the raw files already in `data/raw/`; `live` = allowed to call the source APIs |
| `DATA_DIR` | `/opt/project/data` | Data folder inside the containers; all paths are built from it (`src/utils/paths.py`) |
| `CONFIG_DIR` | `/opt/project/config` | Config folder inside the containers |
| `AIRFLOW_UID` | `50000` | User ID the containers run as. **Linux:** set it to the output of `id -u` |
| `AIRFLOW_ADMIN_USER` | `admin` | Airflow UI login |
| `AIRFLOW_ADMIN_PASSWORD` | `change_me` | Airflow UI password |
| `FERNET_KEY` | *(blank)* | Encrypts saved Airflow connections; blank = unencrypted, fine locally |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `airflow_jwt_secret` | Signs Airflow's internal API tokens |
| `AIRFLOW__API_AUTH__JWT_ISSUER` | `airflow` | Issuer name for those tokens |
 
Optional, not in `.env.example`: `OUTPUT_DIR` (default: `outputs/` next to the data folder) and `LOG_LEVEL` (default `INFO`).
 
**Configuration files (`config/`)**
 
| File | Contents |
|---|---|
| `sources.yaml` | Endpoints, request settings, retries and backoff, OWQ row cap, WQP characteristic names and site types, Open-Meteo variables and rate budgets, Natural Earth URL |
| `sampling.yaml` | Period (2015–2025), realms with primary indicator and threshold, eligibility, K, seed, water-body → realm mapping, strata |
| `mappings.yaml` | Indicator names, unit factors, censoring rules, row filters, record-ID rules, drop reasons |
| `business_rules.yaml` | Hotspot and priority rules: exceedance, persistence, rainfall, trend, weights. Justified in [`docs/business_rules.md`](docs/business_rules.md) |
| `staging_schema.yaml` | Staging tables: columns, types, nullability, keys |
| `source_catalog.yaml` | Descriptive names for `dim_source` and `dim_indicator` |
| `schemas/{source_code}.yaml` | Required raw columns per source, used by raw validation and profiling |
 
**No secrets are committed.** `.env` is git-ignored, no code contains credentials, and none of the sources need an API key. Changing a rule value is a config change only; no code edits are needed.

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
<!-- TODO (ING-6): replace the DAG id, parameter names and runtime below with the real values once the DAG is merged. -->
 
**Full run (Airflow).** With the services healthy (§9), the DAG runs every step in order: extract → validate raw → sites and sampling → weather → staging → validate staging → curated → marts → load → validate curated.
 
```bash
docker compose exec airflow-scheduler airflow dags unpause wq_hotspot_pipeline
docker compose exec airflow-scheduler airflow dags trigger wq_hotspot_pipeline
```
 
With parameters (any not given use the values in `config/sampling.yaml`):
 
```bash
docker compose exec airflow-scheduler airflow dags trigger wq_hotspot_pipeline --conf '{"start_year": 2015, "end_year": 2025, "k": 60, "seed": 42}'
```
 
On Windows PowerShell, quotes inside JSON are not passed reliably to `docker`, so trigger with parameters from the UI instead (§12): **Trigger DAG** → edit the parameters.
 
**Step by step (without Airflow).** Each step is also a module with a command-line entry point. Useful for debugging one step; run them in this order:
 
| # | Step | Command (prefix each with `docker compose run --rm pipeline`) |
|---|---|---|
| 1 | Extract OWQ (GEMStat + Eionet) | `python -m src.extract.owq` |
| 2 | Extract WQP summary and sample US sites | `python -m src.extract.wqp_sites` |
| 3 | Extract WQP results for sampled sites | `python -m src.extract.wqp_results` |
| 4 | Extract Natural Earth boundaries | `python -m src.extract.boundaries` |
| 5 | Validate raw | `python -m src.validation.raw_checks --all` |
| 6 | Build sites, regions, OWQ sampling | `python -m src.transform.sites` |
| 7 | Extract weather for sampled sites | `python -m src.extract.weather` (check cost first with `--plan`) |
| 8 | Build staging observations | `python -m src.transform.staging` |
| 9 | Validate staging | `python -m src.validation.staging_checks` |
| 10 | Build curated tables | `python -m src.transform.curated` |
| 11 | Build marts and ranking | `python -m src.transform.marts` |
| 12 | Load PostgreSQL | *(LOAD-1: command to add)* |
| 13 | Validate curated and reconcile | `python -m src.validation.curated_checks` |
 
**Expected runtime.** *(fill from the final run)* With all raw batches present, the transform, load and validation steps take minutes. A first **live** extraction is much longer: the Open-Meteo free tier allows about 156 grid cells per day, so the weather step may stop as `partial` and resume on the next run.
 
**Rerun strategy.** Running twice with the same parameters gives the same result and never duplicates data:
 
- **Deterministic batch IDs.** `batch_id = {source}_{sha256(params)[:12]}` with no timestamps, so the same parameters write to the same raw folder. A complete batch is detected **before** any network call and skipped; an interrupted one resumes from its last completed file.
- **Atomic staging.** Staging is written to a temporary folder and swapped in, so no partitions from an earlier run are left behind.
- **UPSERT on natural keys.** Loads use `INSERT … ON CONFLICT (pk) DO UPDATE`, so reloading the same rows updates them instead of adding copies.
- **Fixed seed** (42) and sorted outputs make site sampling and rankings identical on every run.
Evidence: `docs/evidence/` *(add the two-run row-count comparison, e.g. `rerun_row_counts.txt`)*.
 
## 12. Running and Inspecting Airflow
<!-- TODO (ING-6): confirm the DAG id, task ids, retries and schedule against dags/wq_hotspot_pipeline.py once merged. -->
 
**Open the UI:** http://localhost:8080. Log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` (default `admin` / `change_me`). DAGs are paused when first created (`DAGS_ARE_PAUSED_AT_CREATION`), so unpause `wq_hotspot_pipeline` with its toggle before the first run.
 
**DAG:** `wq_hotspot_pipeline` (`dags/wq_hotspot_pipeline.py`). Every task calls a function in `src/`; the DAG file contains no data logic.
 
**Task order** *(planned; confirm task ids against the DAG)*
 
```
extract_owq ─┐
extract_wqp_sites → extract_wqp_results ─┤
extract_boundaries ─┘
        → validate_raw → build_sites → extract_weather → build_staging → validate_staging
        → build_curated → build_marts → load_postgres → validate_curated
```
 
Independent extracts can run in parallel; everything after `validate_raw` runs in sequence because each step reads the previous one's output.
 
| Setting | Value | Why |
|---|---|---|
| Executor | LocalExecutor | One machine; no Redis/Celery needed |
| Retries | *(fill)* per task, exponential backoff | Recovers from short network failures; HTTP retries also happen inside the extractors (`config/sources.yaml › http`) |
| Schedule | None (manual trigger) | Sources update irregularly; a run is triggered when a refresh is needed |
| Catchup | Off | No historical backfill runs |
| Failure behaviour | A failed task stops downstream tasks | A critical data-quality failure raises an error, so bad data never reaches the next layer |
 
**Find and read a failed task's log**
 
1. Open http://localhost:8080 and click **wq_hotspot_pipeline**.
2. Open the **Grid** view. Each column is a run, each row a task; a red square is a failed task.
3. Click the red square, then the **Logs** tab. Each try has its own log.
4. Every pipeline line is tagged `stage=… | source=… | batch=…`. Scroll to the last `ERROR` line: the exception names the source, URL or check that failed.
5. For data-quality failures, open the report named in the log: `outputs/dq/dq_report_{stage}_{run_id}.json`, or query `dq_results` (§13).
6. Fix the cause, then click **Clear** on the failed task to rerun it and everything after it. Completed batches are skipped, so earlier work is not redone.
From the command line:
 
```bash
docker compose exec airflow-scheduler airflow dags list-runs wq_hotspot_pipeline
docker compose logs airflow-scheduler --tail 100
```
 
Evidence of a successful run, a failed run and its log: `docs/evidence/` *(add screenshots)*.
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

### Staging stage (VAL-2, `src/validation/staging_checks.py`)

Runs after STG-2 builds the staging Parquet, before CUR-1 reads it. Observations are read through `pyarrow.dataset` with hive partitioning, exactly as downstream steps see them.

| Check | What it tests | Severity |
|---|---|---|
| `schema_matches` | Observations and sites have the columns and types in `config/staging_schema.yaml` (integer width may differ: partition columns read back as int32) | Critical |
| `keys_not_null` | Columns declared `nullable: false` contain no nulls | Critical |
| `obs_key_unique` | No observation key appears twice, across all partitions | Critical |
| `accepted_values` | `indicator_code`, `source_code`, `censor_direction` and sampled sites' `realm` only use allowed values | Critical |
| `value_range` | No negative values; values above 1,000,000 CFU/100 mL flagged for review | Warning |
| `date_in_period` | Every `sample_date` is within the project period, and matches its `year` partition | Critical |
| `coordinates_valid` | Site latitude in [-90, 90], longitude in [-180, 180] | Critical |
| `observation_site_exists` | Every observation's `site_key` exists in `sites.parquet` | Critical |
| `stratum_coverage` | Each stratum × realm has at least `min_sites_to_keep` sampled sites (empty strata, e.g. GEMStat Asia/Africa, show here) | Warning |

Run manually:
```
python -m src.validation.staging_checks
```


## 14. Expected Outputs
| Output | Location | Description |
|---|---|---|
| Raw batches | `data/raw/{source_code}/batch_id={batch_id}/` | Source files exactly as received + `manifest.json` (request, checksum, row count) |
| Sites | `data/staging/sites/sites.parquet` | Every site from every source with realm, region, stratum, eligibility, `is_sampled` |
| Sampled sites | `data/staging/sampled_sites/sampled_sites.parquet` | The 300 sampled sites (input to the weather extract) |
| Staging observations | `data/staging/observations/source_code=*/year=*/part-0000.parquet` | Harmonized observations, partitioned by source and year |
| Drop log | `data/staging/_drop_log.parquet` | Every removed row, counted by reason |
| Curated tables | `data/curated/*.parquet` and PostgreSQL | `dim_source`, `dim_indicator`, `dim_region`, `dim_grid_cell`, `dim_site`, `fact_weather_daily`, `fact_observation` |
| Site hotspot mart | `mart_site_hotspot` (PostgreSQL + Parquet) | Per site: persistence, severity, trend, wet vs dry exceedance |
| Regional priority mart | `mart_region_priority` (PostgreSQL + Parquet) | Admin-1 regions with component scores and ranks |
| Priority ranking | `outputs/priority_ranking.csv` | Ranked regions only, for planners |
| Reconciliation | `outputs/reconciliation.csv` | Per source: raw rows, drops by reason, staging rows, curated rows |
| DQ reports | `outputs/dq/dq_report_{stage}_{run_id}.json` and `dq_results` | Every check's result |
| Source profiling | `outputs/profiling/` | Raw-data profiles ([`docs/source_profiling.md`](docs/source_profiling.md)) |
| Format benchmark | `outputs/benchmark/` | CSV vs JSON vs Parquet ([`docs/format_benchmark.md`](docs/format_benchmark.md)) |
| Hotspot map | *(fill)* | Map of persistent hotspots and ranked regions |
 
`outputs/` is git-ignored; committed copies of evidence are in `docs/evidence/`. Column definitions: [`docs/data_dictionary.md`](docs/data_dictionary.md).
 
**Hotspot rules** (`config/business_rules.yaml`, justified in [`docs/business_rules.md`](docs/business_rules.md))
 
| Rule | Value |
|---|---|
| Thresholds | *E. coli* > **410** CFU/100 mL (freshwater); enterococci > **130** (marine). EPA 2012 |
| Sample-day | Same-day replicates combined by shifted geometric mean |
| Poor year | More than **10%** of sample-days exceed; years with fewer than **5** sample-days are "insufficient" |
| Persistent hotspot | Poor in at least **3** of the site's most recent **5** classified years |
| Trend | Theil-Sen slope of annual exceedance rate, at least **3** years |
| Wet sample-day | At least **10 mm** of rain in the 48 h before sampling |
 
**Regional priority score**
 
```
priority_score = 0.40 × persistence + 0.30 × severity + 0.15 × trend + 0.15 × rain_sensitivity
```
 
| Component | Raw value per region | Weight |
|---|---|---|
| Persistence | Share of the region's sites that are persistent hotspots | 0.40 |
| Severity | Median of sites' median value ÷ threshold | 0.30 |
| Trend | Median site trend slope (worsening = higher) | 0.15 |
| Rain sensitivity | Median of sites' wet ÷ dry exceedance rate | 0.15 |
 
Each component is min-max scaled to 0–1 across ranked regions before weighting. Regions need at least **3** sites to be ranked (others are listed as `insufficient_sites`). Ties: more hotspots first, then region code.
 
**Example queries:** [`sql/queries.sql`](sql/queries.sql) answers the key questions: top 10 priority regions, hotspots per stratum and realm, wet vs dry exceedance, hotspots per country, and a lineage trace from the top region back to its raw file.
 
```bash
docker compose exec -T postgres psql -U wq_user -d water_quality < sql/queries.sql
```
 
On Windows PowerShell: `Get-Content sql/queries.sql | docker compose exec -T postgres psql -U wq_user -d water_quality`

## 15. Known Limitations and Assumptions
- **Lab upper limits:** GEMStat values cluster at 24,196 and 2,419,600 MPN/100 mL (reporting maximums, "at least this much"), so severity is understated for Latin America; exceedance/hotspots unaffected. New column `pct_at_lab_upper_limit` shows the share.
- **Censoring only visible for WQP** (5–13%); OWQ values arrive pre-filled.
- **~55% nulls** in `activity_id`, `activity_type`, `result_status` = OWQ rows (OWQ has no such fields).
- **1,449 WQP rows (~11%) dropped as unmapped units**, mostly `MPN`/`CFU` without volume (STG-2 follow-up).
- **Cross-stratum differences reflect monitoring design** (GEMStat river stations vs Eionet bathing sites vs WQP mixed networks), not only pollution.

## 16. Troubleshooting


| Problem | Cause | Fix |
|---|---|---|
| `docker: command not found` / cannot connect to the Docker daemon | Docker Desktop not running | Start Docker Desktop and wait until it says "running" |
| `port is already allocated` for 5432 | A local PostgreSQL uses the port | Set `POSTGRES_PORT=5433` in `.env`, then `docker compose up -d` |
| `port is already allocated` for 8080 | Another app uses 8080 | Stop that app (Airflow's port is fixed in `docker-compose.yml`) |
| Services stay `(health: starting)` or restart; Airflow is very slow | Docker has too little memory | Docker Desktop → Settings → Resources → Memory ≥ 4 GB |
| `PermissionError` writing `data/` or `outputs/` (Linux) | Containers run as a different user ID | Set `AIRFLOW_UID` in `.env` to the output of `id -u`, then `docker compose up -d` |
| New column in `01_schema.sql` doesn't appear | `CREATE TABLE IF NOT EXISTS` never changes an existing table; init scripts only run on a new volume | `docker compose down -v && docker compose up -d` (deletes loaded data) |
| `ConfigError: ... POSTGRES_PASSWORD` | Running a module outside Docker without the variable | Run through `docker compose run --rm pipeline ...`, or set it in `.env` |
| `SourceRequestError ... returned status 429` | Provider rate limit | Retries with backoff are automatic (`config/sources.yaml › http`). If it persists, wait and rerun; completed files are skipped |
| Weather manifest `status: partial` | Open-Meteo daily budget reached (expected on a first live run) | Rerun after 24 h; completed cells are skipped |
| WQP results ignore the date range | WQP POST ignores date filters in the query string | Already handled: dates are sent in the JSON body. Keep it that way if the extractor changes |
| `LandingFileMissingError` | `export_mode` set to landing but no OWQ file in `data/landing/owq/` | Use `export_mode: scripted` (default), or place the export file named as in `config/sources.yaml › landing_filename_pattern` |
| `FileNotFoundError: No successful raw batch for ...` | A step ran before the step that feeds it | Run the steps in the order of §11 |
| Pipeline image doesn't pick up new packages | Image built before `requirements.txt` changed | `docker compose up -d --build` |
| Windows: files saved by `>` look garbled (`ÿþ...`) | Windows PowerShell writes UTF-16 | Use `| Out-File -FilePath <file> -Encoding utf8` |
| Windows: a long pasted command is cancelled half-way | PowerShell treats multi-line pastes as several commands | Paste and run one command at a time |
| Shell script fails with `\r: command not found` | CRLF line endings | Keep LF endings (`.gitattributes`); re-checkout the file |
 
**Full reset** (deletes both databases and all containers; raw files in `data/` are kept):
 
```bash
docker compose down -v
docker compose up -d --build
```


## 17. Future Improvements

- **More sources through native APIs:** Hub'Eau (France), DataStream (Canada), UK Open WIMS, NMMP, LAWA (New Zealand), to fill the gaps behind the OWQ compilation and add stations outside the US and Europe.
- **Incremental loading:** a date watermark per site, so a refresh only requests observations newer than the last load.
- **Volume-less WQP units:** recover the ~11% of WQP rows dropped as `MPN`/`CFU` without a volume, once the reporting volume can be confirmed per organization.
- **Watershed-based regions:** aggregate by river basin (HydroBASINS) instead of administrative regions, closer to how rehabilitation is planned.
- **Storm-event analysis:** compare exceedance in the days after heavy rain events, not only a fixed 48 h window.
- **Higher-resolution and local-time weather:** station data where available, and weather aligned to the local sampling day.
- **Censored-data statistics:** survival-analysis methods for censored values instead of the half-limit substitution.
- **Interactive dashboard:** hotspot map and ranking served from the warehouse.
- **Scheduled refresh and alerting:** a regular schedule plus notifications when a critical check fails.
---
 
## Acknowledgements
 
Data from the Open Water Quality project (UNU-INWEH, Colorado State University, University of Colorado Boulder), the Water Quality Portal (USGS, US EPA, NWQMC), Open-Meteo, and Natural Earth. Observations remain the property of the contributing monitoring programmes; see the attribution file bundled with each Open Water Quality export.
 

Data from the Open Water Quality project (UNU-INWEH, Colorado State University, University of Colorado Boulder), the Water Quality Portal (USGS, US EPA, NWQMC), Open-Meteo, and Natural Earth. Observations remain the property of the contributing monitoring programmes; see the attribution file bundled with each Open Water Quality export.
