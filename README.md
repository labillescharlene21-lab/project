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
| TODO | Project Manager | Problem definition, source inventory, profiling, diagrams, data dictionary and contract, report, slides, analytics |
| TODO | Data Engineer 1: Ingestion & Platform | Extractors, WQP site sampling, ingestion metadata, Docker environment, Airflow DAG |
| TODO | Data Engineer 2: Transform & Storage | Staging and curated layers, validation, PostgreSQL model and loads, partitioning, format benchmark |

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
│   ├── 01_schema.sql           # DDL: tables, keys, constraints
│   └── queries.sql             # representative queries
│
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


<!-- Include: required software and versions (Docker Desktop / Compose v2, Git), minimum Docker memory, disk space, internet access, and the git clone + cp .env.example .env steps. -->

## 8. Configuration and Environment Variables


<!-- Include: table of every variable in .env.example (name, example value, description); what lives in config/*.yaml vs .env; statement that no secrets are committed. -->

## 9. Starting the Docker Services


<!-- Include: docker compose up -d / ps / down / down -v; list of services and ports; how to confirm everything is healthy. -->

## 10. Initializing PostgreSQL


<!-- Include: how the DDL runs (automatic on first start, or manual command); list of tables created; how to connect with psql to check. -->

## 11. Running the Pipeline


<!-- Include: trigger command with defaults and with parameters (start_year, end_year, k, seed); expected runtime; rerun strategy (deterministic batch IDs + UPSERT) and link to rerun evidence in docs/evidence/. -->

## 12. Running and Inspecting Airflow


<!-- Include: UI URL and where credentials come from; DAG id; task order; retries/backoff; schedule; step-by-step how to find and read a failed task's log. -->

## 13. Data Quality and Validation Approach


<!-- Include: table of every check (name, stage, what it tests, critical vs warning); what happens on failure; where results go (dq_results table, outputs/dq_report_*.json); link to docs/data_contract.md. -->

## 14. Expected Outputs


<!-- Include: table of outputs (raw, staging Parquet, curated tables, marts, priority CSV, DQ reports, format benchmark, maps) with location and description; hotspot and priority score rules with the final thresholds and weights; link to sql/queries.sql. -->

## 15. Known Limitations and Assumptions


<!-- Include at least: OWQ data arrives pre-harmonized (MPN treated as CFU, censored values at half detection limit); sampling frequency and methods differ by country; weather is gridded reanalysis; Natural Earth admin-1 is beta and coarse for some countries; results describe sampled sites only; estuaries treated as marine; WPdx excluded; fecal/total coliform unscored. Add anything found in profiling. -->

## 16. Troubleshooting


<!-- Include: problems actually hit during development and their fixes, e.g. ports in use, Airflow memory, Linux file permissions (AIRFLOW_UID), API rate limits (HTTP 429), missing OWQ landing file, full reset procedure. -->

## 17. Future Improvements


<!-- Include: e.g. native API ingestion for Hub'Eau / DataStream / UK Open WIMS, incremental loading by date watermark, storm-event analysis, watershed-based regions (HydroBASINS). -->

---

## Acknowledgements

Data from the Open Water Quality project (UNU-INWEH, Colorado State University, University of Colorado Boulder), the Water Quality Portal (USGS, US EPA, NWQMC), Open-Meteo, and Natural Earth. Observations remain the property of the contributing monitoring programmes; see the attribution file bundled with each Open Water Quality export.