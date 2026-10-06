## Architecture 
 
<img width="4327" height="1967" alt="image" src="https://github.com/user-attachments/assets/0c4f6625-78e9-491f-8799-45961e88c957" />

 
> README §5 links `docs/architecture.png`. Export this diagram to PNG (e.g. mermaid.live → PNG) and save it under that name, or change the README link to this file.
 
## 2. Components
 
| Component | Runs in | Code / file | Responsibility |
|---|---|---|---|
| Extractors | Airflow worker (pipeline image) | `src/extract/*.py` | Download each source and save it **exactly as received** in `data/raw/{source_code}/batch_id={batch_id}/`, with a `manifest.json` |
| Raw validation (VAL-1) | Airflow | `src/validation/raw_checks.py` | Manifest status, file presence, SHA-256, row counts, required columns, JSON shape |
| Sites + sampling (STG-1) | Airflow | `src/transform/sites.py` | One site table across sources, admin-1 spatial join, realm and stratum, stratified OWQ sampling (seed 42) |
| Staging (STG-2) | Airflow | `src/transform/staging.py`, `harmonize.py` | Indicator names, units → CFU/100 mL, censoring, filters, natural keys, drop log; writes partitioned Parquet |
| Staging validation (VAL-2) | Airflow | `src/validation/staging_checks.py` | Schema, nulls, key uniqueness, accepted values, ranges, dates, coordinates, site existence |
| Curated (CUR-1) | Airflow | `src/transform/curated.py` | Dimensions and facts, antecedent weather (48 h / 72 h), `exceeds_threshold`, `is_wet` |
| Marts (MART-1) | Airflow | `src/transform/marts.py` | Site hotspot metrics and regional priority ranking, all rules from `config/business_rules.yaml` |
| Load (DB-1 / LOAD-1) | Airflow | `src/load/init_db.py`, `src/load/postgres.py` | Apply DDL; UPSERT curated tables and marts into PostgreSQL |
| Curated validation (VAL-3) | Airflow | `src/validation/curated_checks.py` | Tables present, referential integrity, PK uniqueness, raw → staging → curated reconciliation, rank consistency |
| Orchestrator (ING-6) | `airflow-scheduler`, `airflow-dag-processor`, `airflow-apiserver` | `dags/wq_hotspot_pipeline.py` | Task order, retries, parameters, per-task logs |
| Warehouse | `postgres` container | `sql/init/01_schema.sql` | 11 tables with PKs, FKs and CHECK constraints ([`erd.md`](erd.md)) |
| Tools container | `pipeline` service (`docker compose run --rm pipeline ...`) | same image | One-off commands: tests, `init_db`, manual module runs, benchmark |
 
> **To update when merged:** `dags/wq_hotspot_pipeline.py` (ING-6) and `src/load/postgres.py` (LOAD-1) are still empty on `main`. Replace the "Orchestrator" and "Load" rows with the real DAG id, task ids and load command once those PRs land.
 
## 3. Storage layers
 
| Layer | Location | Format | Written by | Rule |
|---|---|---|---|---|
| Landing (fallback only) | `data/landing/owq/` | as exported | a person | Only if the OWQ export cannot be scripted. Not used: the export is scripted (ING-2) |
| Raw | `data/raw/{source_code}/batch_id={batch_id}/` | CSV, zip, JSON, Shapefile | extractors | Untouched files + `manifest.json`. No cleaning |
| Staging | `data/staging/` | Parquet, partitioned `source_code=` / `year=` | STG-1, STG-2 | Typed, harmonized, filtered to scope; schema = `config/staging_schema.yaml` |
| Curated | `data/curated/` | Parquet, one file per table | CUR-1, MART-1 | Joined, derived, scored; columns = [`erd.md`](erd.md) |
| Warehouse | PostgreSQL `water_quality` | relational | LOAD-1 | Same tables as curated + `etl_batch_log`, `dq_results` |
| Outputs | `outputs/` | CSV, JSON, Markdown | MART-1, VAL-*, BENCH-1 | Consumer-facing files. Git-ignored (see [`format_benchmark.md`](format_benchmark.md) §6 for committing evidence) |
 
## 4. Cross-cutting design
 
| Concern | How it is handled | Where |
|---|---|---|
| Configuration | Every endpoint, rule, threshold, weight, K and seed is in YAML; no values in code | `config/*.yaml` |
| Secrets | Only in `.env` (git-ignored); every variable has a non-secret default for local use | `.env.example`, `docker-compose.yml` |
| Paths | Built from `DATA_DIR` / `CONFIG_DIR` / `OUTPUT_DIR`; no machine-specific paths | `src/utils/paths.py` |
| Batch identity | `batch_id = {source_code}_{sha256(params)[:12]}`; no timestamps in params | `src/utils/manifest.py` |
| Rerun safety | Same params → same batch folder (skipped if complete); staging swapped in atomically; UPSERT on natural keys | extractors, `staging.write_partitions`, LOAD-1 |
| Resume | Manifest rewritten after every file (`partial`), `success` at the end | extractors (WQP results, Open-Meteo) |
| Network control | `PIPELINE_MODE=snapshot` (default) blocks all HTTP; only `live` downloads | `src/utils/http.py` |
| Provider limits | Throttling, `Retry-After`, Open-Meteo weighted-call budget (stops cleanly as `partial`) | `config/sources.yaml`, `src/extract/weather.py` |
| Logging | `stage= / source= / batch=` prefix on every line; no `print` | `src/utils/log.py` |
| Errors | Typed exceptions; a failed extract fails its Airflow task | `src/utils/exceptions.py` |
| Data quality | Critical check failure stops the run; warnings are recorded | `src/validation/core.py`, `dq_results`, `outputs/dq/` |
| Lineage | `raw_batch_id` + `raw_file` on every observation; `load_batch_id` → `etl_batch_log` | staging, curated, warehouse |
 
## 5. Technology choices
 
Summarized from README §5; the reasons below are the ones to defend in the report.
 
| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Processing engine | pandas + PyArrow | Spark, DuckDB | ~300 sampled sites fit in memory; Spark adds cluster overhead with no benefit at this size |
| Staging/curated format | Parquet | CSV, JSON | Typed, columnar, compressed, supports partition pruning. Measured in BENCH-1 ([`format_benchmark.md`](format_benchmark.md)) |
| Partitioning | `source_code=` / `year=` | by site, by indicator | Matches how downstream steps filter (one source, a period); keeps the partition count small |
| Warehouse | PostgreSQL 16 | SQLite, files only | Keys, FKs, CHECKs, `ON CONFLICT` UPSERT; required stack |
| Keys | Natural keys (`source_code:id`) | Surrogate serial IDs | Identical on every rerun, so UPSERT and lineage work without lookups |
| Orchestration | Airflow 3.3.2, LocalExecutor | Cron, Prefect | Required stack; LocalExecutor is enough for one machine |
| Sampling | Stratified, K=60 per stratum × realm, seed 42 | Ingest everything (11M+ rows) | Balances regions, keeps the weather pull within free-tier limits, stays rerunnable ([`business_rules.md`](business_rules.md) §6) |
 
## 6. Deployment and reproducibility
 
- Everything runs in Docker Compose: `postgres`, `airflow-db`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-init`, `pipeline` (README §9).
- Image `wq-hotspot-pipeline:airflow-3.3.2-py3.11` is built from the `Dockerfile` with Airflow's constraints file; all Python packages are pinned in `requirements.txt`.
- The repo is mounted into `/opt/project`; `data/`, `outputs/` and `docs/` are bind mounts, so results appear on the host.
- The professor's path: fresh clone → `docker compose up -d` → trigger the DAG in snapshot mode → identical row counts and rankings (AGENTS §7).
## 7. Known architectural limitations
 
- Single machine, LocalExecutor: no parallel workers across hosts. Fine at this volume.
- Weather is gridded reanalysis (0.25°), not station data, and weather days are UTC while sample dates are local.
- OWQ arrives pre-harmonized, so censoring and MPN handling for GEMStat/Eionet are OWQ's, not ours.
- Admin-1 regions are administrative, not watershed boundaries (future: HydroBASINS).
