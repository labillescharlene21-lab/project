# AGENTS.md: Context and Rules for Anyone (Human or AI) Working on This Repo

Read this file completely before starting any ticket. Tickets reference the conventions below instead of repeating them.

---

## 1. Project context

**Project:** Global Water Quality Hotspot Pipeline (university Data Engineering project, graded against a rubric).

**Goal:** An automated, reproducible pipeline that ingests fecal indicator bacteria observations from several global sources, joins rainfall and temperature, identifies monitoring sites that exceed safety thresholds year after year ("persistent hotspots"), and ranks regions for water rehabilitation.

**Required stack:** Python, PostgreSQL, Apache Airflow, Docker / Docker Compose, Git/GitHub.
**Required layers:** Raw → Staging → Curated.

**Grading facts that shape every decision**
- Ingestion must be automated. Manual steps reduce the grade; "no actual automated ingestion" can cap the project at 65/100.
- Raw data must stay as close to the original as possible and be traceable to its origin. No cleaning in the raw layer.
- The pipeline must be rerun-safe: running twice with the same parameters must not duplicate or corrupt data.
- No hard-coded credentials, no absolute machine-specific paths, no secrets in Git.
- Production code lives in `src/` modules, never in notebooks.
- Every team member must be able to explain all code, including AI-generated code.

## 2. Sources

| source_code | What | Access | Raw format |
|---|---|---|---|
| `owq_gemstat` | GEMStat fecal indicator data via the Open Water Quality export (https://openwaterquality.org/, Data tab) | Export form (scripted if possible, else landing zone) | CSV / GeoJSON / zip |
| `owq_eionet` | Eionet (EEA bathing water) via the same export | Same | Same |
| `wqp_summary` | Water Quality Portal per-site annual sample counts (used for sampling) | REST API | CSV |
| `wqp_results` | Water Quality Portal results for sampled sites | REST API | CSV (zip) |
| `open_meteo` | Daily precipitation and temperature per 0.25° grid cell | REST API | JSON |
| `natural_earth` | Admin-1 (states/provinces) boundaries, 10m | File download | Shapefile zip |

Endpoints and request settings live in `config/sources.yaml`. Sampling rules live in `config/sampling.yaml`. Staging schema and mappings live in `config/staging_schema.yaml` and `config/mappings.yaml`. Hotspot and priority rules live in `config/business_rules.yaml`. **Never hard-code either.**

## 3. Scope decisions (do not change without PM approval)

- Period: 2015–2025.
- Scored indicators: E. coli (freshwater, threshold 410 CFU/100 mL), enterococci (marine, 130 CFU/100 mL).
- Supplementary indicators: fecal coliform, total coliform. Ingest them; never use them for sampling eligibility or scoring.
- Strata: `us` (WQP), `europe` (Eionet), `gemstat_africa`, `gemstat_asia`, `gemstat_latin_america`.
- Site eligibility and K are in `config/sampling.yaml`.

## 4. Repository layout (relevant parts)

```
config/sources.yaml, config/sampling.yaml, config/schemas/
dags/wq_hotspot_pipeline.py
data/landing/owq/            # human-placed files, only if OWQ export cannot be scripted
data/raw/{source_code}/batch_id={batch_id}/
data/staging/  data/curated/
src/extract/   owq.py, wqp_sites.py, wqp_results.py, weather.py, boundaries.py
src/transform/ src/load/ src/validation/ src/analysis/   # owned by DE2; do not edit in ING tickets
src/utils/     config.py, log.py, http.py, manifest.py, paths.py, exceptions.py, sampling.py
tests/
docs/evidence/
docs/tickets/all_issues.md       # every issue, grouped by phase
scripts/        make_snapshot.py, fetch_snapshot.py
.github/        pull_request_template.md
```

`data/**` is git-ignored except `.gitkeep` files. Never commit data.

## 5. Conventions

### 5.1 Paths
- All paths are built from the `DATA_DIR` environment variable via `src/utils/paths.py`. Never write `C:\...`, `/Users/...`, or `/home/...` in code.
- Raw batch folder: `data/raw/{source_code}/batch_id={batch_id}/`.

### 5.2 batch_id
- Deterministic: `f"{source_code}-{sha256(json.dumps(params, sort_keys=True, default=str))[:10]}"`.
- `params` = every parameter that changes what is downloaded (period, indicators, site list hash, K, seed, URL). Never include timestamps.
- Same parameters → same batch_id → same folder. This is how reruns avoid duplicates.

### 5.3 Manifest (`manifest.json` in every raw batch folder)
```json
{
  "manifest_version": 1,
  "source_code": "wqp_results",
  "batch_id": "wqp_results-3f9a2c1b7d",
  "status": "success | partial | failed",
  "retrieved_at_utc": "2026-09-27T08:15:02Z",
  "params": { "...": "exactly the params used for batch_id" },
  "requests": [ { "method": "POST", "url": "...", "query": {}, "body_ref": "chunk_0001", "http_status": 200 } ],
  "files": [ { "name": "chunk_0001.csv", "format": "csv", "size_bytes": 12345, "sha256": "...", "row_count": 980 } ],
  "total_rows": 980,
  "warnings": [],
  "notes": ""
}
```
- `row_count` = data rows excluding header (use the `csv` module, not line counting). Use `null` for formats without rows (zip, shapefile parts).
- Write the manifest after every completed file (status `partial`), then set `success` at the end. This enables resume.

### 5.4 Logging
- Use `get_logger(stage, source_code, batch_id)` from `src/utils/log.py`. Never use `print`.
- Format: `%(asctime)s | %(levelname)s | stage=%(stage)s | source=%(source)s | batch=%(batch_id)s | %(message)s`
- Log: start, each request (URL without secrets), row counts, retries, warnings, skipped files, finish with totals.

### 5.5 Errors
- Raise exceptions from `src/utils/exceptions.py` (`SourceRequestError`, `EmptyResponseError`, `ManifestMismatchError`, `LandingFileMissingError`, `ConfigError`). Include source, URL, and HTTP status in the message.
- Never swallow exceptions silently. A failed extract must fail the Airflow task.

### 5.6 Module shape
Every extractor in `src/extract/` exposes:
```python
def run(**overrides) -> Path:
    """Run the extraction. Returns the path to the batch manifest.json."""
```
and a CLI: `python -m src.extract.<module> [--options]` using `argparse`. Airflow calls `run()`.

### 5.7 Code style
- Python 3.11, type hints, docstrings on public functions, small functions.
- Dependencies go in `requirements.txt`. Do not add heavy libraries without noting it in the PR.

### 5.8 Tests
- `pytest -q` must pass. Unit tests must not hit the network; mock HTTP with the `responses` library.
- Live tests (real API calls) are marked `@pytest.mark.live` and only run when `RUN_LIVE_TESTS=1`.

## 6. Rules for AI agents

1. **Verify, don't guess.** Before coding against an API, make a small live request and inspect the actual response (status, headers, columns). Record what you observed in the PR. If the docs and reality disagree, trust reality and note it.
2. **Never invent endpoints, parameters, column names, or values.** If you cannot confirm something, stop and report it in the PR as a blocker.
3. **Never fabricate output.** Logs, row counts, and test results in the PR must be copied from real runs.
4. **Stay in scope.** Only create or modify the files listed in the ticket. If another file needs a change, explain why in the PR instead of silently changing it.
5. **Never commit** anything under `data/`, `.env`, credentials, tokens, or large generated files.
6. **Respect providers.** Throttle requests, honour HTTP 429 and `Retry-After`, and do not parallelize requests unless the ticket says so.
7. **If blocked** (API down, authentication needed, unexpected format), stop, write up exactly what happened in the PR, and propose options. Do not build an unrequested workaround.

## 7. Reproducibility rules (the professor must get identical results)

1. `PIPELINE_MODE=snapshot` (default) must never touch the network: `http.request()` raises `NetworkDisabledError`. Only `live` mode downloads.
2. Every extractor computes its batch_id from config only and checks for a complete batch **before** any network call.
3. Pin every version: Docker image tags, Python packages (with Airflow's constraints file). Never `latest`.
4. Deterministic outputs: sort rows before writing, use fixed seeds, break ties explicitly, never depend on dict/set iteration order or the current date (except `validate_period`).
5. Every command in the README must work from a fresh clone on Windows and Mac/Linux, with or without a `.env` file.
6. Line endings are LF (`.gitattributes`); never commit CRLF shell scripts.

## 8. Git and PR workflow

- Branch name: `ing-<n>-<short-name>` (e.g. `ing-3-wqp-results`).
- Commit messages start with the ticket ID: `ING-3: chunked result download with resume`.
- Open a PR per ticket. PR description must use this template:

```markdown
## Ticket
ING-n: <title>

## What changed
- files created/modified

## How to run
<exact commands>

## Evidence
- real log excerpt (start/finish lines, row counts)
- pytest output
- manifest.json excerpt

## Findings / deviations from the ticket
<API behaviour observed, decisions made, anything that differed from the ticket>

## Open questions / blockers
```
