### Curated stage (VAL-3, `src/validation/curated_checks.py`)

Runs after LOAD-1, against PostgreSQL. It checks the warehouse itself, and reconciles row counts across all three layers.

| Check | What it tests | Severity |
|---|---|---|
| `tables_present` | All 11 warehouse tables exist | Critical |
| `referential_integrity` | Every foreign key in the database has 0 orphan rows (foreign keys are read from the database catalog, so new ones are covered automatically) | Critical |
| `pk_unique_in_db` | No duplicate natural-key values in any table (keys from `docs/erd.md`, so duplicates are caught even if a constraint were missing) | Critical |
| `reconciliation` | Per source: raw rows − staging drops = staging rows, and staging rows = `fact_observation` rows | Critical |
| `weather_coverage` | At least 90% of observations have antecedent rainfall (per-source shares in the details) | Warning |
| `mart_rank_consistency` | Ranked regions are numbered 1..n with no gaps; `rank_in_country` is 1..n per country; unranked regions have no rank | Critical |
| `hotspot_share_sane` | The share of persistent hotspots is above 0% and below 100% | Warning |

The run also writes **`outputs/reconciliation.csv`**: one row per source with raw rows, rows dropped per reason, staging rows and curated rows. It shows exactly where every raw row went, and is used in the report and live demo.

Run manually (needs Postgres, e.g. inside Docker):
```
python -m src.validation.curated_checks
```
