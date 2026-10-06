"""VAL-3: curated checks and raw -> staging -> curated reconciliation. Runs after LOAD-1, against Postgres.

Checks
    tables_present           all 11 warehouse tables exist                                       critical
    referential_integrity    every foreign key in the database has 0 orphan rows (read from the
                             catalog, so new FKs are covered automatically)                        critical
    pk_unique_in_db          no duplicate natural-key values in any table (keys from the ERD)     critical
    reconciliation           per source: raw rows - staging drops = staging rows, and
                             staging rows = fact_observation rows                                  critical
    weather_coverage         share of observations with rain_48h_mm >= 90%                        warning
    mart_rank_consistency    ranked regions: priority_rank is 1..n with no gaps; rank_in_country
                             is 1..n per country; unranked regions have no ranks                   critical
    hotspot_share_sane       share of persistent hotspots is between 0% and 100% (exclusive)      warning

Also writes outputs/reconciliation.csv (one row per source; used in the report and live demo).

Run:  python -m src.validation.curated_checks [--curated-ref <batch id>] [--run-id <id>]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.utils.paths import data_dir, staging_dir
from src.validation.core import (
    CheckResult, DataQualityError, format_table, make_result, outputs_dir, raise_on_critical, write_report,
)
from src.validation.raw_checks import _data_files, _schema

STAGE = "curated"
SOURCE = "all"
WEATHER_COVERAGE_MIN = 0.90
TABLES = ["etl_batch_log", "dim_source", "dim_indicator", "dim_region", "dim_grid_cell", "dim_site",
          "fact_weather_daily", "fact_observation", "mart_site_hotspot", "mart_region_priority", "dq_results"]
RAW_SOURCE_FOR = {"wqp": "wqp_results", "owq_gemstat": "owq_gemstat", "owq_eionet": "owq_eionet"}
# Natural keys from docs/erd.md. Checked directly, so duplicates are caught even if a PK constraint were missing.
EXPECTED_KEYS = {
    "etl_batch_log": ["batch_id"], "dim_source": ["source_code"], "dim_indicator": ["indicator_code"],
    "dim_region": ["region_code"], "dim_grid_cell": ["cell_id"], "dim_site": ["site_key"],
    "fact_weather_daily": ["cell_id", "weather_date"], "fact_observation": ["obs_key"],
    "mart_site_hotspot": ["site_key", "indicator_code"], "mart_region_priority": ["region_code"],
    "dq_results": ["dq_result_id"],
}


# ---------------------------------------------------------------- pure functions (unit-tested without a DB)
def reconcile(raw_rows: dict[str, int], drops: pd.DataFrame, staging_rows: dict[str, int],
              curated_rows: dict[str, int]) -> tuple[pd.DataFrame, list[str]]:
    """Build the reconciliation table and list every mismatch.

    drops: staging drop log rows for stage "observations" (source_code, reason, row_count).
    """
    sources = sorted(set(raw_rows) | set(staging_rows) | set(curated_rows))
    reasons = sorted(drops["reason"].unique()) if len(drops) else []
    rows, problems = [], []
    for s in sources:
        d = drops[drops["source_code"] == s]
        by_reason = {f"dropped_{r}": int(d.loc[d["reason"] == r, "row_count"].sum()) for r in reasons}
        dropped = int(d["row_count"].sum())
        raw, stg, cur = raw_rows.get(s, 0), staging_rows.get(s, 0), curated_rows.get(s, 0)
        if raw - dropped != stg:
            problems.append(f"{s}: raw {raw} - dropped {dropped} = {raw - dropped}, but staging has {stg}")
        if stg != cur:
            problems.append(f"{s}: staging has {stg} rows, fact_observation has {cur}")
        rows.append({"source_code": s, "raw_rows": raw, **by_reason, "dropped_total": dropped,
                     "staging_rows": stg, "curated_rows": cur,
                     "raw_to_staging_ok": raw - dropped == stg, "staging_to_curated_ok": stg == cur})
    return pd.DataFrame(rows), problems


def rank_problems(regions: pd.DataFrame) -> list[str]:
    """mart_region_priority rows: status, priority_rank, rank_in_country, country_iso."""
    problems = []
    ranked = regions[regions["status"] == "ranked"]
    unranked = regions[regions["status"] != "ranked"]
    ranks = sorted(ranked["priority_rank"].dropna().astype(int))
    if ranked["priority_rank"].isna().any():
        problems.append(f"{int(ranked['priority_rank'].isna().sum())} ranked regions have no priority_rank")
    if ranks != list(range(1, len(ranks) + 1)):
        problems.append(f"priority_rank is not 1..{len(ranks)} without gaps or repeats")
    for country, grp in ranked.groupby("country_iso", dropna=False):
        r = sorted(grp["rank_in_country"].dropna().astype(int))
        if r != list(range(1, len(grp) + 1)):
            problems.append(f"rank_in_country for {country} is not 1..{len(grp)}")
    if unranked[["priority_rank", "rank_in_country"]].notna().any(axis=None):
        problems.append("some unranked regions have a rank")
    return problems


def hotspot_share_problems(n_sites: int, n_hotspots: int) -> list[str]:
    if n_sites == 0:
        return ["mart_site_hotspot is empty"]
    share = n_hotspots / n_sites
    if share == 0:
        return ["no persistent hotspots at all (0%): check rules and data"]
    if share == 1:
        return ["every site is a persistent hotspot (100%): check rules and data"]
    return []


# ---------------------------------------------------------------- inputs from files
def raw_rows_by_source(staging_manifest: dict) -> dict[str, int]:
    """Rows in the data files of the raw batches STG-2 actually read (STG-2 manifest › params.inputs)."""
    out = {}
    raw_root = data_dir() / "raw"
    for source, batch_id in staging_manifest.get("params", {}).get("inputs", {}).items():
        raw_code = RAW_SOURCE_FOR.get(source, source)
        manifests = [p for p in (raw_root / raw_code).rglob("manifest.json")
                     if json.loads(p.read_text(encoding="utf-8")).get("batch_id") == batch_id]
        if not manifests:
            out[source] = 0
            continue
        m = json.loads(manifests[0].read_text(encoding="utf-8"))
        data = set(_data_files(m, _schema(raw_code)))
        out[source] = sum(f.get("row_count") or 0 for f in m.get("files", []) if f["name"] in data)
    return out


def staging_inputs() -> tuple[dict, pd.DataFrame, dict[str, int]]:
    """(STG-2 manifest, observation drop-log rows, staging row counts by source)."""
    root = staging_dir()
    mpath = root / "observations" / "_manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8")) if mpath.exists() else {}
    log_path = root / "_drop_log.parquet"
    drops = pd.read_parquet(log_path) if log_path.exists() else pd.DataFrame(
        columns=["stage", "source_code", "reason", "row_count", "batch_id"])
    drops = drops[drops["stage"] == "observations"]
    counts = {p["source_code"]: 0 for p in manifest.get("partitions", [])}
    for p in manifest.get("partitions", []):
        counts[p["source_code"]] += int(p["rows"])
    return manifest, drops, counts


# ---------------------------------------------------------------- database checks
def _tables_present(conn) -> list[str]:
    found = set(conn.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()")).scalars())
    return [t for t in TABLES if t not in found]


def check_referential_integrity(conn, ref: str) -> CheckResult:
    fks = conn.execute(text("""
        SELECT tc.table_name, kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column,
               tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = current_schema()
        ORDER BY tc.constraint_name""")).fetchall()
    problems = []
    for t, c, rt, rc, name in fks:
        n = conn.execute(text(
            f'SELECT count(*) FROM "{t}" x LEFT JOIN "{rt}" r ON x."{c}" = r."{rc}" '
            f'WHERE x."{c}" IS NOT NULL AND r."{rc}" IS NULL')).scalar()
        if n:
            problems.append(f"{name}: {n} rows in {t}.{c} without a match in {rt}.{rc}")
    result = make_result("referential_integrity", STAGE, SOURCE, ref, "critical", problems)
    if not problems:
        result.details = f"{len(fks)} foreign keys checked, 0 orphans"
    return result


def check_pk_unique(conn, ref: str) -> CheckResult:
    problems = []
    for table, cols in EXPECTED_KEYS.items():
        quoted = ", ".join(f'"{c}"' for c in cols)
        n = conn.execute(text(
            f'SELECT count(*) FROM (SELECT {quoted} FROM "{table}" GROUP BY {quoted} HAVING count(*) > 1) d')).scalar()
        if n:
            problems.append(f"{table}: {n} duplicated key values ({', '.join(cols)})")
    return make_result("pk_unique_in_db", STAGE, SOURCE, ref, "critical", problems)


def check_weather_coverage(conn, ref: str) -> CheckResult:
    rows = conn.execute(text("""
        SELECT s.source_code, count(*) AS n, count(o.rain_48h_mm) AS with_rain
        FROM fact_observation o JOIN dim_site s ON s.site_key = o.site_key
        GROUP BY s.source_code ORDER BY s.source_code""")).fetchall()
    total = sum(r.n for r in rows)
    covered = sum(r.with_rain for r in rows)
    share = covered / total if total else 0.0
    per_source = ", ".join(f"{r.source_code} {r.with_rain / r.n:.0%}" for r in rows if r.n)
    problems = [] if share >= WEATHER_COVERAGE_MIN else [
        f"{share:.1%} of observations have rain_48h_mm (minimum {WEATHER_COVERAGE_MIN:.0%}); by source: {per_source}"]
    result = make_result("weather_coverage", STAGE, SOURCE, ref, "warning", problems)
    if not problems:
        result.details = f"{share:.1%} covered ({per_source})"
    return result


def run_checks(curated_ref: str = "manual", engine: Engine | None = None) -> tuple[list[CheckResult], pd.DataFrame]:
    """All curated checks. Returns (results, reconciliation table)."""
    if engine is None:
        from src.load.db import get_engine
        engine = get_engine()
    with engine.connect() as conn:
        missing = _tables_present(conn)
        if missing:
            return [make_result("tables_present", STAGE, SOURCE, curated_ref, "critical",
                                [f"missing tables {missing}; run DB-1 (init_db)"])], pd.DataFrame()
        results = [make_result("tables_present", STAGE, SOURCE, curated_ref, "critical", []),
                   check_referential_integrity(conn, curated_ref),
                   check_pk_unique(conn, curated_ref)]

        manifest, drops, staging_rows = staging_inputs()
        # counted by the observation's own key prefix (not via dim_site), so orphan rows still count
        curated_rows = {r.source_code: r.n for r in conn.execute(text(
            "SELECT split_part(obs_key, ':', 1) AS source_code, count(*) AS n "
            "FROM fact_observation GROUP BY 1")).fetchall()}
        table, problems = reconcile(raw_rows_by_source(manifest), drops, staging_rows, curated_rows)
        if not manifest:
            problems.insert(0, "no STG-2 manifest (data/staging/observations/_manifest.json)")
        results.append(make_result("reconciliation", STAGE, SOURCE, curated_ref, "critical", problems))

        results.append(check_weather_coverage(conn, curated_ref))

        regions = pd.read_sql(text("SELECT status, priority_rank, rank_in_country, country_iso "
                                   "FROM mart_region_priority"), conn)
        results.append(make_result("mart_rank_consistency", STAGE, SOURCE, curated_ref, "critical",
                                   rank_problems(regions)))
        n_sites, n_hot = conn.execute(text(
            "SELECT count(*), count(*) FILTER (WHERE is_persistent_hotspot) FROM mart_site_hotspot")).one()
        results.append(make_result("hotspot_share_sane", STAGE, SOURCE, curated_ref, "warning",
                                   hotspot_share_problems(n_sites, n_hot)))
    return results, table


def write_reconciliation(table: pd.DataFrame) -> Path:
    path = outputs_dir() / "reconciliation.csv"
    table.to_csv(path, index=False)
    return path


def curated_batch_id() -> str:
    m = data_dir() / "curated" / "_manifest.json"
    return json.loads(m.read_text(encoding="utf-8")).get("batch_id", "manual") if m.exists() else "manual"


def validate_curated(curated_ref: str, run_id: str = "manual", engine: Engine | None = None) -> None:
    """Run all curated checks, write the JSON report and reconciliation.csv, raise on critical failure.
    ING-6 calls this after LOAD-1."""
    results, table = run_checks(curated_ref, engine)
    if len(table):
        write_reconciliation(table)
    write_report(results, STAGE, run_id)
    raise_on_critical(results)


def main() -> None:
    parser = argparse.ArgumentParser(description="VAL-3: curated checks + reconciliation (needs Postgres)")
    parser.add_argument("--curated-ref", default=None, help="curated batch id (default: from CUR-1's manifest)")
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args()
    ref = args.curated_ref or curated_batch_id()
    results, table = run_checks(ref)
    print(format_table(results))
    if len(table):
        print("\nReconciliation:\n" + table.to_string(index=False))
        print(f"\nWritten: {write_reconciliation(table)}")
    print(f"Report: {write_report(results, STAGE, args.run_id)}")
    try:
        raise_on_critical(results)
    except DataQualityError as exc:
        print(f"\nFAILED: {exc}")
        sys.exit(1)
    print("\nAll critical checks passed.")


if __name__ == "__main__":
    main()
