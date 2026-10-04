"""VAL-2: staging-layer checks. Run after STG-2 (build_staging), before CUR-1 reads staging.

Reads observations only through pyarrow.dataset with hive partitioning (source_code=/year=),
so partition columns come from the folder names exactly as downstream readers see them.

Checks
    schema_matches            observations + sites columns and types == config/staging_schema.yaml  critical
    keys_not_null             columns declared nullable: false have no nulls                          critical
    obs_key_unique            no duplicate obs_key                                                     critical
    accepted_values           indicator_code, source_code, censor_direction, sampled sites' realm     critical
    value_range               value_cfu_100ml >= 0; values above 1,000,000 flagged                   warning
    date_in_period            sample_date within sampling.yaml › period                              critical
    coordinates_valid         sites: latitude in [-90, 90], longitude in [-180, 180]                  critical
    observation_site_exists   every observation site_key exists in sites.parquet                     critical
    stratum_coverage          sampled sites per stratum x realm >= sampling.min_sites_to_keep         warning

Run:  python -m src.validation.staging_checks [--staging-ref <batch id>] [--run-id <id>]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from src.utils.config import load_yaml
from src.utils.paths import staging_dir
from src.validation.core import (
    CheckResult, DataQualityError, format_table, make_result, raise_on_critical, write_report,
)

STAGE = "staging"
SOURCE = "all"
VALUE_FLAG_ABOVE = 1_000_000
ALLOWED_CENSOR = {"<", ">"}
ALLOWED_REALMS = {"freshwater", "marine"}


# ---------------------------------------------------------------- loading
def load_observations(root: Path) -> pa.Table:
    """All observation partitions (files starting with '_' or '.', e.g. _manifest.json, are ignored)."""
    if not root.exists() or not any(root.rglob("*.parquet")):
        raise FileNotFoundError(f"No staging observations under {root}; run STG-2 (build_staging) first")
    return ds.dataset(root, format="parquet", partitioning="hive").to_table()


def load_sites(path: Path) -> pa.Table:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}; run STG-1 (build_sampled_sites) first")
    return ds.dataset(path, format="parquet").to_table()


# ---------------------------------------------------------------- type matching
def _type_ok(declared: str, actual: pa.DataType) -> bool:
    if declared == "string":
        return pa.types.is_string(actual) or pa.types.is_large_string(actual) or pa.types.is_dictionary(actual)
    if declared in ("int16", "int32", "int64"):
        # integer width may differ: hive partition columns (year) read back as int32
        return pa.types.is_integer(actual)
    if declared == "float64":
        return pa.types.is_floating(actual)
    if declared == "bool":
        return pa.types.is_boolean(actual)
    if declared == "date32":
        return pa.types.is_date(actual)
    return False


# ---------------------------------------------------------------- checks
def check_schema(obs: pa.Table, sites: pa.Table, schema: dict, ref: str) -> CheckResult:
    problems = []
    for table_name, table in (("observations", obs), ("sites", sites)):
        declared = {c["name"]: c["dtype"] for c in schema["tables"][table_name]["columns"]}
        actual = {f.name: f.type for f in table.schema}
        for name, dtype in declared.items():
            if name not in actual:
                problems.append(f"{table_name}.{name} missing")
            elif not _type_ok(dtype, actual[name]):
                problems.append(f"{table_name}.{name} is {actual[name]}, expected {dtype}")
        extra = sorted(set(actual) - set(declared))
        if extra:
            problems.append(f"{table_name} has undeclared columns {extra}")
    return make_result("schema_matches", STAGE, SOURCE, ref, "critical", problems)


def check_not_null(obs: pd.DataFrame, sites: pd.DataFrame, schema: dict, ref: str) -> CheckResult:
    problems = []
    for table_name, df in (("observations", obs), ("sites", sites)):
        for c in schema["tables"][table_name]["columns"]:
            if not c["nullable"] and c["name"] in df.columns:
                n = int(df[c["name"]].isna().sum())
                if n:
                    problems.append(f"{table_name}.{c['name']}: {n} nulls")
    return make_result("keys_not_null", STAGE, SOURCE, ref, "critical", problems)


def check_unique(obs: pd.DataFrame, ref: str) -> CheckResult:
    dup = obs["obs_key"][obs["obs_key"].duplicated(keep=False)]
    problems = [f"{dup.nunique()} obs_key values appear more than once (e.g. {sorted(dup.unique())[:3]})"] if len(dup) else []
    return make_result("obs_key_unique", STAGE, SOURCE, ref, "critical", problems)


def check_accepted_values(obs: pd.DataFrame, sites: pd.DataFrame, mappings: dict, ref: str) -> CheckResult:
    indicators = {code for src in mappings["indicator_names"].values() for code in src.values()}
    sources = set(mappings["indicator_names"])
    problems = []

    def bad(series: pd.Series, allowed: set, label: str):
        values = series.dropna()
        wrong = values[~values.isin(allowed)]
        if len(wrong):
            problems.append(f"{label}: {len(wrong)} rows with {sorted(wrong.astype(str).unique())[:5]}")

    bad(obs["indicator_code"], indicators, "indicator_code")
    bad(obs["source_code"], sources, "source_code")
    bad(obs["censor_direction"], ALLOWED_CENSOR, "censor_direction")
    sampled = sites[sites["is_sampled"].astype(bool)]
    bad(sampled["realm"], ALLOWED_REALMS, "sampled sites realm")
    n_null_realm = int(sampled["realm"].isna().sum())
    if n_null_realm:
        problems.append(f"sampled sites realm: {n_null_realm} sampled sites have no realm")
    return make_result("accepted_values", STAGE, SOURCE, ref, "critical", problems)


def check_value_range(obs: pd.DataFrame, ref: str) -> CheckResult:
    v = obs["value_cfu_100ml"]
    problems = []
    if (v < 0).any():
        problems.append(f"{int((v < 0).sum())} negative values")
    if (v > VALUE_FLAG_ABOVE).any():
        problems.append(f"{int((v > VALUE_FLAG_ABOVE).sum())} values above {VALUE_FLAG_ABOVE:,} (max {v.max():,.0f})")
    return make_result("value_range", STAGE, SOURCE, ref, "warning", problems)


def check_dates(obs: pd.DataFrame, period: dict, ref: str) -> CheckResult:
    start, end = date(int(period["start_year"]), 1, 1), date(int(period["end_year"]), 12, 31)
    d = pd.to_datetime(obs["sample_date"], errors="coerce").dt.date
    outside = obs[(d < start) | (d > end)]
    problems = []
    if len(outside):
        problems.append(f"{len(outside)} rows outside {start}..{end} (e.g. {sorted(outside['sample_date'].astype(str).unique())[:3]})")
    years = pd.to_datetime(obs["sample_date"], errors="coerce").dt.year
    mismatch = int((years != obs["year"].astype("Int64")).sum())
    if mismatch:
        problems.append(f"{mismatch} rows whose year partition differs from sample_date")
    return make_result("date_in_period", STAGE, SOURCE, ref, "critical", problems)


def check_coordinates(sites: pd.DataFrame, ref: str) -> CheckResult:
    lat, lon = sites["latitude"], sites["longitude"]
    bad = sites[~lat.between(-90, 90) | ~lon.between(-180, 180)]
    problems = [f"{len(bad)} sites with invalid coordinates (e.g. {bad['site_key'].head(3).tolist()})"] if len(bad) else []
    return make_result("coordinates_valid", STAGE, SOURCE, ref, "critical", problems)


def check_sites_exist(obs: pd.DataFrame, sites: pd.DataFrame, ref: str) -> CheckResult:
    orphans = sorted(set(obs["site_key"].dropna()) - set(sites["site_key"]))
    problems = [f"{len(orphans)} site_key values not in sites.parquet (e.g. {orphans[:3]})"] if orphans else []
    return make_result("observation_site_exists", STAGE, SOURCE, ref, "critical", problems)


def check_stratum_coverage(sites: pd.DataFrame, sampling: dict, ref: str) -> CheckResult:
    minimum = int(sampling["sampling"]["min_sites_to_keep"])
    sampled = sites[sites["is_sampled"].astype(bool)]
    counts = sampled.groupby(["stratum", "realm"]).size()
    problems = []
    for stratum in sampling["strata"]:
        for realm in sampling["realms"]:
            n = int(counts.get((stratum, realm), 0))
            if n < minimum:
                problems.append(f"{stratum}/{realm}: {n} sampled sites (< {minimum})")
    return make_result("stratum_coverage", STAGE, SOURCE, ref, "warning", problems)


# ---------------------------------------------------------------- entry points
def run_checks(staging_ref: str = "manual") -> list[CheckResult]:
    schema, mappings, sampling = load_yaml("staging_schema"), load_yaml("mappings"), load_yaml("sampling")
    root = staging_dir()
    try:
        obs_t = load_observations(root / "observations")
        sites_t = load_sites(root / "sites" / "sites.parquet")
    except FileNotFoundError as exc:
        return [make_result("staging_readable", STAGE, SOURCE, staging_ref, "critical", [str(exc)])]
    obs, sites = obs_t.to_pandas(), sites_t.to_pandas()
    return [
        check_schema(obs_t, sites_t, schema, staging_ref),
        check_not_null(obs, sites, schema, staging_ref),
        check_unique(obs, staging_ref),
        check_accepted_values(obs, sites, mappings, staging_ref),
        check_value_range(obs, staging_ref),
        check_dates(obs, sampling["period"], staging_ref),
        check_coordinates(sites, staging_ref),
        check_sites_exist(obs, sites, staging_ref),
        check_stratum_coverage(sites, sampling, staging_ref),
    ]


def staging_batch_id() -> str:
    """batch id from STG-2's manifest, if present (used when no staging_ref is given)."""
    m = staging_dir() / "observations" / "_manifest.json"
    if m.exists():
        return json.loads(m.read_text(encoding="utf-8")).get("batch_id", "manual")
    return "manual"


def validate_staging(staging_ref: str, run_id: str = "manual") -> None:
    """Run all staging checks, write the report, raise DataQualityError on any critical failure. ING-6 calls this."""
    results = run_checks(staging_ref)
    write_report(results, STAGE, run_id)
    raise_on_critical(results)


def main() -> None:
    parser = argparse.ArgumentParser(description="VAL-2: staging-layer data-quality checks")
    parser.add_argument("--staging-ref", default=None, help="staging batch id (default: from STG-2's manifest)")
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args()
    ref = args.staging_ref or staging_batch_id()
    results = run_checks(ref)
    print(format_table(results))
    print(f"\nReport: {write_report(results, STAGE, args.run_id)}")
    try:
        raise_on_critical(results)
    except DataQualityError as exc:
        print(f"\nFAILED: {exc}")
        sys.exit(1)
    print("\nAll critical checks passed.")


if __name__ == "__main__":
    main()
