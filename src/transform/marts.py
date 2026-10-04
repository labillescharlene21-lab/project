"""MART-1: persistent hotspot sites and regional rehabilitation priority.

Inputs (curated layer, written by CUR-1, columns as in the ERD):
    data/curated/fact_observation.parquet   obs_key, site_key, indicator_code, sample_date, value_cfu_100ml, is_wet
    data/curated/dim_site.parquet           site_key, realm, region_code
    data/curated/dim_indicator.parquet      indicator_code, realm, threshold_cfu_100ml, is_scored
    data/curated/dim_region.parquet         region_code, admin1_name, country_iso, country_name

Outputs:
    data/curated/mart_site_hotspot.parquet
    data/curated/mart_region_priority.parquet
    outputs/priority_ranking.csv

Every threshold, window and weight comes from config/business_rules.yaml (plus the
realm thresholds in dim_indicator). No rule value is written in this file.

Steps
1. Sample-days: replicates of the same (site, indicator, date) are collapsed into one value.
   Collapse = shifted geometric mean, exp(mean(log(v + 1))) - 1, applied to every group so zeros
   are allowed and all groups are treated the same way. A single replicate keeps its exact value.
2. Site-years: n_days, exceed_days, exceedance_rate; "insufficient" if too few sample-days,
   else "poor" (rate > poor_year_exceedance_rate) or "good".
3. Sites: persistence over the most recent classified years, severity, trend, wet vs dry.
4. Regions: component scores, min-max normalized across ranked regions, weighted sum, ranks.

Run:  python -m src.transform.marts [--curated-ref <id>]
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import theilslopes

from src.utils.config import load_yaml
from src.utils.exceptions import ConfigError
from src.utils.log import get_logger
from src.utils.manifest import make_batch_id
from src.utils.paths import curated_dir, data_dir

STAGE = "marts"
COMPONENTS = ["persistence", "severity", "trend", "rain_sensitivity"]

INPUT_CONTRACT = {
    "fact_observation": ["obs_key", "site_key", "indicator_code", "sample_date", "value_cfu_100ml", "is_wet"],
    "dim_site": ["site_key", "realm", "region_code"],
    "dim_indicator": ["indicator_code", "realm", "threshold_cfu_100ml", "is_scored"],
    "dim_region": ["region_code", "admin1_name", "country_iso", "country_name"],
}

SITE_COLUMNS = [
    "site_key", "indicator_code", "region_code", "realm",
    "n_observations", "n_samples", "years_monitored", "insufficient_years", "poor_years",
    "is_persistent_hotspot", "exceedance_rate", "median_ratio_to_threshold", "trend_slope",
    "wet_exceedance_rate", "dry_exceedance_rate", "batch_id",
]
REGION_COLUMNS = [
    "region_code", "admin1_name", "country_iso", "country_name", "status",
    "n_sites", "n_hotspots",
    "persistence_raw", "severity_raw", "trend_raw", "rain_sensitivity_raw",
    "persistence_score", "severity_score", "trend_score", "rain_sensitivity_score",
    "priority_score", "priority_rank", "rank_in_country", "notes", "batch_id",
]


# ------------------------------------------------------------------ config
def load_rules() -> dict:
    """Load business_rules.yaml and check the values the marts rely on."""
    rules = load_yaml("business_rules")
    try:
        weights = rules["priority"]["weights"]
        needed = [
            rules["exceedance"]["poor_year_exceedance_rate"],
            rules["exceedance"]["min_sample_days_per_year"],
            rules["persistence"]["window_years"],
            rules["persistence"]["min_poor_years"],
            rules["trend"]["min_years"],
            rules["priority"]["min_sites_per_region"],
        ]
    except KeyError as exc:
        raise ConfigError(f"business_rules.yaml is missing key {exc}") from exc
    if set(weights) != set(COMPONENTS):
        raise ConfigError(f"priority.weights must have exactly {COMPONENTS}, got {sorted(weights)}")
    total = sum(float(w) for w in weights.values())
    if abs(total - 1.0) > 1e-9:
        raise ConfigError(f"priority.weights must sum to 1.0, got {total}")
    if any(v is None for v in needed):
        raise ConfigError("business_rules.yaml has an empty required value")
    if rules.get("daily_collapse", "geometric_mean") != "geometric_mean":
        raise ConfigError("daily_collapse: only 'geometric_mean' is implemented")
    return rules


# ------------------------------------------------------------------ input check
def check_inputs(tables: dict[str, pd.DataFrame]) -> None:
    """Fail early, with one clear message, if the curated tables do not match the contract."""
    problems = []
    for name, cols in INPUT_CONTRACT.items():
        if name not in tables:
            problems.append(f"{name}: table missing")
            continue
        missing = [c for c in cols if c not in tables[name].columns]
        if missing:
            problems.append(f"{name}: missing columns {missing}")
    if problems:
        raise ConfigError("Curated input does not match the MART-1 contract: " + "; ".join(problems))
    fo = tables["fact_observation"]
    if not pd.api.types.is_numeric_dtype(fo["value_cfu_100ml"]):
        raise ConfigError("fact_observation.value_cfu_100ml must be numeric")
    if pd.to_datetime(fo["sample_date"], errors="coerce").isna().any():
        raise ConfigError("fact_observation.sample_date has values that are not valid dates")


# ------------------------------------------------------------------ step 1: sample-days
def shifted_geomean(values: pd.Series) -> float:
    return float(np.expm1(np.log1p(values.astype(float)).mean()))


def sample_days(obs: pd.DataFrame, sites: pd.DataFrame, indicators: pd.DataFrame) -> pd.DataFrame:
    """One row per (site, primary indicator, date) with the collapsed value and flags."""
    scored = indicators[indicators["is_scored"].astype(bool)][["indicator_code", "realm", "threshold_cfu_100ml"]]
    # Each site is scored only on its realm's primary indicator.
    primary = sites[["site_key", "realm", "region_code"]].merge(scored, on="realm", how="inner")
    df = obs.merge(primary[["site_key", "indicator_code", "threshold_cfu_100ml"]],
                   on=["site_key", "indicator_code"], how="inner")
    df = df[df["value_cfu_100ml"].notna()].copy()
    df["sample_date"] = pd.to_datetime(df["sample_date"]).dt.normalize()

    def wet_flag(s: pd.Series):
        s = s.dropna()
        return pd.NA if s.empty else bool(s.astype(bool).any())

    days = (
        df.groupby(["site_key", "indicator_code", "sample_date"], as_index=False)
        .agg(value=("value_cfu_100ml", shifted_geomean),
             n_replicates=("obs_key", "count"),
             is_wet=("is_wet", wet_flag),
             threshold=("threshold_cfu_100ml", "first"))
    )
    days["year"] = days["sample_date"].dt.year
    days["exceeds"] = days["value"] > days["threshold"]
    days["ratio"] = days["value"] / days["threshold"]
    return days.sort_values(["site_key", "indicator_code", "sample_date"]).reset_index(drop=True)


# ------------------------------------------------------------------ step 2: site-years
def site_years(days: pd.DataFrame, rules: dict) -> pd.DataFrame:
    ex = rules["exceedance"]
    sy = (
        days.groupby(["site_key", "indicator_code", "year"], as_index=False)
        .agg(n_days=("exceeds", "size"), exceed_days=("exceeds", "sum"))
    )
    sy["exceedance_rate"] = sy["exceed_days"] / sy["n_days"]
    sy["year_class"] = np.where(
        sy["n_days"] < ex["min_sample_days_per_year"], "insufficient",
        np.where(sy["exceedance_rate"] > ex["poor_year_exceedance_rate"], "poor", "good"),
    )
    return sy.sort_values(["site_key", "indicator_code", "year"]).reset_index(drop=True)


# ------------------------------------------------------------------ step 3: sites
def _rate(flags: pd.Series) -> float:
    return float(flags.mean()) if len(flags) else np.nan


def site_hotspots(days: pd.DataFrame, sy: pd.DataFrame, obs_counts: pd.Series, rules: dict) -> pd.DataFrame:
    window = int(rules["persistence"]["window_years"])
    min_poor = int(rules["persistence"]["min_poor_years"])
    min_trend_years = int(rules["trend"]["min_years"])
    rows = []
    for (site, ind), g in days.groupby(["site_key", "indicator_code"], sort=True):
        years = sy[(sy["site_key"] == site) & (sy["indicator_code"] == ind)]
        classified = years[years["year_class"] != "insufficient"].sort_values("year")
        recent = classified.tail(window)
        poor_years = int((recent["year_class"] == "poor").sum())

        slope = np.nan
        if len(classified) >= min_trend_years and classified["year"].nunique() >= 2:
            slope = float(theilslopes(classified["exceedance_rate"].to_numpy(), classified["year"].to_numpy())[0])

        wet = g[g["is_wet"].eq(True).fillna(False)]["exceeds"]
        dry = g[g["is_wet"].eq(False).fillna(False)]["exceeds"]
        rows.append({
            "site_key": site,
            "indicator_code": ind,
            "n_observations": int(obs_counts.get((site, ind), 0)),
            "n_samples": int(len(g)),
            "years_monitored": int(len(classified)),
            "insufficient_years": int(len(years) - len(classified)),
            "poor_years": poor_years,
            "is_persistent_hotspot": poor_years >= min_poor,
            "exceedance_rate": _rate(g["exceeds"]),
            "median_ratio_to_threshold": float(g["ratio"].median()),
            "trend_slope": slope,
            "wet_exceedance_rate": _rate(wet),
            "dry_exceedance_rate": _rate(dry),
        })
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ step 4: regions
def _minmax(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    if pd.isna(lo) or hi == lo:
        return pd.Series(0.0, index=s.index)
    return (s - lo) / (hi - lo)


def region_priority(site_mart: pd.DataFrame, regions: pd.DataFrame, rules: dict) -> pd.DataFrame:
    pr = rules["priority"]
    weights = {k: float(v) for k, v in pr["weights"].items()}
    judged = site_mart[(site_mart["years_monitored"] > 0) & site_mart["region_code"].notna()].copy()
    judged["rain_ratio"] = np.where(
        judged["wet_exceedance_rate"].notna() & (judged["dry_exceedance_rate"] > 0),
        judged["wet_exceedance_rate"] / judged["dry_exceedance_rate"], np.nan,
    )
    agg = (
        judged.groupby("region_code")
        .agg(n_sites=("site_key", "count"),
             n_hotspots=("is_persistent_hotspot", "sum"),
             severity_raw=("median_ratio_to_threshold", "median"),
             trend_raw=("trend_slope", "median"),
             rain_sensitivity_raw=("rain_ratio", "median"))
        .reset_index()
    )
    agg["n_hotspots"] = agg["n_hotspots"].astype(int)
    agg["persistence_raw"] = agg["n_hotspots"] / agg["n_sites"]
    agg = agg.merge(regions[["region_code", "admin1_name", "country_iso", "country_name"]],
                    on="region_code", how="left")

    ranked = agg["n_sites"] >= int(pr["min_sites_per_region"])
    agg["status"] = np.where(ranked, "ranked", "insufficient_sites")
    agg["notes"] = ""
    for comp in COMPONENTS:
        raw = f"{comp}_raw"
        score = pd.Series(np.nan, index=agg.index)
        score[ranked] = _minmax(agg.loc[ranked, raw])
        missing = ranked & agg[raw].isna()
        score[missing] = 0.0
        agg.loc[missing, "notes"] = agg.loc[missing, "notes"] + f"{comp} missing (scored 0); "
        agg[f"{comp}_score"] = score

    agg["priority_score"] = np.nan
    agg.loc[ranked, "priority_score"] = sum(weights[c] * agg.loc[ranked, f"{c}_score"] for c in COMPONENTS)

    order = agg[ranked].sort_values(["priority_score", "n_hotspots", "region_code"],
                                    ascending=[False, False, True])
    agg["priority_rank"] = pd.Series(pd.NA, index=agg.index, dtype="Int64")
    agg.loc[order.index, "priority_rank"] = range(1, len(order) + 1)
    agg["rank_in_country"] = pd.Series(pd.NA, index=agg.index, dtype="Int64")
    for _, grp in order.groupby("country_iso", sort=False, dropna=False):
        agg.loc[grp.index, "rank_in_country"] = range(1, len(grp) + 1)
    agg["notes"] = agg["notes"].str.strip()
    return agg.sort_values(["priority_rank", "region_code"], na_position="last").reset_index(drop=True)


# ------------------------------------------------------------------ orchestration
def compute_marts(tables: dict[str, pd.DataFrame], rules: dict, batch_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pure function: curated tables in, the two marts out. Used by build_marts and by tests."""
    check_inputs(tables)
    obs, sites = tables["fact_observation"], tables["dim_site"]
    days = sample_days(obs, sites, tables["dim_indicator"])
    sy = site_years(days, rules)
    obs_counts = obs.groupby(["site_key", "indicator_code"]).size()
    site_mart = site_hotspots(days, sy, obs_counts, rules)
    if site_mart.empty:
        raise ConfigError("No scored observations matched any site's primary indicator; check CUR-1 output")
    site_mart = site_mart.merge(sites[["site_key", "region_code", "realm"]], on="site_key", how="left")
    region_mart = region_priority(site_mart, tables["dim_region"], rules)
    site_mart["batch_id"] = batch_id
    region_mart["batch_id"] = batch_id
    site_mart = site_mart[SITE_COLUMNS].sort_values(["site_key", "indicator_code"]).reset_index(drop=True)
    return site_mart, region_mart[REGION_COLUMNS]


def outputs_dir() -> Path:
    """OUTPUT_DIR if set, else the folder next to the data folder (repo root / outputs)."""
    value = os.environ.get("OUTPUT_DIR")
    path = Path(value) if value else data_dir().resolve().parent / "outputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)          # overwrite atomically: reruns never leave half-written files


def build_marts(curated_ref: str) -> None:
    """Read curated tables, compute both marts, write Parquet + priority_ranking.csv. ING-6 calls this."""
    rules = load_rules()
    batch_id = make_batch_id(STAGE, {"curated_ref": curated_ref, "rules": rules})
    log = get_logger(STAGE, "curated", batch_id)
    cdir = curated_dir()
    tables = {}
    for name in INPUT_CONTRACT:
        path = cdir / f"{name}.parquet"
        if not path.exists():
            raise ConfigError(f"Missing curated input {path}; run CUR-1 (build_curated) first")
        tables[name] = pd.read_parquet(path)
    log.info("inputs: " + ", ".join(f"{k}={len(v)} rows" for k, v in tables.items()))

    site_mart, region_mart = compute_marts(tables, rules, batch_id)
    _write_parquet(site_mart, cdir / "mart_site_hotspot.parquet")
    _write_parquet(region_mart, cdir / "mart_region_priority.parquet")

    ranked = region_mart[region_mart["status"] == "ranked"]
    ranked[["priority_rank", "region_code", "admin1_name", "country_name", "country_iso", "n_sites",
            "n_hotspots", "priority_score", "rank_in_country"]].to_csv(
        outputs_dir() / "priority_ranking.csv", index=False)

    n_hot = int(site_mart["is_persistent_hotspot"].sum())
    log.info(f"sites scored: {len(site_mart)}, persistent hotspots: {n_hot} "
             f"({n_hot / len(site_mart):.1%}); regions: {len(region_mart)}, ranked: {len(ranked)}, "
             f"insufficient_sites: {int((region_mart['status'] == 'insufficient_sites').sum())}")


def main() -> None:
    parser = argparse.ArgumentParser(description="MART-1: hotspot and priority marts")
    parser.add_argument("--curated-ref", default="manual", help="curated batch id (from CUR-1)")
    build_marts(parser.parse_args().curated_ref)


if __name__ == "__main__":
    main()
