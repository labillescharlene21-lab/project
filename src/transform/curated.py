"""CUR-1: curated layer. Staging + weather + boundaries -> dimension and fact tables.

Inputs
    data/staging/observations/source_code=*/year=*/*.parquet   (STG-2, staging_schema.yaml › observations)
    data/staging/sites/sites.parquet                           (STG-1, staging_schema.yaml › sites)
    data/raw/open_meteo/<batch>/{grid_cells.csv, site_cells.csv, cell_*.json}   (ING-4, all batches)
    data/raw/natural_earth/<batch>/extracted/*.shp             (ING-5, attribute table only)
    config/sampling.yaml (realms, thresholds), config/business_rules.yaml (wet threshold),
    config/source_catalog.yaml (descriptive names)

Outputs (data/curated/, one Parquet per table, columns as in docs/erd.md; LOAD-1 adds load_batch_id)
    dim_source, dim_indicator, dim_region, dim_grid_cell, dim_site, fact_weather_daily, fact_observation
    _manifest.json  (batch id, inputs, row counts, weather coverage)

Rules
    - Only sampled sites (sites.is_sampled) enter the curated layer; their observations only.
    - exceeds_threshold = value_cfu_100ml > the indicator's threshold (scored indicators only; null otherwise).
      MART-1 then scores each site on its realm's primary indicator.
    - Antecedent weather from the site's grid cell (ING-4 site_cells.csv):
        rain_48h_mm = precipitation on the sample day + the previous day
        rain_72h_mm = rain_48h_mm + the day before that
        temp_mean_c = mean temperature on the sample day
      A window with any missing day -> null (never a partial sum). Assumption: the sample date is a local
      date and weather days are UTC; daily totals may include rain that fell after the sample was taken.
    - is_wet = rain_48h_mm >= business_rules.rainfall.wet_threshold_mm_48h; null if rain_48h_mm is null.
    - Weather from several ING-4 batches is combined (e.g. US sites first, other strata later). If a cell
      appears in more than one batch, the newest complete copy is used.

Run:  python -m src.transform.curated [--staging-ref <STG-2 batch id>]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.utils.config import load_yaml
from src.utils.exceptions import ConfigError
from src.utils.log import get_logger
from src.utils.manifest import file_is_complete, make_batch_id
from src.utils.paths import curated_dir, data_dir, staging_dir

STAGE = "curated"

OBS_REQUIRED = [
    "obs_key", "source_code", "source_record_id", "site_key", "activity_id", "activity_type",
    "indicator_code", "sample_date", "value_cfu_100ml", "original_value", "original_unit",
    "is_censored", "censor_direction", "detection_limit", "raw_batch_id", "raw_file",
]
SITES_REQUIRED = [
    "site_key", "source_code", "source_site_id", "site_name", "water_body_type", "realm",
    "latitude", "longitude", "region_code", "region_match", "stratum", "is_sampled",
]
NE_COLUMNS = {"adm1_code": "region_code", "name": "admin1_name", "iso_a2": "iso_a2",
              "adm0_a3": "adm0_a3", "admin": "country_name"}

FACT_OBS_COLUMNS = [
    "obs_key", "site_key", "indicator_code", "source_record_id", "activity_id", "activity_type",
    "sample_date", "value_cfu_100ml", "original_value", "original_unit", "is_censored",
    "censor_direction", "detection_limit", "exceeds_threshold", "rain_48h_mm", "rain_72h_mm",
    "temp_mean_c", "is_wet", "raw_batch_id", "raw_file",
]
DIM_SITE_COLUMNS = [
    "site_key", "source_code", "source_site_id", "region_code", "cell_id", "site_name",
    "water_body_type", "realm", "stratum", "latitude", "longitude", "region_match",
]


# ---------------------------------------------------------------- reading inputs
def _require(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ConfigError(f"{name} is missing columns {missing} (see config/staging_schema.yaml)")


def read_staging_observations(root: Path) -> pd.DataFrame:
    """Read every Parquet part under observations/, whether or not the files also store the
    partition columns. Partition values from the path fill source_code/year when absent."""
    parts = sorted(root.rglob("*.parquet"))
    if not parts:
        raise ConfigError(f"No staging observations under {root}; run STG-2 (build_staging) first")
    frames = []
    for p in parts:
        df = pq.read_table(p).to_pandas()
        # Check every file on its own: concatenating would silently fill a missing column with nulls.
        _require(df, [c for c in OBS_REQUIRED if c != "source_code"], f"staging file {p.relative_to(root)}")
        for seg in p.relative_to(root).parts[:-1]:
            if "=" in seg:
                key, value = seg.split("=", 1)
                if key not in df.columns:
                    df[key] = value
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def read_weather(raw_root: Path, log) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    """Combine all ING-4 batches. Returns (grid_cells, site_cells, weather_daily, batch_ids)."""
    base = raw_root / "open_meteo"
    batches = []
    for mpath in base.rglob("manifest.json") if base.exists() else []:
        m = json.loads(mpath.read_text(encoding="utf-8"))
        if m.get("status") in ("success", "partial"):
            batches.append((m.get("retrieved_at_utc", ""), mpath.parent, m))
    if not batches:
        log.warning("no Open-Meteo batches found; all weather columns will be null")
        empty = pd.DataFrame
        return (empty(columns=["cell_id", "cell_lat", "cell_lon"]), empty(columns=["site_key", "cell_id"]),
                empty(columns=["cell_id", "weather_date", "precipitation_mm", "temp_mean_c", "raw_batch_id"]), [])

    grids, site_cells, weather, ids, seen_cells = [], [], [], [], set()
    for _, bdir, m in sorted(batches, key=lambda b: b[0], reverse=True):     # newest first
        ids.append(m["batch_id"])
        grids.append(pd.read_csv(bdir / "grid_cells.csv", dtype={"cell_id": str}))
        site_cells.append(pd.read_csv(bdir / "site_cells.csv", dtype={"site_key": str, "cell_id": str}))
        for f in m.get("files", []):
            name = f["name"]
            if not (name.startswith("cell_") and name.endswith(".json")):
                continue
            cell_id = name[len("cell_"):-len(".json")]
            if cell_id in seen_cells or not file_is_complete(bdir, name):
                continue
            body = json.loads((bdir / name).read_text(encoding="utf-8"))
            daily = body.get("daily", {})
            weather.append(pd.DataFrame({
                "cell_id": cell_id,
                "weather_date": pd.to_datetime(daily.get("time", [])).date,
                "precipitation_mm": pd.to_numeric(pd.Series(daily.get("precipitation_sum", [])), errors="coerce"),
                "temp_mean_c": pd.to_numeric(pd.Series(daily.get("temperature_2m_mean", [])), errors="coerce"),
                "raw_batch_id": m["batch_id"],
            }))
            seen_cells.add(cell_id)
    grid = pd.concat(grids).drop_duplicates("cell_id").sort_values("cell_id").reset_index(drop=True)
    sc = pd.concat(site_cells).drop_duplicates("site_key").sort_values("site_key").reset_index(drop=True)
    wd = (pd.concat(weather, ignore_index=True) if weather else
          pd.DataFrame(columns=["cell_id", "weather_date", "precipitation_mm", "temp_mean_c", "raw_batch_id"]))
    return grid[["cell_id", "cell_lat", "cell_lon"]], sc[["site_key", "cell_id"]], wd, sorted(ids)


def read_regions(raw_root: Path) -> tuple[pd.DataFrame, str]:
    """Natural Earth admin-1 attributes (no geometry). Returns (table, batch_id)."""
    base = raw_root / "natural_earth"
    found = []
    for mpath in base.rglob("manifest.json") if base.exists() else []:
        m = json.loads(mpath.read_text(encoding="utf-8"))
        shps = sorted(mpath.parent.rglob("*.shp"))
        if m.get("status") == "success" and shps:
            found.append((m.get("retrieved_at_utc", ""), shps[0], m["batch_id"]))
    if not found:
        raise ConfigError(f"No successful Natural Earth batch under {base}; run ING-5 first")
    _, shp, batch_id = sorted(found)[-1]
    import geopandas as gpd  # heavy; only needed here
    attrs = pd.DataFrame(gpd.read_file(shp, ignore_geometry=True))
    _require(attrs, list(NE_COLUMNS), f"Natural Earth {shp.name}")
    return attrs[list(NE_COLUMNS)].rename(columns=NE_COLUMNS), batch_id


# ---------------------------------------------------------------- pure transforms
def build_dim_indicator(sampling: dict, catalog: dict) -> pd.DataFrame:
    rows = []
    for realm, spec in sampling["realms"].items():
        rows.append({"indicator_code": spec["primary_indicator"], "realm": realm,
                     "threshold_cfu_100ml": float(spec["threshold_cfu_100ml"]), "is_scored": True})
    for code in sampling.get("supplementary_indicators", []):
        rows.append({"indicator_code": code, "realm": None, "threshold_cfu_100ml": None, "is_scored": False})
    df = pd.DataFrame(rows)
    df.insert(1, "indicator_name", df["indicator_code"].map(catalog.get("indicator_names", {})))
    return df.sort_values("indicator_code").reset_index(drop=True)


def build_dim_source(catalog: dict, codes: set[str]) -> pd.DataFrame:
    missing = sorted(codes - set(catalog["sources"]))
    if missing:
        raise ConfigError(f"source_catalog.yaml has no entry for {missing}")
    rows = [{"source_code": c, **catalog["sources"][c]} for c in sorted(codes)]
    return pd.DataFrame(rows)[["source_code", "source_name", "provider", "access_method", "url"]]


def build_dim_region(regions: pd.DataFrame, sites: pd.DataFrame) -> pd.DataFrame:
    used = sites[["region_code", "country_iso", "continent"]].dropna(subset=["region_code"]).drop_duplicates("region_code")
    df = used.merge(regions[["region_code", "admin1_name", "country_name"]].drop_duplicates("region_code"),
                    on="region_code", how="left")
    return df[["region_code", "admin1_name", "country_iso", "country_name", "continent"]] \
        .sort_values("region_code").reset_index(drop=True)


def antecedent_weather(obs: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    """Add rain_48h_mm, rain_72h_mm, temp_mean_c for each observation (needs cell_id, sample_date)."""
    w = weather[["cell_id", "weather_date", "precipitation_mm", "temp_mean_c"]].copy()
    w["weather_date"] = pd.to_datetime(w["weather_date"]).dt.date
    out = obs.copy()
    lags = {}
    for k in (0, 1, 2):
        key = out[["cell_id"]].copy()
        key["weather_date"] = [d - timedelta(days=k) for d in out["sample_date"]]
        merged = key.merge(w, on=["cell_id", "weather_date"], how="left")
        lags[k] = merged
    p0, p1, p2 = (lags[k]["precipitation_mm"].to_numpy() for k in (0, 1, 2))
    out["rain_48h_mm"] = pd.Series(p0 + p1, index=out.index)            # NaN if either day missing
    out["rain_72h_mm"] = pd.Series(p0 + p1 + p2, index=out.index)
    out["temp_mean_c"] = lags[0]["temp_mean_c"].to_numpy()
    return out


def build_fact_observation(obs: pd.DataFrame, dim_site: pd.DataFrame, dim_indicator: pd.DataFrame,
                           weather: pd.DataFrame, wet_threshold: float) -> pd.DataFrame:
    df = obs.merge(dim_site[["site_key", "cell_id"]], on="site_key", how="inner")
    df["sample_date"] = pd.to_datetime(df["sample_date"]).dt.date
    df = antecedent_weather(df, weather)
    thr = dim_indicator.set_index("indicator_code")["threshold_cfu_100ml"]
    scored = set(dim_indicator.loc[dim_indicator["is_scored"], "indicator_code"])
    threshold = df["indicator_code"].map(thr)
    df["exceeds_threshold"] = pd.Series(pd.NA, index=df.index, dtype="boolean")
    mask = df["indicator_code"].isin(scored) & df["value_cfu_100ml"].notna()
    df.loc[mask, "exceeds_threshold"] = (df.loc[mask, "value_cfu_100ml"] > threshold[mask]).astype("boolean")
    df["is_wet"] = pd.Series(pd.NA, index=df.index, dtype="boolean")
    has_rain = df["rain_48h_mm"].notna()
    df.loc[has_rain, "is_wet"] = (df.loc[has_rain, "rain_48h_mm"] >= wet_threshold).astype("boolean")
    return df[FACT_OBS_COLUMNS].sort_values("obs_key").reset_index(drop=True)


# ---------------------------------------------------------------- orchestration
def _write(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)          # overwrite atomically: reruns never leave half-written files


def build_curated(staging_ref: str = "manual", run_params: dict | None = None) -> str:
    """Build every curated table. Returns the curated batch_id (ING-6 passes it to build_marts)."""
    sampling, rules = load_yaml("sampling"), load_yaml("business_rules")
    catalog = load_yaml("source_catalog")
    wet_threshold = float(rules["rainfall"]["wet_threshold_mm_48h"])
    raw = data_dir() / "raw"
    log = get_logger(STAGE, "all", None)

    obs = read_staging_observations(staging_dir() / "observations")
    sites_path = staging_dir() / "sites" / "sites.parquet"
    if not sites_path.exists():
        raise ConfigError(f"Missing {sites_path}; run STG-1 (build_sampled_sites) first")
    sites = pd.read_parquet(sites_path)
    _require(obs, OBS_REQUIRED, "staging observations")
    _require(sites, SITES_REQUIRED + ["country_iso", "continent"], "staging sites")

    grid, site_cells, weather, weather_batches = read_weather(raw, log)
    regions, ne_batch = read_regions(raw)

    batch_id = make_batch_id(STAGE, {
        "staging_ref": staging_ref, "weather_batches": weather_batches, "natural_earth": ne_batch,
        "wet_threshold_mm_48h": wet_threshold, "realms": sampling["realms"],
    })
    log = get_logger(STAGE, "all", batch_id)

    sampled = sites[sites["is_sampled"].astype(bool)].copy()
    dim_site = sampled.merge(site_cells, on="site_key", how="left")[DIM_SITE_COLUMNS] \
        .sort_values("site_key").reset_index(drop=True)
    dim_indicator = build_dim_indicator(sampling, catalog)
    dim_source = build_dim_source(catalog, set(dim_site["source_code"]))
    dim_region = build_dim_region(regions, sampled)
    dim_grid = grid[grid["cell_id"].isin(dim_site["cell_id"].dropna())].reset_index(drop=True)
    fact_weather = weather[weather["cell_id"].isin(dim_grid["cell_id"])] \
        .sort_values(["cell_id", "weather_date"]).reset_index(drop=True)

    n_obs_in = len(obs)
    fact_obs = build_fact_observation(obs, dim_site, dim_indicator, fact_weather, wet_threshold)
    dropped_not_sampled = n_obs_in - len(fact_obs)

    # Referential integrity inside the curated layer (fail loudly rather than load orphans)
    problems = []
    if not set(fact_obs["indicator_code"]) <= set(dim_indicator["indicator_code"]):
        problems.append(f"unknown indicator codes {sorted(set(fact_obs['indicator_code']) - set(dim_indicator['indicator_code']))}")
    orphan_regions = set(dim_site["region_code"].dropna()) - set(dim_region["region_code"])
    if orphan_regions:
        problems.append(f"{len(orphan_regions)} site region_codes not in dim_region")
    orphan_cells = set(dim_site["cell_id"].dropna()) - set(dim_grid["cell_id"])
    if orphan_cells:
        problems.append(f"{len(orphan_cells)} site cell_ids not in dim_grid_cell")
    if fact_obs["obs_key"].duplicated().any():
        problems.append(f"{int(fact_obs['obs_key'].duplicated().sum())} duplicate obs_key values")
    if problems:
        raise ConfigError("Curated integrity check failed: " + "; ".join(problems))

    tables = {
        "dim_source": dim_source, "dim_indicator": dim_indicator, "dim_region": dim_region,
        "dim_grid_cell": dim_grid, "dim_site": dim_site,
        "fact_weather_daily": fact_weather, "fact_observation": fact_obs,
    }
    out = curated_dir()
    for name, df in tables.items():
        _write(df, out / f"{name}.parquet")

    coverage = float(fact_obs["rain_48h_mm"].notna().mean()) if len(fact_obs) else 0.0
    sites_without_cell = int(dim_site["cell_id"].isna().sum())
    manifest = {
        "stage": STAGE, "batch_id": batch_id, "status": "success",
        "input_refs": {"staging": staging_ref, "weather_batches": weather_batches, "natural_earth": ne_batch},
        "params": {"wet_threshold_mm_48h": wet_threshold, **(run_params or {})},
        "outputs": [{"path": f"{n}.parquet", "row_count": len(df)} for n, df in tables.items()],
        "drop_counts": {"observation_site_not_sampled": int(dropped_not_sampled)},
        "weather_coverage": round(coverage, 4),
        "sites_without_weather_cell": sites_without_cell,
    }
    tmp = out / "._manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, out / "_manifest.json")

    log.info("rows: " + ", ".join(f"{n}={len(df)}" for n, df in tables.items()))
    log.info(f"observations not at sampled sites (dropped): {dropped_not_sampled}; "
             f"weather coverage: {coverage:.1%}; sites without a weather cell: {sites_without_cell}")
    if coverage < 0.9:
        log.warning(f"weather coverage {coverage:.1%} is below 90% (VAL-3 will flag this)")
    return batch_id


def main() -> None:
    parser = argparse.ArgumentParser(description="CUR-1: build curated dimension and fact tables")
    parser.add_argument("--staging-ref", default="manual", help="staging batch id (from STG-2)")
    print(build_curated(parser.parse_args().staging_ref))


if __name__ == "__main__":
    main()
