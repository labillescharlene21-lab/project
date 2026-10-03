"""STG-1: one site table across sources, admin-1 regions, OWQ sampling.

Reads the latest successful raw batches (OWQ GEMStat + Eionet, ING-2's WQP
sampled_sites.csv, Natural Earth admin-1) and writes:

  data/staging/sites/sites.parquet                  every site (sampled and unsampled)
  data/staging/sampled_sites/sampled_sites.parquet  contract with ING-4 (weather)
  data/staging/sampled_sites/_manifest.json         sampling summary per stratum x realm
  data/staging/_drop_log.parquet                    rows with stage="sites"

Run:  python -m src.transform.sites
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pycountry_convert as pcc

from src.utils import config as cfg
from src.utils import log, manifest, paths
from src.utils.exceptions import ConfigError
from src.utils.sampling import stratified_sample

STAGE = "staging"
SOURCE_CODE = "stg_sites"  # batch / manifest code for this step
OWQ_SOURCES = ("owq_gemstat", "owq_eionet")
OWQ_COLUMNS = ["date", "indicator", "value", "unit", "latitude", "longitude",
               "source", "region", "water_body"]
NE_SOURCE = "natural_earth"
NE_SHP = "ne_10m_admin_1_states_provinces.shp"
NEAREST_MAX_M = 25_000      # issue: max distance for "nearest" region match
METRIC_CRS = "EPSG:6933"    # equal-area, units = meters
WQP_SOURCE = "wqp_summary"            # ING-2 raw batch folder
WQP_SITES_CSV = "sampled_sites.csv"
WQP_SUMMARY_CSV = "sampling_summary.csv"

# Column order and types from config/staging_schema.yaml (STG-0)
SITES_DTYPES = {
    "site_key": "string", "source_code": "string", "source_site_id": "string",
    "site_name": "string", "source_region": "string", "water_body_type": "string",
    "realm": "string", "latitude": "float64", "longitude": "float64",
    "country_iso": "string", "continent": "string", "region_code": "string",
    "region_match": "string", "stratum": "string", "n_samples": "int32",
    "n_years": "int16", "is_eligible": "bool", "is_sampled": "bool",
}
SAMPLED_COLUMNS = ["site_key", "source_code", "latitude", "longitude", "stratum", "realm"]


# ---------------------------------------------------------------- load raw

def latest_success_batch(source_code: str) -> Path:
    """Newest raw batch folder for a source whose manifest status is 'success'."""
    root = paths.data_dir() / "raw" / source_code
    found = []
    for batch_dir in sorted(root.glob("batch_id=*")):
        m = manifest.read_manifest(batch_dir)
        if m and m.get("status") == "success":
            found.append((m["retrieved_at_utc"], batch_dir.name, batch_dir))
    if not found:
        raise FileNotFoundError(f"No successful raw batch for {source_code} in {root}")
    return max(found)[2]


def batch_id_of(batch_dir: Path) -> str:
    return batch_dir.name.removeprefix("batch_id=")


def _count_comment_lines(path: Path, prefix: str = "#") -> int:
    """OWQ CSVs start with '#' attribution lines before the real header."""
    n = 0
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            if not line.startswith(prefix):
                break
            n += 1
    return n


def read_owq_csv(path: Path) -> pd.DataFrame:
    """One OWQ chunk CSV, everything as text (numbers are parsed later)."""
    return pd.read_csv(path, skiprows=_count_comment_lines(path), dtype=str,
                       keep_default_na=False, encoding="utf-8-sig")


def load_owq(source_code: str) -> tuple[pd.DataFrame, str]:
    
    batch_dir = latest_success_batch(source_code)
    frames = [read_owq_csv(p) for p in sorted(batch_dir.glob("*.csv"))]
    frames = [f for f in frames if not f.empty]
    rows = (pd.concat(frames, ignore_index=True) if frames
            else pd.DataFrame(columns=OWQ_COLUMNS))
    rows["source_code"] = source_code
    return rows, batch_id_of(batch_dir)

# ---------------------------------------------------------------- OWQ sites

def realm_lookup(sampling_cfg: dict) -> dict[str, str]:
    """water_body (lowercase) -> 'freshwater' | 'marine' | 'excluded' (sampling.yaml)."""
    lookup = {}
    for realm, names in sampling_cfg["water_body_realm"].items():
        for name in names:
            lookup[str(name).strip().lower()] = realm
    return lookup


def owq_site_id(lat: float, lon: float) -> str:
    """mappings.yaml: coordinates rounded to 5 decimals."""
    return f"{round(lat, 5):.5f}_{round(lon, 5):.5f}"


def build_owq_sites(rows: pd.DataFrame, source_code: str, sampling_cfg: dict,
                    mappings_cfg: dict, start_year: int, end_year: int
                    ) -> tuple[pd.DataFrame, Counter]:
    """One row per OWQ site with realm, n_samples, n_years and is_eligible.

    Returns (sites, drops). drops counts sites removed per reason, except
    missing_coordinates, which counts raw rows (they cannot form a site).
    """
    drops: Counter = Counter()
    df = rows.copy()

    # 1. Coordinates
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    coords_ok = df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)
    drops["missing_coordinates"] += int((~coords_ok).sum())
    df = df[coords_ok].copy()

    # 2. Site id from rounded coordinates
    df["source_site_id"] = [owq_site_id(la, lo)
                            for la, lo in zip(df["latitude"], df["longitude"])]
    df["water_body"] = df["water_body"].str.strip().str.lower()

    # 3. One row per site (lat/lon = the rounded values the id is built from)
    df["_lat5"] = df["latitude"].round(5)
    df["_lon5"] = df["longitude"].round(5)
    sites = df.groupby("source_site_id", sort=True).agg(
        latitude=("_lat5", "first"),
        longitude=("_lon5", "first"),
        water_body_type=("water_body", lambda s: s.mode().iat[0]),  # ties -> alphabetical
        source_region=("region", "first"),
    )

    # 4. Realm from water-body type
    sites["realm"] = sites["water_body_type"].map(realm_lookup(sampling_cfg))
    excluded = sites["realm"] == "excluded"
    unmapped = sites["realm"].isna()
    drops["water_body_excluded"] += int(excluded.sum())
    drops["water_body_unmapped"] += int(unmapped.sum())
    sites = sites[~excluded & ~unmapped].copy()

    # 5. Eligibility: primary indicator of the site's realm, within the period
    primary = {realm: c["primary_indicator"] for realm, c in sampling_cfg["realms"].items()}
    df["indicator_code"] = df["indicator"].map(mappings_cfg["indicator_names"][source_code])
    df["year"] = pd.to_numeric(df["date"].str[:4], errors="coerce")
    df = df.join(sites["realm"], on="source_site_id", how="inner")
    counted = df[(df["indicator_code"] == df["realm"].map(primary))
                 & df["year"].between(start_year, end_year)]
    stats = counted.groupby("source_site_id").agg(
        n_samples=("year", "size"), n_years=("year", "nunique"))
    sites = sites.join(stats)
    sites[["n_samples", "n_years"]] = sites[["n_samples", "n_years"]].fillna(0).astype(int)

    elig = sampling_cfg["eligibility"]
    sites["is_eligible"] = ((sites["n_samples"] >= elig["min_samples"])
                            & (sites["n_years"] >= elig["min_distinct_years"]))

    sites = sites.reset_index()
    sites["source_code"] = source_code
    sites["site_key"] = source_code + ":" + sites["source_site_id"]
    sites["site_name"] = None  # OWQ provides no site names
    return sites, drops
# ---------------------------------------------------------------- regions

def load_regions() -> tuple[gpd.GeoDataFrame, str]:
    """Natural Earth admin-1 polygons from the latest successful batch."""
    batch_dir = latest_success_batch(NE_SOURCE)
    shp = next(batch_dir.rglob(NE_SHP), None)
    if shp is None:
        raise FileNotFoundError(f"{NE_SHP} not found in {batch_dir}")
    regions = gpd.read_file(shp)[["adm1_code", "iso_a2", "adm0_a3", "geometry"]]
    return regions.to_crs("EPSG:4326"), batch_id_of(batch_dir)


def assign_regions(sites: pd.DataFrame, regions: gpd.GeoDataFrame) -> pd.DataFrame:
    """Add region_code, country_iso and region_match ('within' | 'nearest' | 'none').

    within:  point inside an admin-1 polygon (EPSG:4326)
    nearest: closest polygon within NEAREST_MAX_M, measured in METRIC_CRS
    none:    nothing within NEAREST_MAX_M -> region_code and country_iso are null
    A point on a shared border can match two regions; the lowest adm1_code wins,
    so the result is deterministic.
    """
    pts = gpd.GeoDataFrame(
        sites[["site_key"]].copy(),
        geometry=gpd.points_from_xy(sites["longitude"], sites["latitude"]),
        crs="EPSG:4326",
    )
    reg = regions[["adm1_code", "iso_a2", "adm0_a3", "geometry"]]
    keep = ["site_key", "adm1_code", "iso_a2", "adm0_a3", "region_match"]

    within = gpd.sjoin(pts, reg, how="inner", predicate="within")
    within = within.sort_values(["site_key", "adm1_code"]).drop_duplicates("site_key")
    within["region_match"] = "within"
    matched = [within[keep]]

    rest = pts[~pts["site_key"].isin(within["site_key"])]
    if len(rest):
        # The metric projection is undefined at the poles: clip polygons to +-85 deg first
        reg_m = reg.copy()
        reg_m["geometry"] = reg_m.geometry.clip_by_rect(-180, -85, 180, 85)
        reg_m = reg_m[~reg_m.geometry.is_empty].to_crs(METRIC_CRS)
        nearest = gpd.sjoin_nearest(rest.to_crs(METRIC_CRS), reg_m,
                                    how="inner", max_distance=NEAREST_MAX_M)
        nearest = nearest.sort_values(["site_key", "adm1_code"]).drop_duplicates("site_key")
        nearest["region_match"] = "nearest"
        matched.append(nearest[keep])

    out = sites.merge(pd.concat(matched, ignore_index=True), on="site_key", how="left")
    out["region_match"] = out["region_match"].fillna("none")
    out["region_code"] = out["adm1_code"]
    # Natural Earth uses '-99' when iso_a2 is unknown: fall back to adm0_a3 (STG-0 schema)
    out["country_iso"] = out["iso_a2"].where(out["iso_a2"] != "-99", out["adm0_a3"])
    return out.drop(columns=["adm1_code", "iso_a2", "adm0_a3"])

# ---------------------------------------------------------------- continent + stratum

def continent_of(country_iso, overrides: dict) -> str | None:
    """Continent name from an ISO code (mappings.yaml › continents). None if unknown.

    Accepts alpha-2 and the alpha-3 fallback used when Natural Earth's iso_a2 is '-99'.
    """
    if not isinstance(country_iso, str) or not country_iso:
        return None
    if country_iso in overrides:
        return overrides[country_iso]
    try:
        code = country_iso
        if len(code) == 3:
            code = pcc.country_alpha3_to_country_alpha2(code)
        return pcc.convert_continent_code_to_continent_name(
            pcc.country_alpha2_to_continent_code(code))
    except KeyError:
        return None


def add_continent(sites: pd.DataFrame, mappings_cfg: dict) -> pd.DataFrame:
    overrides = (mappings_cfg.get("continents") or {}).get("overrides") or {}
    out = sites.copy()
    out["continent"] = [continent_of(c, overrides) for c in out["country_iso"]]
    return out


STRATUM_FILTER_KEYS = {"country", "continent", "exclude_country"}


def _as_list(value) -> list:
    return value if isinstance(value, list) else [value]


def assign_strata(sites: pd.DataFrame, strata_cfg: dict) -> pd.Series:
    """Stratum per site from sampling.yaml › strata (source + filter).

    Strata are checked in file order and the first match wins; no match -> None.
    An unknown filter key is a config error, so a typo can't silently match everything.
    """
    stratum = pd.Series([None] * len(sites), index=sites.index, dtype=object)
    for name, spec in strata_cfg.items():
        f = spec.get("filter") or {}
        unknown = set(f) - STRATUM_FILTER_KEYS
        if unknown:
            raise ConfigError(f"sampling.yaml stratum {name!r}: unknown filter keys {sorted(unknown)}")
        mask = (sites["source_code"] == spec["source"]) & stratum.isna()
        if "country" in f:
            mask &= sites["country_iso"].isin(_as_list(f["country"]))
        if "continent" in f:
            mask &= sites["continent"].isin(_as_list(f["continent"]))
        if "exclude_country" in f:
            mask &= ~sites["country_iso"].isin(_as_list(f["exclude_country"]))
        stratum[mask] = name
    return stratum
# ---------------------------------------------------------------- WQP + sampling

def load_wqp_sampled() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """ING-2's WQP sites (already sampled, stratum 'us') and its sampling summary."""
    batch_dir = latest_success_batch(WQP_SOURCE)
    sites = pd.read_csv(batch_dir / WQP_SITES_CSV,
                        dtype={"site_key": str, "source_code": str, "source_site_id": str})
    summary = pd.read_csv(batch_dir / WQP_SUMMARY_CSV)
    sites = sites.rename(columns={"site_type": "water_body_type"})
    sites["site_name"] = None
    sites["source_region"] = None
    sites["is_eligible"] = True
    sites["is_sampled"] = True
    return sites, summary, batch_id_of(batch_dir)


def sample_owq(owq_sites: pd.DataFrame, sampling_cfg: dict, k: int, seed: int
               ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sample eligible OWQ sites per stratum x realm with DE1's stratified_sample.

    Returns (owq_sites with is_sampled, summary per stratum x realm).
    """
    s = sampling_cfg["sampling"]
    pool = owq_sites[owq_sites["is_eligible"] & owq_sites["stratum"].notna()]
    sampled, summary = stratified_sample(
        pool, stratum_col="stratum", realm_col="realm", k=k, seed=seed,
        take_all_if_fewer=s["take_all_if_fewer"], min_sites_to_keep=s["min_sites_to_keep"],
    )
    out = owq_sites.copy()
    out["is_sampled"] = out["site_key"].isin(sampled["site_key"])
    return out, summary


def _check_unique(df: pd.DataFrame, name: str) -> None:
    dupes = df["site_key"][df["site_key"].duplicated()]
    if len(dupes):
        raise ValueError(f"{name}: duplicate site_key values, e.g. {dupes.head(5).tolist()}")


def finalize_sites(sites: pd.DataFrame) -> pd.DataFrame:
    """Exactly the STG-0 `sites` columns, in order, typed, sorted by site_key."""
    out = sites[list(SITES_DTYPES)].astype(SITES_DTYPES)
    out = out.sort_values("site_key", kind="mergesort").reset_index(drop=True)
    _check_unique(out, "sites")
    return out


def finalize_sampled(sites: pd.DataFrame) -> pd.DataFrame:
    """The ING-4 contract: exactly six columns, one row per sampled site."""
    out = sites.loc[sites["is_sampled"], SAMPLED_COLUMNS].reset_index(drop=True)
    _check_unique(out, "sampled_sites")
    return out
# ---------------------------------------------------------------- write outputs

DROP_LOG_DTYPES = {"stage": "string", "source_code": "string", "reason": "string",
                   "row_count": "int32", "batch_id": "string"}


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write to a temp file, then rename, so a crash never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def write_drop_log(drops: dict[str, Counter], batch_id: str, stage: str = "sites") -> Path:
    """Replace this stage's rows in the shared drop log; keep other stages' rows.

    STG-1 writes stage="sites", STG-2 writes stage="observations".
    """
    rows = [{"stage": stage, "source_code": src, "reason": reason,
             "row_count": n, "batch_id": batch_id}
            for src, counts in sorted(drops.items())
            for reason, n in sorted(counts.items()) if n]
    new = pd.DataFrame(rows, columns=list(DROP_LOG_DTYPES)).astype(DROP_LOG_DTYPES)
    path = paths.staging_dir() / "_drop_log.parquet"
    if path.exists():
        old = pd.read_parquet(path)
        new = pd.concat([old[old["stage"] != stage], new], ignore_index=True)
    _write_parquet(new.astype(DROP_LOG_DTYPES), path)
    return path


# ---------------------------------------------------------------- entry point

def build_sampled_sites(run_params: dict | None = None) -> str:
    """STG-1 entry point (ING-6 calls this). Returns the staging batch_id.

    run_params may override start_year, end_year, k and seed; anything else
    comes from sampling.yaml. Unknown keys are ignored.
    """
    run_params = run_params or {}
    s_cfg, m_cfg = cfg.load_yaml("sampling"), cfg.load_yaml("mappings")
    start = int(run_params.get("start_year", s_cfg["period"]["start_year"]))
    end = int(run_params.get("end_year", s_cfg["period"]["end_year"]))
    k = int(run_params.get("k", s_cfg["sampling"]["k_per_stratum_realm"]))
    seed = int(run_params.get("seed", s_cfg["random_seed"]))

    # Load raw
    regions, ne_batch = load_regions()
    inputs = {NE_SOURCE: ne_batch}
    owq_parts, drops = [], {}
    for src in OWQ_SOURCES:
        rows, raw_batch = load_owq(src)
        inputs[src] = raw_batch
        part, drops[src] = build_owq_sites(rows, src, s_cfg, m_cfg, start, end)
        owq_parts.append(part)
    wqp, wqp_summary, inputs[WQP_SOURCE] = load_wqp_sampled()

    # Regions, continent, strata, sampling (OWQ); regions + continent (WQP)
    owq = add_continent(assign_regions(pd.concat(owq_parts, ignore_index=True), regions), m_cfg)
    owq["stratum"] = assign_strata(owq, s_cfg["strata"])
    for src in OWQ_SOURCES:
        drops[src]["no_stratum"] += int((owq["source_code"].eq(src) & owq["stratum"].isna()).sum())
    owq, owq_summary = sample_owq(owq, s_cfg, k, seed)
    wqp = add_continent(assign_regions(wqp, regions), m_cfg)

    sites = finalize_sites(pd.concat([wqp, owq], ignore_index=True))
    sampled = finalize_sampled(sites)
    summary = pd.concat([wqp_summary.assign(sampled_by="ING-2 (WQP)"),
                         owq_summary.assign(sampled_by="STG-1 (OWQ)")], ignore_index=True)

    params = {"start_year": start, "end_year": end, "k": k, "seed": seed, "inputs": inputs}
    batch_id = manifest.make_batch_id(SOURCE_CODE, params)
    logger = log.get_logger(STAGE, SOURCE_CODE, batch_id)
    match_counts = sites.groupby("source_code")["region_match"].value_counts().unstack(fill_value=0)
    logger.info("Region match counts:\n%s", match_counts.to_string())
    logger.info("Sampling summary per stratum x realm:\n%s", summary.to_string(index=False))
    logger.info("Drops: %s", {s: dict(c) for s, c in drops.items()})

    # Write outputs
    staging = paths.staging_dir()
    _write_parquet(sites, staging / "sites" / "sites.parquet")
    _write_parquet(sampled, staging / "sampled_sites" / "sampled_sites.parquet")
    write_drop_log(drops, batch_id)
    info = {
        "stage": STAGE, "source_code": SOURCE_CODE, "batch_id": batch_id,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": params,
        "row_counts": {"sites": len(sites), "sampled_sites": len(sampled)},
        "region_match": {src: {m: int(n) for m, n in row.items()}
                         for src, row in match_counts.iterrows()},
        "drops": {s: dict(c) for s, c in drops.items()},
        "sampling_summary": json.loads(summary.to_json(orient="records")),
    }
    with open(staging / "sampled_sites" / "_manifest.json", "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2)
    logger.info("Wrote %d sites, %d sampled sites", len(sites), len(sampled))
    return batch_id


def main() -> None:
    parser = argparse.ArgumentParser(description="STG-1: build sites.parquet and sampled_sites.parquet")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--k", type=int)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    run_params = {name: v for name, v in vars(args).items() if v is not None}
    print(f"STG-1 done: batch {build_sampled_sites(run_params)}")


if __name__ == "__main__":
    main()