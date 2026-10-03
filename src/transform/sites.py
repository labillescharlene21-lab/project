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

from collections import Counter
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.utils import config as cfg
from src.utils import manifest, paths

STAGE = "staging"
SOURCE_CODE = "stg_sites"  # batch / manifest code for this step
OWQ_SOURCES = ("owq_gemstat", "owq_eionet")
OWQ_COLUMNS = ["date", "indicator", "value", "unit", "latitude", "longitude",
               "source", "region", "water_body"]
NE_SOURCE = "natural_earth"
NE_SHP = "ne_10m_admin_1_states_provinces.shp"
NEAREST_MAX_M = 25_000      # issue: max distance for "nearest" region match
METRIC_CRS = "EPSG:6933"    # equal-area, units = meters


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