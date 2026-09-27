"""ING-4: Open-Meteo daily weather per 0.25° grid cell.

Flow
1. Load sampled sites (DE2's staging file, or --sites-file during development).
2. Drop sites with missing/invalid coordinates.
3. Assign every site to a grid cell; one API request per distinct cell.
4. Write grid_cells.csv and site_cells.csv into the raw batch folder.
5. Download each cell's full period as JSON, saved exactly as received.
6. Check each response's day count; record mismatches as manifest warnings.
7. Resume: cells already complete in the manifest are skipped (no network call).

Rate limits (Open-Meteo free tier): long requests are weighted
    weight = locations * (days / 14) * (variables / 10)
One cell for 2015-2025 with 2 daily variables weighs ~57 calls, so the extractor
throttles to an hourly budget and stops cleanly at a daily budget. The manifest is
then "partial"; rerunning later resumes where it stopped.

Run:
    python -m src.extract.weather --plan                        # count cells + estimated calls, no network
    python -m src.extract.weather --sites-file <csv|parquet>    # dev input
    python -m src.extract.weather                               # default input from staging
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from datetime import date
from pathlib import Path

import pandas as pd

from src.utils import http
from src.utils.config import load_yaml, validate_period
from src.utils.exceptions import ConfigError
from src.utils.log import get_logger
from src.utils.manifest import (
    file_entry,
    file_is_complete,
    make_batch_id,
    read_manifest,
    write_manifest,
)
from src.utils.paths import raw_batch_dir, staging_dir

SOURCE_CODE = "open_meteo"
STAGE = "extract"
REQUIRED_COLUMNS = ["site_key", "source_code", "latitude", "longitude", "stratum", "realm"]
DEFAULT_SITES_FILE = Path("sampled_sites") / "sampled_sites.parquet"   # under data/staging/


# ---------------------------------------------------------------- pure helpers
def cell_centre(value: float, res: float) -> float:
    """Nearest grid-cell centre, rounding halves up (deterministic for negatives too).

    `+ 0.0` turns -0.0 into 0.0 so cell ids never contain "-0.00".
    """
    return round(math.floor(value / res + 0.5) * res, 2) + 0.0


def make_cell_id(cell_lat: float, cell_lon: float) -> str:
    return f"{cell_lat:.2f}_{cell_lon:.2f}"


def load_sites(path: Path) -> pd.DataFrame:
    """Read a CSV or Parquet site file and check the input contract columns."""
    if not path.exists():
        raise ConfigError(f"Sites file not found: {path}. Pass --sites-file or run STG-1 first.")
    df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path, dtype={"site_key": str})
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ConfigError(f"Sites file {path} is missing required columns: {missing}")
    return df


def clean_sites(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop rows with missing, non-numeric, out-of-range, or (0, 0) coordinates. Returns (clean, dropped)."""
    out = df.copy()
    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    valid = (
        out["latitude"].between(-90, 90)
        & out["longitude"].between(-180, 180)
        & ~((out["latitude"] == 0) & (out["longitude"] == 0))   # "null island": a missing-value placeholder
    )
    clean = out[valid].drop_duplicates(subset="site_key").sort_values("site_key").reset_index(drop=True)
    return clean, int(len(df) - len(clean))


def assign_cells(sites: pd.DataFrame, res: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (grid_cells, site_cells), both sorted for deterministic output."""
    s = sites[["site_key", "latitude", "longitude"]].copy()
    s["cell_lat"] = s["latitude"].map(lambda v: cell_centre(v, res))
    s["cell_lon"] = s["longitude"].map(lambda v: cell_centre(v, res))
    s["cell_id"] = [make_cell_id(a, b) for a, b in zip(s["cell_lat"], s["cell_lon"])]
    site_cells = s[["site_key", "cell_id"]].sort_values("site_key").reset_index(drop=True)
    grid_cells = (
        s.groupby(["cell_id", "cell_lat", "cell_lon"], as_index=False)
        .agg(n_sites=("site_key", "count"))
        .sort_values("cell_id")
        .reset_index(drop=True)
    )
    return grid_cells, site_cells


def expected_days(start_year: int, end_year: int) -> int:
    return (date(end_year, 12, 31) - date(start_year, 1, 1)).days + 1


def request_weight(n_days: int, n_variables: int) -> float:
    """Open-Meteo's published weight for one location (never counted below 1 call)."""
    return max(1.0, (n_days / 14) * (n_variables / 10))


def check_response(body: dict, daily_vars: list[str], n_days: int) -> list[str]:
    """Return warning messages for a response body (empty list = OK). Never modifies the file."""
    warnings = []
    daily = body.get("daily")
    if not isinstance(daily, dict) or "time" not in daily:
        return ["response has no daily.time"]
    if len(daily["time"]) != n_days:
        warnings.append(f"daily.time has {len(daily['time'])} days, expected {n_days}")
    for var in daily_vars:
        if var not in daily:
            warnings.append(f"daily.{var} missing")
        elif len(daily[var]) != len(daily["time"]):
            warnings.append(f"daily.{var} length {len(daily[var])} != daily.time length {len(daily['time'])}")
    return warnings


# ---------------------------------------------------------------- main entry
def run(**overrides) -> Path:
    """Extract weather for every grid cell. Returns the manifest path (or grid_cells.csv with plan=True)."""
    sources = load_yaml("sources")
    sampling = load_yaml("sampling")
    cfg = sources["open_meteo"]

    start_year = int(overrides.get("start_year") or sampling["period"]["start_year"])
    end_year = int(overrides.get("end_year") or sampling["period"]["end_year"])
    validate_period(start_year, end_year)

    res = float(cfg["grid_resolution_deg"])
    daily_vars = list(cfg["daily"])
    n_days = expected_days(start_year, end_year)
    weight = request_weight(n_days, len(daily_vars))
    hourly_budget = float(cfg.get("max_weighted_calls_per_hour", 4500))
    daily_budget = float(cfg.get("max_weighted_calls_per_day", 9000))
    min_interval = max(float(cfg.get("min_seconds_between_requests", 1.0)), weight * 3600 / hourly_budget)

    # 1-3. Sites -> cells (no network)
    sites_file = Path(overrides["sites_file"]) if overrides.get("sites_file") else staging_dir() / DEFAULT_SITES_FILE
    raw_sites = load_sites(sites_file)
    sites, dropped = clean_sites(raw_sites)
    grid_cells, site_cells = assign_cells(sites, res)
    cell_ids = grid_cells["cell_id"].tolist()

    params = {
        "cells_sha256": hashlib.sha256("\n".join(cell_ids).encode()).hexdigest(),
        "n_cells": len(cell_ids),
        "start_date": f"{start_year}-01-01",
        "end_date": f"{end_year}-12-31",
        "daily": daily_vars,
        "timezone": cfg["timezone"],
        "grid_resolution_deg": res,
        "archive_url": cfg["archive_url"],
    }
    batch_id = make_batch_id(SOURCE_CODE, params)
    log = get_logger(STAGE, SOURCE_CODE, batch_id)
    log.info(f"sites in: {len(raw_sites)}, dropped (bad coordinates/duplicates): {dropped}, "
             f"sites used: {len(sites)}, grid cells: {len(cell_ids)}")
    log.info(f"per-request weight ≈ {weight:.1f} calls; estimated total ≈ {weight * len(cell_ids):,.0f} calls; "
             f"daily budget {daily_budget:,.0f} → ~{int(daily_budget // weight)} cells/day; "
             f"interval between requests {min_interval:.1f}s")

    batch_dir = raw_batch_dir(SOURCE_CODE, batch_id)
    grid_cells.to_csv(batch_dir / "grid_cells.csv", index=False)
    site_cells.to_csv(batch_dir / "site_cells.csv", index=False)
    if overrides.get("plan"):
        log.info(f"plan only: wrote {batch_dir / 'grid_cells.csv'}; no API calls made")
        return batch_dir / "grid_cells.csv"

    # Manifest state (resume)
    previous = read_manifest(batch_dir) or {}
    files = {f["name"]: f for f in previous.get("files", []) if f["name"].startswith("cell_")}
    requests_log = list(previous.get("requests", []))
    warnings = [w for w in previous.get("warnings", []) if w.split(":", 1)[0] in files]

    def save(status: str) -> Path:
        entries = [
            file_entry(batch_dir / "grid_cells.csv", "csv", len(grid_cells)),
            file_entry(batch_dir / "site_cells.csv", "csv", len(site_cells)),
        ] + [files[k] for k in sorted(files)]
        return write_manifest(batch_dir, source_code=SOURCE_CODE, batch_id=batch_id, params=params,
                              requests_log=requests_log, files=entries, status=status, warnings=warnings,
                              notes=f"weight per request ≈ {weight:.1f}; interval {min_interval:.1f}s")

    session = http.build_session()
    used, requested, skipped, last_request = 0.0, 0, 0, 0.0
    cells = grid_cells.set_index("cell_id")

    for cell_id in cell_ids:
        name = f"cell_{cell_id}.json"
        if name in files and file_is_complete(batch_dir, name):
            skipped += 1
            continue
        if used + weight > daily_budget:
            remaining = len(cell_ids) - skipped - requested
            log.warning(f"daily budget reached after {requested} requests; {remaining} cells remaining. "
                        f"Rerun after 24h to resume (completed cells are skipped).")
            path = save("partial")
            log.info(f"requested {requested}, skipped {skipped}, warnings {len(warnings)}, status partial")
            return path

        wait = min_interval - (time.monotonic() - last_request)
        if requested and wait > 0:
            time.sleep(wait)

        query = {
            "latitude": float(cells.at[cell_id, "cell_lat"]),
            "longitude": float(cells.at[cell_id, "cell_lon"]),
            "start_date": params["start_date"],
            "end_date": params["end_date"],
            "daily": ",".join(daily_vars),
            "timezone": params["timezone"],
        }
        last_request = time.monotonic()
        response = http.request(session, "GET", cfg["archive_url"], params=query, logger=log)
        used += weight
        requested += 1

        path = batch_dir / name
        path.write_bytes(response.content)               # exactly as received
        body = json.loads(response.content)
        cell_warnings = check_response(body, daily_vars, n_days)
        warnings = [w for w in warnings if not w.startswith(f"{name}:")] + [f"{name}: {w}" for w in cell_warnings]
        for w in cell_warnings:
            log.warning(f"{name}: {w}")
        files[name] = file_entry(path, "json", len(body.get("daily", {}).get("time", [])))
        requests_log.append({"method": "GET", "url": cfg["archive_url"], "query": query,
                             "http_status": response.status_code, "file": name})
        save("partial")                                   # after every cell, so an interruption can resume
        if requested % 10 == 0:
            log.info(f"progress: {requested} requested, {skipped} skipped, {len(cell_ids)} cells")

    path = save("success")
    log.info(f"done: cells {len(cell_ids)}, requested {requested}, skipped {skipped}, "
             f"warnings {len(warnings)}, status success")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="ING-4: Open-Meteo weather per grid cell")
    parser.add_argument("--sites-file", help="CSV or Parquet with the input contract columns")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--plan", action="store_true", help="count cells and estimated calls; no API calls")
    args = parser.parse_args()
    print(run(sites_file=args.sites_file, start_year=args.start_year, end_year=args.end_year, plan=args.plan))


if __name__ == "__main__":
    main()
