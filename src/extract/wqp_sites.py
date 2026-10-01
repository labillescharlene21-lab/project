"""Extractor for WQP per-site annual sample counts (summary service) and site sampling.

One request per US state: the summary service times out on very large queries. Each
response is saved exactly as received (summary_US-XX.zip) next to its extracted CSV.
Colons are replaced by '-' in file names because ':' is invalid in Windows file names.

Eligibility and sampling are computed in memory from the saved CSVs (raw files are never
modified) and written as sampled_sites.csv and sampling_summary.csv in the same batch folder.
"""
from __future__ import annotations

import argparse
import io
import time
import zipfile
from pathlib import Path

import pandas as pd

from src.utils import config as cfg
from src.utils import http, log, manifest, paths
from src.utils.exceptions import ConfigError, EmptyResponseError, SourceRequestError
from src.utils.sampling import stratified_sample

STAGE = "extract"
SOURCE_CODE = "wqp_summary"   # batch/manifest source code
SITE_SOURCE_CODE = "wqp"      # source_code column in sampled_sites.csv
STRATUM = "us"
_PAUSE_SECONDS = 1.0  # polite pause between state requests

# Real column names observed in the periodOfRecord summary CSV (verified on US:15 and US:55).
SITE_COL = "MonitoringLocationIdentifier"
YEAR_COL = "YearSummarized"
CHAR_COL = "CharacteristicName"
ACTIVITY_COL = "ActivityCount"
RESULT_COL = "ResultCount"
# Raw site type, NOT ResolvedMonitoringLocationTypeName: the resolved column labels
# 'BEACH Program Site-Great Lake' and 'BEACH Program Site-Lake' as 'Ocean'.
SITE_TYPE_COL = "MonitoringLocationTypeName"
LAT_COL = "MonitoringLocationLatitude"
LON_COL = "MonitoringLocationLongitude"
REQUIRED_COLUMNS = [SITE_COL, YEAR_COL, CHAR_COL, ACTIVITY_COL, SITE_TYPE_COL, LAT_COL, LON_COL]

SITES_CSV = "sampled_sites.csv"
SUMMARY_CSV = "sampling_summary.csv"
OUTPUT_NAMES = {SITES_CSV, SUMMARY_CSV}
OUTPUT_COLUMNS = ["site_key", "source_code", "source_site_id", "stratum", "realm", "site_type",
                  "latitude", "longitude", "n_samples", "n_years"]


def _load_wqp_config() -> dict:
    return cfg.load_yaml("sources")["wqp"]


def primary_characteristic_names(wqp_cfg: dict, sampling_cfg: dict) -> list[str]:
    """WQP characteristicName strings of the primary indicators (from sampling.yaml realms)."""
    char_map = wqp_cfg["characteristic_map"]
    names = []
    for realm, spec in sampling_cfg["realms"].items():
        indicator = spec["primary_indicator"]
        if indicator not in char_map:
            raise ConfigError(f"realm {realm!r}: primary indicator {indicator!r} not in wqp.characteristic_map")
        names.append(char_map[indicator])
    return sorted(set(names))


def fetch_state_codes(session, logger, wqp_cfg: dict) -> list[str]:
    """Return sorted WQP state codes (e.g. 'US:15') from Codes/statecode (path is lowercase)."""
    url = f"{wqp_cfg['codes_url']}/statecode"
    params = {"countrycode": wqp_cfg["countrycode"], "mimeType": "json"}
    resp = http.request(session, "GET", url, params=params, logger=logger)
    return sorted(c["value"] for c in resp.json()["codes"])


def _state_stem(statecode: str) -> str:
    return "summary_" + statecode.replace(":", "-")


def _summary_params(wqp_cfg: dict, statecode: str, site_types: list[str], characteristics: list[str]) -> dict:
    return {
        "countrycode": wqp_cfg["countrycode"],
        "statecode": statecode,
        "siteType": ";".join(site_types),
        "characteristicName": ";".join(characteristics),
        "dataProfile": "periodOfRecord",
        "summaryYears": "all",
        "mimeType": "csv",
        "zip": wqp_cfg["zip"],
    }


def _unzip_single_csv(content: bytes, url: str) -> bytes:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            members = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if len(members) != 1:
                raise SourceRequestError(f"GET {url}: expected exactly one CSV in zip, found {z.namelist()}")
            return z.read(members[0])
    except zipfile.BadZipFile as exc:
        raise SourceRequestError(f"GET {url}: response is not a valid zip file: {exc}") from exc


def _done_states(existing: dict | None, batch_dir: Path, states: list[str]) -> set[str]:
    """States whose zip+csv are intact in the manifest, or that were recorded as empty."""
    if existing is None:
        return set()
    intact = {f["name"] for f in existing["files"] if manifest.file_is_complete(batch_dir, f["name"])}
    empty = {r["statecode"] for r in existing.get("requests_log", []) if r.get("empty")}
    done = set()
    for s in states:
        stem = _state_stem(s)
        if s in empty or (f"{stem}.zip" in intact and f"{stem}.csv" in intact):
            done.add(s)
    return done


def _download_state(session, logger, wqp_cfg, statecode, site_types, characteristics, batch_dir):
    """Download one state. Returns (file_entries, request_entry, warning_or_None)."""
    url = wqp_cfg["summary_url"]
    params = _summary_params(wqp_cfg, statecode, site_types, characteristics)
    logger.info("Requesting summary for %s", statecode)
    try:
        resp = http.request(session, "GET", url, params=params, logger=logger)
    except EmptyResponseError:
        msg = f"{statecode}: empty response (no matching sites), nothing saved"
        logger.warning(msg)
        return [], {"method": "GET", "url": url, "query": params, "statecode": statecode,
                    "http_status": None, "empty": True}, msg

    stem = _state_stem(statecode)
    zip_path = batch_dir / f"{stem}.zip"
    csv_path = batch_dir / f"{stem}.csv"
    zip_path.write_bytes(resp.content)                      # as received
    csv_path.write_bytes(_unzip_single_csv(resp.content, resp.url))

    rows = manifest.count_csv_rows(csv_path)
    logger.info("Saved %s and %s (%d rows)", zip_path.name, csv_path.name, rows)
    entries = [manifest.file_entry(zip_path, "zip", None), manifest.file_entry(csv_path, "csv", rows)]
    request = {"method": "GET", "url": url, "query": params, "statecode": statecode,
               "http_status": resp.status_code}
    return entries, request, None


# ---------------------------------------------------------------- eligibility (in memory)

def realm_lookup(sampling_cfg: dict) -> tuple[dict[str, str], set[str]]:
    """Flatten sampling.yaml water_body_realm into (site type -> realm, excluded types), lowercase."""
    mapping: dict[str, str] = {}
    excluded: set[str] = set()
    for realm, types in sampling_cfg["water_body_realm"].items():
        for t in types:
            key = str(t).strip().lower()
            if realm == "excluded":
                excluded.add(key)
            else:
                mapping[key] = realm
    return mapping, excluded


def load_summaries(batch_dir: Path, states: list[str]) -> pd.DataFrame:
    frames = []
    for statecode in states:
        path = batch_dir / f"{_state_stem(statecode)}.csv"
        if path.exists():
            frames.append(pd.read_csv(path, dtype=str))
    if not frames:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def build_eligible_sites(summary: pd.DataFrame, wqp_cfg: dict, sampling_cfg: dict, logger,
                         start_year: int, end_year: int) -> pd.DataFrame:
    """Aggregate per-site annual counts of the realm's primary indicator and apply eligibility."""
    missing = [c for c in REQUIRED_COLUMNS if c not in summary.columns]
    if missing:
        raise SourceRequestError(f"summary CSV is missing expected columns {missing}; got {list(summary.columns)}")

    df = summary.copy()
    df["_year"] = pd.to_numeric(df[YEAR_COL], errors="coerce")
    df["_activity"] = pd.to_numeric(df[ACTIVITY_COL], errors="coerce").fillna(0).astype(int)
    df["_lat"] = pd.to_numeric(df[LAT_COL], errors="coerce")
    df["_lon"] = pd.to_numeric(df[LON_COL], errors="coerce")

    df = df[(df["_year"] >= start_year) & (df["_year"] <= end_year)]

    # Realm from raw site type (case-insensitive). Unmapped or excluded types are dropped and logged.
    mapping, excluded = realm_lookup(sampling_cfg)
    df = df.assign(_type=df[SITE_TYPE_COL].fillna("").str.strip().str.lower())
    df["realm"] = df["_type"].map(mapping)
    dropped = df[df["realm"].isna()]
    for raw_type, group in dropped.groupby(dropped[SITE_TYPE_COL].fillna("(blank)"), sort=True):
        reason = "excluded" if group["_type"].iloc[0] in excluded else "unmapped"
        logger.info("Dropped site type %r (%s): %d sites", raw_type, reason, group[SITE_COL].nunique())
    df = df[df["realm"].notna()]

    # Count only the primary indicator of the site's realm.
    char_map = wqp_cfg["characteristic_map"]
    primary = {realm: char_map[spec["primary_indicator"]] for realm, spec in sampling_cfg["realms"].items()}
    df = df[df[CHAR_COL] == df["realm"].map(primary)]

    if df.empty:
        logger.warning("No candidate sites after period, site type and indicator filters")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    multi = df.groupby(SITE_COL)["realm"].nunique()
    if (multi > 1).any():
        logger.warning("%d sites appear under more than one realm; first realm kept", int((multi > 1).sum()))

    agg = df.groupby(SITE_COL, sort=True).agg(
        realm=("realm", "first"),
        site_type=(SITE_TYPE_COL, "first"),
        latitude=("_lat", "first"),
        longitude=("_lon", "first"),
        n_samples=("_activity", "sum"),
    )
    years = df[df["_activity"] >= 1].groupby(SITE_COL)["_year"].nunique().rename("n_years")
    agg = agg.join(years)
    agg["n_years"] = agg["n_years"].fillna(0).astype(int)
    agg["n_samples"] = agg["n_samples"].astype(int)
    logger.info("Candidate sites: %d (%s)", len(agg), agg["realm"].value_counts().to_dict())

    elig_cfg = sampling_cfg["eligibility"]
    ok = (agg["n_samples"] >= elig_cfg["min_samples"]) & (agg["n_years"] >= elig_cfg["min_distinct_years"])
    if elig_cfg.get("require_coordinates", True):
        ok &= agg["latitude"].between(-90, 90) & agg["longitude"].between(-180, 180)
    eligible = agg[ok].reset_index()

    eligible["site_key"] = SITE_SOURCE_CODE + ":" + eligible[SITE_COL]
    eligible["source_code"] = SITE_SOURCE_CODE
    eligible["source_site_id"] = eligible[SITE_COL]
    eligible["stratum"] = STRATUM
    eligible = eligible[OUTPUT_COLUMNS].sort_values("site_key", kind="mergesort").reset_index(drop=True)
    logger.info("Eligible sites per realm: %s", eligible["realm"].value_counts().to_dict())
    return eligible


# ---------------------------------------------------------------- run

def run(**overrides) -> Path:
    """Run the extraction. Returns the path to the batch manifest.json."""
    cfg.load_env()
    wqp_cfg = _load_wqp_config()
    sampling_cfg = cfg.load_yaml("sampling")

    start_year = overrides.get("start_year", sampling_cfg["period"]["start_year"])
    end_year = overrides.get("end_year", sampling_cfg["period"]["end_year"])
    cfg.validate_period(start_year, end_year)

    site_types = list(wqp_cfg["site_types"])
    characteristics = primary_characteristic_names(wqp_cfg, sampling_cfg)

    session = http.build_session()
    states = overrides.get("states")
    if not states:
        # Deviation from AGENTS.md 7.2: the state list comes from the Codes endpoint (ticket step 2)
        # and is part of the batch params, so one cheap Codes call happens before the batch check.
        states = fetch_state_codes(session, log.get_logger(STAGE, SOURCE_CODE), wqp_cfg)
    states = sorted(states)

    params = {
        "states": states,
        "site_types": site_types,
        "characteristic_names": characteristics,
        "period": {"start_year": start_year, "end_year": end_year},
        "sampling": sampling_cfg,
        "summary_url": wqp_cfg["summary_url"],
    }
    batch_id = manifest.make_batch_id(SOURCE_CODE, params)
    logger = log.get_logger(STAGE, SOURCE_CODE, batch_id)
    batch_dir = paths.raw_batch_dir(SOURCE_CODE, batch_id, create=True)
    logger.info("Start: states requested=%d, characteristics=%s, site_types=%s",
                len(states), characteristics, site_types)

    existing = manifest.read_manifest(batch_dir)
    done = _done_states(existing, batch_dir, states)
    files: list[dict] = []
    requests_log: list[dict] = []
    warnings: list[str] = []
    if existing is not None:
        files = [f for f in existing["files"]
                 if f["name"] not in OUTPUT_NAMES and manifest.file_is_complete(batch_dir, f["name"])]
        requests_log = list(existing.get("requests_log", []))
        warnings = [w for w in existing.get("warnings", []) if not w.startswith("sampling:")]
    if done:
        logger.info("Resuming: %d of %d states already complete", len(done), len(states))

    def _write(status: str, notes: str) -> Path:
        return manifest.write_manifest(
            batch_dir, source_code=SOURCE_CODE, batch_id=batch_id, params=params,
            requests_log=requests_log, files=sorted(files, key=lambda f: f["name"]),
            status=status, warnings=warnings, notes=notes,
        )

    for statecode in states:
        if statecode in done:
            logger.info("Skipping %s (already complete)", statecode)
            continue
        entries, request, warning = _download_state(
            session, logger, wqp_cfg, statecode, site_types, characteristics, batch_dir
        )
        files.extend(entries)
        requests_log.append(request)
        if warning:
            warnings.append(warning)
        _write("partial", "summaries downloading; eligibility and sampling not yet built")
        time.sleep(_PAUSE_SECONDS)

    # Eligibility and sampling (in memory; raw summary files are not modified)
    summary = load_summaries(batch_dir, states)
    logger.info("Loaded %d summary rows from %d states", len(summary), len(states))
    eligible = build_eligible_sites(summary, wqp_cfg, sampling_cfg, logger, start_year, end_year)

    samp = sampling_cfg["sampling"]
    sampled, sampling_summary = stratified_sample(
        eligible,
        stratum_col="stratum",
        realm_col="realm",
        k=samp["k_per_stratum_realm"],
        seed=sampling_cfg["random_seed"],
        take_all_if_fewer=samp["take_all_if_fewer"],
        min_sites_to_keep=samp["min_sites_to_keep"],
    )
    for row in sampling_summary.itertuples():
        if row.eligible < samp["k_per_stratum_realm"]:
            msg = (f"sampling: {row.stratum}/{row.realm} has {row.eligible} eligible sites, "
                   f"fewer than k={samp['k_per_stratum_realm']}")
            logger.warning(msg)
            warnings.append(msg)
        if row.low_coverage:
            msg = f"sampling: {row.stratum}/{row.realm} is low coverage ({row.sampled} sites sampled)"
            logger.warning(msg)
            warnings.append(msg)
    logger.info("Sampled sites per realm: %s", sampled["realm"].value_counts().to_dict())

    sites_path = batch_dir / SITES_CSV
    summary_path = batch_dir / SUMMARY_CSV
    sampled[OUTPUT_COLUMNS].to_csv(sites_path, index=False)
    sampling_summary.to_csv(summary_path, index=False)
    files = [f for f in files if f["name"] not in OUTPUT_NAMES]
    files.append(manifest.file_entry(sites_path, "csv", len(sampled)))
    files.append(manifest.file_entry(summary_path, "csv", len(sampling_summary)))

    manifest_path = _write("success", "per-site annual counts, eligible sites sampled per realm")
    logger.info("Finished: states requested=%d, eligible=%d, sampled=%d, warnings=%d",
                len(states), len(eligible), len(sampled), len(warnings))
    return manifest_path


def _cli():
    parser = argparse.ArgumentParser(description="Extract WQP per-site annual sample counts and sample sites")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--states", default=None,
                        help="comma-separated WQP state codes, e.g. US:15,US:55 (default: all from Codes/statecode)")
    args = parser.parse_args()

    overrides = {}
    if args.start_year is not None:
        overrides["start_year"] = args.start_year
    if args.end_year is not None:
        overrides["end_year"] = args.end_year
    if args.states:
        overrides["states"] = [s.strip() for s in args.states.split(",") if s.strip()]

    manifest_path = run(**overrides)
    print(f"Manifest written to: {manifest_path}")


if __name__ == "__main__":
    _cli()