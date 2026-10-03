"""STG-2: harmonize raw observations into partitioned staging Parquet.

Reads the latest successful raw batches (ING-3 WQP results, ING-1 OWQ GEMStat + Eionet),
keeps only STG-1's sampled sites and the period, parses values / censoring / units with
the pure rules in harmonize.py, and writes

  data/staging/observations/source_code={code}/year={yyyy}/part-0000.parquet
  data/staging/observations/_manifest.json
  data/staging/_drop_log.parquet                rows with stage="observations"

The partition columns (source_code, year) live in the folder names, not inside the files:
read the whole dataset with pd.read_parquet("data/staging/observations").

Run:  python -m src.transform.staging
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.transform import harmonize as h
from src.transform.sites import (OWQ_SOURCES, batch_id_of, latest_success_batch,
                                 owq_site_id, read_owq_csv, write_drop_log)
from src.utils import config as cfg
from src.utils import log, manifest, paths

STAGE = "staging"
SOURCE_CODE = "stg_observations"  # batch / manifest code for this step
WQP = "wqp"
WQP_RAW_SOURCE = "wqp_results"    # ING-3 raw batch folder

# Common intermediate layout: one row per raw result, everything still text
ROW_COLUMNS = ["source_code", "source_record_id", "source_site_id", "activity_id",
               "activity_type", "indicator_label", "date_text", "value_text", "unit_text",
               "condition_text", "limit_text", "limit_unit_text", "result_status", "media",
               "raw_batch_id", "raw_file"]

# WQP raw column (ING-3) -> common layout column
WQP_COLUMNS = {
    "MonitoringLocationIdentifier": "source_site_id",
    "ActivityIdentifier": "activity_id",
    "ActivityTypeCode": "activity_type",
    "CharacteristicName": "indicator_label",
    "ActivityStartDate": "date_text",
    "ResultMeasureValue": "value_text",
    "ResultMeasure/MeasureUnitCode": "unit_text",
    "ResultDetectionConditionText": "condition_text",
    "DetectionQuantitationLimitMeasure/MeasureValue": "limit_text",
    "DetectionQuantitationLimitMeasure/MeasureUnitCode": "limit_unit_text",
    "ResultStatusIdentifier": "result_status",
    "ActivityMediaName": "media",
}

# Output columns and types (config/staging_schema.yaml › observations); sample_date is a date
OBS_DTYPES = {
    "obs_key": "string", "source_code": "string", "source_record_id": "string",
    "site_key": "string", "source_site_id": "string", "activity_id": "string",
    "activity_type": "string", "indicator_code": "string", "year": "int16",
    "value_cfu_100ml": "float64", "original_value": "string", "original_unit": "string",
    "is_censored": "bool", "censor_direction": "string", "detection_limit": "float64",
    "result_status": "string", "raw_batch_id": "string", "raw_file": "string",
}
OBS_COLUMNS = ["obs_key", "source_code", "source_record_id", "site_key", "source_site_id",
               "activity_id", "activity_type", "indicator_code", "sample_date", "year",
               "value_cfu_100ml", "original_value", "original_unit", "is_censored",
               "censor_direction", "detection_limit", "result_status", "raw_batch_id", "raw_file"]


# ---------------------------------------------------------------- load raw

def sha1_of(parts) -> str:
    """mappings.yaml › record_id: fields joined with '|', empty string for nulls."""
    text = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _read_all(batch_dir, pattern: str, reader) -> pd.DataFrame | None:
    """All non-empty files matching pattern, in file-name order, with a raw_file column."""
    frames = []
    for path in sorted(batch_dir.glob(pattern)):
        df = reader(path)
        if len(df):
            frames.append(df.assign(raw_file=path.name))
    return pd.concat(frames, ignore_index=True) if frames else None


def load_wqp_rows(record_fields: list[str]) -> tuple[pd.DataFrame, str]:
    """ING-3 WQP results (latest success batch) in the common text layout."""
    batch_dir = latest_success_batch(WQP_RAW_SOURCE)
    batch_id = batch_id_of(batch_dir)
    raw = _read_all(batch_dir, "chunk_*.csv",
                    lambda p: pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8-sig"))
    if raw is None:
        return pd.DataFrame(columns=ROW_COLUMNS), batch_id
    rows = raw[list(WQP_COLUMNS)].rename(columns=WQP_COLUMNS)
    rows["source_code"] = WQP
    rows["source_record_id"] = [sha1_of(r) for r in
                                raw[record_fields].itertuples(index=False, name=None)]
    rows["raw_batch_id"] = batch_id
    rows["raw_file"] = raw["raw_file"]
    return rows[ROW_COLUMNS], batch_id


def load_owq_rows(source_code: str) -> tuple[pd.DataFrame, str]:
    """ING-1 OWQ rows (latest success batch) in the common text layout.

    source_site_id uses STG-1's rounded-coordinate rule, so site_key matches sampled_sites.
    """
    batch_dir = latest_success_batch(source_code)
    batch_id = batch_id_of(batch_dir)
    raw = _read_all(batch_dir, "*.csv", read_owq_csv)
    if raw is None:
        return pd.DataFrame(columns=ROW_COLUMNS), batch_id
    lat = pd.to_numeric(raw["latitude"], errors="coerce")
    lon = pd.to_numeric(raw["longitude"], errors="coerce")
    site_ids = [owq_site_id(a, b) if pd.notna(a) and pd.notna(b) else ""
                for a, b in zip(lat, lon)]
    rows = pd.DataFrame({
        "source_code": source_code, "source_site_id": site_ids,
        "activity_id": None, "activity_type": None,
        "indicator_label": raw["indicator"], "date_text": raw["date"],
        "value_text": raw["value"], "unit_text": raw["unit"],
        "condition_text": "", "limit_text": "", "limit_unit_text": "",
        "result_status": None, "media": None,
        "raw_batch_id": batch_id, "raw_file": raw["raw_file"],
    }, index=raw.index)
    rows["source_record_id"] = [sha1_of(p) for p in
                                zip(rows["source_site_id"], raw["date"], raw["indicator"])]
    return rows[ROW_COLUMNS], batch_id


# ---------------------------------------------------------------- harmonize

def harmonize_rows(rows: pd.DataFrame, source_code: str, sampled_keys: set,
                   start_year: int, end_year: int, mappings: dict
                   ) -> tuple[pd.DataFrame, Counter, Counter]:
    """Scope filters, value / censoring / unit parsing and dedupe for one source.

    Returns (observations, drops by reason, unmapped units with counts).
    Every input row ends up either in observations or in exactly one drop reason.
    """
    drops: Counter = Counter()
    df = rows.copy()

    def drop(mask: pd.Series, reason: str) -> None:
        nonlocal df
        mask = mask.fillna(False).astype(bool)
        drops[reason] += int(mask.sum())
        df = df.loc[~mask]

    # 1. Scope: sampled sites, period, known indicators
    df["site_key"] = df["source_code"] + ":" + df["source_site_id"]
    drop(~df["site_key"].isin(sampled_keys), "site_not_sampled")
    df["_date"] = pd.to_datetime(df["date_text"].str.strip(), format="%Y-%m-%d", errors="coerce")
    drop(~df["_date"].dt.year.between(start_year, end_year), "date_out_of_period")
    names = mappings["indicator_names"].get(source_code, {})
    df["indicator_code"] = df["indicator_label"].str.strip().map(names)
    drop(df["indicator_code"].isna(), "indicator_unmapped")

    # 2. WQP-only filters (mappings.yaml › filters)
    if source_code == WQP:
        f = mappings.get("filters") or {}
        drop(~df["media"].isin(f.get("wqp_media_keep") or []), "media_not_water")
        blank = pd.Series(False, index=df.index)
        for text in f.get("wqp_activity_type_exclude_contains") or []:
            blank |= df["activity_type"].str.contains(text, case=False, regex=False, na=False)
        drop(blank, "qc_blank")
        drop(df["result_status"].isin(f.get("wqp_result_status_exclude") or []), "result_rejected")

    # 3. Values and censoring (harmonize.py)
    parsed = [h.parse_value(v, c, lim, mappings["censoring"])
              for v, c, lim in zip(df["value_text"], df["condition_text"], df["limit_text"])]
    df["_value"] = pd.Series([p.value for p in parsed], index=df.index, dtype="float64")
    df["_limit"] = pd.Series([p.limit for p in parsed], index=df.index, dtype="float64")
    df["_censored"] = [p.is_censored for p in parsed]
    df["_direction"] = [p.direction for p in parsed]
    df["_from_dl"] = [p.from_detection_limit for p in parsed]
    df["_reason"] = [p.reason for p in parsed]
    for reason in ("non_numeric_value", "negative_value"):
        drop(df["_reason"] == reason, reason)

    # 4. Units -> CFU/100 mL
    units = mappings["units"]
    df["_unit"] = [h.pick_unit(u, lu, fdl) for u, lu, fdl in
                   zip(df["unit_text"], df["limit_unit_text"], df["_from_dl"])]
    df["_mult"] = pd.Series([h.unit_multiplier(u, units) for u in df["_unit"]],
                            index=df.index, dtype="float64")
    unmapped = Counter(u if u else "(blank)" for u in df.loc[df["_mult"].isna(), "_unit"])
    drop(df["_mult"].isna(), "unit_unmapped")
    df["value_cfu_100ml"] = df["_value"] * df["_mult"]
    reported_dl = (pd.Series([h.to_number(t) for t in df["limit_text"]], index=df.index, dtype="float64")
                   * pd.Series([h.unit_multiplier(u, units) for u in df["limit_unit_text"]],
                               index=df.index, dtype="float64"))
    df["detection_limit"] = reported_dl.fillna(df["_limit"] * df["_mult"])

    # 5. Dedupe exact obs_key (keep the first in raw file order)
    df["obs_key"] = df["source_code"] + ":" + df["source_record_id"]
    drop(df["obs_key"].duplicated(keep="first"), "duplicate_obs_key")

    # 6. Output columns and types
    out = pd.DataFrame({
        "obs_key": df["obs_key"], "source_code": df["source_code"],
        "source_record_id": df["source_record_id"], "site_key": df["site_key"],
        "source_site_id": df["source_site_id"], "activity_id": df["activity_id"],
        "activity_type": df["activity_type"], "indicator_code": df["indicator_code"],
        "sample_date": df["_date"].dt.date, "year": df["_date"].dt.year,
        "value_cfu_100ml": df["value_cfu_100ml"],
        "original_value": df["value_text"].where(df["value_text"] != "", None),
        "original_unit": df["unit_text"].where(df["unit_text"] != "", None),
        "is_censored": df["_censored"], "censor_direction": df["_direction"],
        "detection_limit": df["detection_limit"], "result_status": df["result_status"],
        "raw_batch_id": df["raw_batch_id"], "raw_file": df["raw_file"],
    })
    out = out.astype(OBS_DTYPES)[OBS_COLUMNS].reset_index(drop=True)
    return out, drops, unmapped

# ---------------------------------------------------------------- write

PARTITION_COLS = ["source_code", "year"]


def write_partitions(obs: pd.DataFrame, root: Path, info: dict) -> list[dict]:
    """Write source_code=/year= partitions plus _manifest.json, replacing the whole dataset.

    Everything goes to a temp folder that is then swapped in: a rerun never leaves
    partitions from a previous run behind, and a crash never leaves a half-written dataset.
    """
    tmp = root.with_name(root.name + ".tmp")
    old = root.with_name(root.name + ".old")
    for folder in (tmp, old):
        if folder.exists():
            shutil.rmtree(folder)
    tmp.mkdir(parents=True)
    parts = []
    for (src, year), part in obs.groupby(PARTITION_COLS, sort=True):
        folder = tmp / f"source_code={src}" / f"year={int(year)}"
        folder.mkdir(parents=True)
        (part.drop(columns=PARTITION_COLS)
             .sort_values("obs_key", kind="mergesort")
             .to_parquet(folder / "part-0000.parquet", index=False))
        parts.append({"source_code": src, "year": int(year), "rows": len(part)})
    with open(tmp / "_manifest.json", "w", encoding="utf-8") as f:
        json.dump({**info, "partitions": parts}, f, indent=2)
    if root.exists():
        root.rename(old)
    tmp.rename(root)
    if old.exists():
        shutil.rmtree(old)
    return parts


# ---------------------------------------------------------------- entry point

def build_staging(run_params: dict | None = None) -> str:
    """STG-2 entry point (ING-6 calls this). Returns the staging batch_id.

    run_params may override start_year and end_year; everything else comes from config.
    Needs STG-1's sampled_sites.parquet.
    """
    run_params = run_params or {}
    s_cfg, m_cfg = cfg.load_yaml("sampling"), cfg.load_yaml("mappings")
    start = int(run_params.get("start_year", s_cfg["period"]["start_year"]))
    end = int(run_params.get("end_year", s_cfg["period"]["end_year"]))

    staging = paths.staging_dir()
    sampled_path = staging / "sampled_sites" / "sampled_sites.parquet"
    if not sampled_path.exists():
        raise FileNotFoundError(f"{sampled_path} not found: run STG-1 (python -m src.transform.sites) first")
    sampled_keys = set(pd.read_parquet(sampled_path, columns=["site_key"])["site_key"])

    loaders = [(WQP, lambda: load_wqp_rows(m_cfg["record_id"]["wqp"]["fields"]))]
    loaders += [(src, lambda src=src: load_owq_rows(src)) for src in OWQ_SOURCES]
    inputs, counts, drops, unmapped, frames = {}, {}, {}, {}, []
    for src, load in loaders:
        rows, inputs[src] = load()
        obs, drops[src], unmapped[src] = harmonize_rows(rows, src, sampled_keys, start, end, m_cfg)
        counts[src] = {"rows_in": len(rows), "rows_out": len(obs),
                       "rows_dropped": sum(drops[src].values())}
        if counts[src]["rows_in"] != counts[src]["rows_out"] + counts[src]["rows_dropped"]:
            raise RuntimeError(f"{src}: rows in != rows out + drops ({counts[src]})")
        frames.append(obs)
    obs = pd.concat(frames, ignore_index=True)

    params = {"start_year": start, "end_year": end, "inputs": inputs,
              "sampled_sites_sha256": manifest.sha256_file(sampled_path)}
    batch_id = manifest.make_batch_id(SOURCE_CODE, params)
    logger = log.get_logger(STAGE, SOURCE_CODE, batch_id)
    for src, c in counts.items():
        logger.info("%s: rows in %d, rows out %d, drops %s", src, c["rows_in"], c["rows_out"],
                    {k: v for k, v in drops[src].items() if v})
        if unmapped[src]:
            logger.warning("%s: unmapped units (dropped): %s", src, dict(unmapped[src].most_common()))

    info = {
        "stage": STAGE, "source_code": SOURCE_CODE, "batch_id": batch_id,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": params, "row_counts": counts, "partition_by": PARTITION_COLS,
        "drops": {s: {k: v for k, v in d.items() if v} for s, d in drops.items()},
        "unmapped_units": {s: dict(u.most_common()) for s, u in unmapped.items()},
    }
    parts = write_partitions(obs, staging / "observations", info)
    write_drop_log(drops, batch_id, stage="observations")
    logger.info("Wrote %d observations in %d partitions", len(obs), len(parts))
    return batch_id


def main() -> None:
    parser = argparse.ArgumentParser(description="STG-2: harmonize observations into staging Parquet")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    args = parser.parse_args()
    run_params = {name: v for name, v in vars(args).items() if v is not None}
    print(f"STG-2 done: batch {build_staging(run_params)}")


if __name__ == "__main__":
    main()