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

import hashlib
from collections import Counter

import pandas as pd

from src.transform import harmonize as h
from src.transform.sites import (OWQ_SOURCES, batch_id_of, latest_success_batch,
                                 owq_site_id, read_owq_csv)

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