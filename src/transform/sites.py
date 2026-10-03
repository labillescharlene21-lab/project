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

from pathlib import Path

import pandas as pd

from src.utils import manifest, paths

STAGE = "staging"
SOURCE_CODE = "stg_sites"  # batch / manifest code for this step
OWQ_SOURCES = ("owq_gemstat", "owq_eionet")
OWQ_COLUMNS = ["date", "indicator", "value", "unit", "latitude", "longitude",
               "source", "region", "water_body"]


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