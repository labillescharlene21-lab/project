"""Deterministic stratified site sampling, shared by the WQP and OWQ extractors."""
from __future__ import annotations

import pandas as pd

SITE_KEY_COL = "site_key"
SUMMARY_COLUMNS = ["stratum", "realm", "eligible", "sampled", "low_coverage"]


def stratified_sample(
    sites: pd.DataFrame,
    *,
    stratum_col: str,
    realm_col: str,
    k: int,
    seed: int,
    take_all_if_fewer: bool,
    min_sites_to_keep: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (sampled_sites, summary).

    Deterministic: sort by site key before sampling, then
    DataFrame.sample(n=min(k, len(group)), random_state=seed) per (stratum, realm).
    summary has one row per (stratum, realm): eligible, sampled, low_coverage flag.

    If a group has fewer than k sites and take_all_if_fewer is False, nothing is
    sampled from that group (sampled=0, flagged low_coverage).
    """
    if k < 1:
        raise ValueError(f"k must be >= 1, got {k}")
    missing = [c for c in (stratum_col, realm_col, SITE_KEY_COL) if c not in sites.columns]
    if missing:
        raise ValueError(f"sites is missing required columns: {missing}")

    ordered = sites.sort_values(
        [stratum_col, realm_col, SITE_KEY_COL], kind="mergesort"
    ).reset_index(drop=True)

    sampled_parts: list[pd.DataFrame] = []
    summary_rows: list[dict] = []

    for (stratum, realm), group in ordered.groupby([stratum_col, realm_col], sort=True):
        eligible = len(group)
        if eligible < k and not take_all_if_fewer:
            picked = group.iloc[0:0]
        else:
            picked = group.sample(n=min(k, eligible), random_state=seed)
        sampled_parts.append(picked)
        summary_rows.append({
            "stratum": stratum,
            "realm": realm,
            "eligible": eligible,
            "sampled": len(picked),
            "low_coverage": len(picked) < min_sites_to_keep,
        })

    if sampled_parts:
        sampled = pd.concat(sampled_parts, ignore_index=True)
        sampled = sampled.sort_values(
            [stratum_col, realm_col, SITE_KEY_COL], kind="mergesort"
        ).reset_index(drop=True)
    else:
        sampled = ordered.iloc[0:0].copy()

    summary = pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS)
    return sampled, summary