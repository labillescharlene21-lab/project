"""Unit tests for characteristic-name handling and eligibility in src/extract/wqp_sites.py (no network)."""
import logging

import pandas as pd

from src.extract.wqp_sites import build_eligible_sites, primary_characteristic_names

LOG = logging.getLogger("test")
WQP = {"characteristic_map": {
    "e_coli": "Escherichia coli",
    "enterococci": "Enterococcus",
    "enterococci_alt": "Enterococci",
    "fecal_coliform": "Fecal Coliform",
}}
SAMPLING = {
    "realms": {"freshwater": {"primary_indicator": "e_coli"}, "marine": {"primary_indicator": "enterococci"}},
    "water_body_realm": {"freshwater": ["stream"], "marine": ["ocean"], "excluded": []},
    "eligibility": {"min_samples": 10, "min_distinct_years": 2, "require_coordinates": True},
}


def rows(site, site_type, char, counts):
    return [{"MonitoringLocationIdentifier": site, "YearSummarized": str(y), "CharacteristicName": char,
             "ActivityCount": str(n), "MonitoringLocationTypeName": site_type,
             "MonitoringLocationLatitude": "10", "MonitoringLocationLongitude": "20"}
            for y, n in counts.items()]


def eligible(*parts):
    df = pd.DataFrame([r for p in parts for r in p])
    return build_eligible_sites(df, WQP, SAMPLING, LOG, 2015, 2025)


def test_request_includes_alternate_spelling():
    assert primary_characteristic_names(WQP, SAMPLING) == ["Enterococci", "Enterococcus", "Escherichia coli"]


def test_both_spellings_count_toward_marine_samples():
    out = eligible(rows("M1", "Ocean", "Enterococcus", {2020: 6}), rows("M1", "Ocean", "Enterococci", {2021: 6}))
    assert list(out["site_key"]) == ["wqp:M1"]
    assert (out.loc[0, "n_samples"], out.loc[0, "n_years"]) == (12, 2)


def test_single_spelling_below_threshold_is_not_eligible():
    out = eligible(rows("M1", "Ocean", "Enterococcus", {2020: 3, 2021: 3}))
    assert out.empty


def test_alternate_spelling_not_counted_for_freshwater_and_other_indicators_ignored():
    out = eligible(
        rows("F1", "Stream", "Enterococci", {2020: 10, 2021: 10}),       # not freshwater's primary
        rows("F2", "Stream", "Fecal Coliform", {2020: 10, 2021: 10}),    # not a primary indicator
        rows("F3", "Stream", "Escherichia coli", {2020: 10, 2021: 10}),
    )
    assert list(out["site_key"]) == ["wqp:F3"]