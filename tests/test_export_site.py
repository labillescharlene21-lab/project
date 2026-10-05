"""Site export: synthetic curated marts with known answers."""
import json

import numpy as np
import pandas as pd
import pytest

from src.analysis import export_site
from src.utils.exceptions import ConfigError


def world(cur):
    cur.mkdir(parents=True, exist_ok=True)
    sites = pd.DataFrame({
        "site_key": ["a", "b", "c", "d", "e"], "source_code": "wqp", "stratum": ["us", "us", "gemstat_latin_america", "europe", "us"],
        "realm": ["freshwater", "freshwater", "freshwater", "marine", "freshwater"],
        "latitude": [10.0, 11.0, 12.0, 13.0, np.nan], "longitude": [20.0, 21.0, 22.0, 23.0, 24.0],
        "region_code": ["R1", "R1", "R2", "R3", "R1"], "water_body_type": "lake"})
    sites.to_parquet(cur / "dim_site.parquet")
    hot = pd.DataFrame({"site_key": ["a", "b", "c", "d"], "is_persistent_hotspot": [True, False, True, False],
                        "exceedance_rate": [0.5, 0.0, 0.9, 0.01], "median_ratio_to_threshold": [1.2, 0.1, 3.0, 0.05],
                        "poor_years": [3, 0, 4, 0], "years_monitored": [4, 4, 4, 0], "trend_slope": [0.1, 0.0, 0.2, np.nan],
                        "n_samples": 40})
    hot.to_parquet(cur / "mart_site_hotspot.parquet")
    regs = pd.DataFrame({"region_code": ["R1", "R2", "R3"], "admin1_name": ["One", "Dos", "Tres"],
                         "country_name": ["United States", "México", "Hungary"], "country_iso": ["US", "MX", "HU"],
                         "status": ["ranked", "ranked", "insufficient_sites"], "n_sites": [2, 3, 1], "n_hotspots": [1, 3, 0],
                         "priority_score": [0.4, 0.9, np.nan], "priority_rank": [2, 1, np.nan], "rank_in_country": [1, 1, np.nan]})
    regs.to_parquet(cur / "mart_region_priority.parquet")
    pd.DataFrame({"obs_key": list("12345678"), "rain_48h_mm": [1, 2, np.nan, np.nan, 3, 4, 5, 6]}).to_parquet(
        cur / "fact_observation.parquet")


def load(path):
    text = path.read_text(encoding="utf-8")
    assert text.startswith("window.SITE_DATA = ") and text.rstrip().endswith(";")
    return json.loads(text[len("window.SITE_DATA = "):].rstrip().rstrip(";"))


def test_payload_contents(isolated_dirs, tmp_path):
    data, _ = isolated_dirs
    world(data / "curated")
    p = load(export_site.run(docs_dir=tmp_path / "docs"))
    m = p["meta"]
    assert m["sites"] == 4 and m["sites_scored"] == 3 and m["hotspots"] == 2        # site e has no coordinates -> skipped
    assert m["observations"] == 8 and m["weather_coverage"] == 0.75 and m["hotspot_share"] == round(2 / 3, 4)
    status = {s["site_key"]: s["status"] for s in p["sites"]}
    assert status == {"a": "hotspot", "b": "monitored", "c": "hotspot", "d": "insufficient"}
    assert [r["region"] for r in p["top_regions"]] == ["Dos", "One"]                  # unranked region excluded, rank order
    assert p["top_regions"][0]["country"] == "México" and p["top_regions"][0]["score"] == 0.9
    assert [s["site"] for s in p["top_sites"]] == ["c", "a"]                          # highest exceedance first
    assert p["top_sites"][0]["region"] == "Dos"


def test_strata_and_nulls(isolated_dirs, tmp_path):
    data, _ = isolated_dirs
    world(data / "curated")
    p = load(export_site.run(docs_dir=tmp_path / "docs"))
    us = next(s for s in p["strata"] if s["stratum"] == "us")
    assert us["sites"] == 2 and us["hotspots"] == 1 and us["share"] == 0.5
    d = next(s for s in p["sites"] if s["site_key"] == "d")
    assert d["exceedance_rate"] == 0.01 and d["poor_years"] == 0                      # JSON-safe, no NaN tokens
    assert "NaN" not in (tmp_path / "docs" / "data" / "site_data.js").read_text(encoding="utf-8")


def test_without_observations_table(isolated_dirs, tmp_path):
    data, _ = isolated_dirs
    world(data / "curated")
    (data / "curated" / "fact_observation.parquet").unlink()
    m = load(export_site.run(docs_dir=tmp_path / "docs"))["meta"]
    assert m["observations"] is None and m["weather_coverage"] is None


def test_missing_marts_fail_clearly(isolated_dirs, tmp_path):
    with pytest.raises(ConfigError, match="run CUR-1 and MART-1"):
        export_site.run(docs_dir=tmp_path / "docs")


def test_rerun_identical_apart_from_timestamp(isolated_dirs, tmp_path):
    data, _ = isolated_dirs
    world(data / "curated")
    a = load(export_site.run(docs_dir=tmp_path / "docs"))
    b = load(export_site.run(docs_dir=tmp_path / "docs"))
    a["meta"].pop("generated_at"); b["meta"].pop("generated_at")
    assert a == b


@pytest.mark.parametrize("status,rate,expected", [
    ("hotspot", 0.49, "high"), ("hotspot", 0.50, "critical"), ("hotspot", 1.0, "critical"),
    ("monitored", 0.10, "low"), ("monitored", 0.11, "moderate"), ("monitored", 0.0, "low"),
    ("insufficient", None, "none"), ("monitored", float("nan"), "none"),
])
def test_severity_class(status, rate, expected):
    assert export_site.severity_class(status, rate, 0.10) == expected


def test_severity_in_payload_and_legend(isolated_dirs, tmp_path):
    data, _ = isolated_dirs
    world(data / "curated")
    p = load(export_site.run(docs_dir=tmp_path / "docs"))
    sev = {s["site_key"]: s["severity"] for s in p["sites"]}
    assert sev == {"a": "critical", "b": "low", "c": "critical", "d": "none"}       # a: hotspot at 0.5
    assert [i["key"] for i in p["legend"]] == ["low", "moderate", "high", "critical", "none"]
    assert "10%" in p["legend"][0]["text"]                                          # boundary comes from business_rules.yaml
