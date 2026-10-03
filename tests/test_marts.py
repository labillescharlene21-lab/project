"""MART-1 tests on synthetic data whose correct answers are known in advance.

Freshwater E. coli threshold = 410. Exceeding sample-days use 1000, others use 10.
Default rules (config/business_rules.yaml): poor year if > 10% of sample-days exceed,
>= 5 sample-days per year, persistent = poor in >= 3 of the last 5 classified years,
>= 3 sites per region to be ranked, weights 0.40 / 0.30 / 0.15 / 0.15.
"""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from src.transform import marts
from src.utils.exceptions import ConfigError

HIGH, LOW = 1000.0, 10.0


def indicators():
    return pd.DataFrame({
        "indicator_code": ["e_coli", "enterococci", "total_coliform"],
        "realm": ["freshwater", "marine", None],
        "threshold_cfu_100ml": [410.0, 130.0, None],
        "is_scored": [True, True, False],
    })


def regions(codes=("R1", "R2", "R3"), country="PH"):
    return pd.DataFrame({"region_code": list(codes), "admin1_name": [f"Name {c}" for c in codes],
                         "country_iso": country, "country_name": "Philippines"})


def site_obs(site, years, days_per_year=10, indicator="e_coli", wet=None, start_obs=0):
    """years: {year: number of exceeding days}. wet: optional {year: list of is_wet per day}."""
    rows, n = [], start_obs
    for year, n_exceed in years.items():
        for d in range(days_per_year):
            n += 1
            rows.append({
                "obs_key": f"{site}:{n}", "site_key": site, "indicator_code": indicator,
                "sample_date": date(year, 1 + d % 12, 1 + d // 12),
                "value_cfu_100ml": HIGH if d < n_exceed else LOW,
                "is_wet": (wet[year][d] if wet and year in wet else pd.NA),
            })
    return rows


def tables(obs_rows, sites_rows, region_codes=("R1", "R2", "R3")):
    return {
        "fact_observation": pd.DataFrame(obs_rows),
        "dim_site": pd.DataFrame(sites_rows, columns=["site_key", "realm", "region_code"]),
        "dim_indicator": indicators(),
        "dim_region": regions(region_codes),
    }


@pytest.fixture
def rules():
    return marts.load_rules()


def run(t, rules):
    return marts.compute_marts(t, rules, "test-batch")


def site_row(site_mart, key):
    return site_mart.set_index("site_key").loc[key]


# ---------------------------------------------------------------- persistence rules
def test_poor_3_of_5_is_hotspot_and_2_of_5_is_not(rules):
    obs = site_obs("A", {2021: 2, 2022: 2, 2023: 2, 2024: 0, 2025: 0}) \
        + site_obs("B", {2021: 2, 2022: 2, 2023: 0, 2024: 0, 2025: 0})
    sm, _ = run(tables(obs, [("A", "freshwater", "R1"), ("B", "freshwater", "R1")]), rules)
    assert site_row(sm, "A")["poor_years"] == 3 and bool(site_row(sm, "A")["is_persistent_hotspot"])
    assert site_row(sm, "B")["poor_years"] == 2 and not bool(site_row(sm, "B")["is_persistent_hotspot"])


def test_exactly_10_percent_is_not_poor(rules):
    # 1 of 10 days = 10%, rule is "more than 10%"
    obs = site_obs("A", {2021: 1, 2022: 1, 2023: 1})
    sm, _ = run(tables(obs, [("A", "freshwater", "R1")]), rules)
    assert site_row(sm, "A")["poor_years"] == 0


def test_insufficient_year_is_ignored(rules):
    # 2023 has only 4 sample-days (< 5), all exceeding: must not count as poor or as monitored
    obs = site_obs("A", {2021: 2, 2022: 2, 2024: 0}) + site_obs("A", {2023: 4}, days_per_year=4, start_obs=100)
    sm, _ = run(tables(obs, [("A", "freshwater", "R1")]), rules)
    r = site_row(sm, "A")
    assert r["years_monitored"] == 3 and r["insufficient_years"] == 1 and r["poor_years"] == 2
    assert not bool(r["is_persistent_hotspot"])


def test_only_most_recent_window_counts(rules):
    # poor 2015-2017, good 2018-2022: the last 5 classified years are all good
    years = {y: (2 if y <= 2017 else 0) for y in range(2015, 2023)}
    sm, _ = run(tables(site_obs("A", years), [("A", "freshwater", "R1")]), rules)
    assert site_row(sm, "A")["poor_years"] == 0 and site_row(sm, "A")["years_monitored"] == 8


# ---------------------------------------------------------------- sample-days
def test_replicates_collapse_with_shifted_geometric_mean(rules):
    rows = [
        {"obs_key": "r1", "site_key": "A", "indicator_code": "e_coli", "sample_date": date(2021, 1, 1),
         "value_cfu_100ml": 10.0, "is_wet": pd.NA},
        {"obs_key": "r2", "site_key": "A", "indicator_code": "e_coli", "sample_date": date(2021, 1, 1),
         "value_cfu_100ml": 1000.0, "is_wet": pd.NA},
    ]
    days = marts.sample_days(pd.DataFrame(rows), pd.DataFrame([("A", "freshwater", "R1")],
                             columns=["site_key", "realm", "region_code"]), indicators())
    assert len(days) == 1
    assert days.loc[0, "value"] == pytest.approx(np.sqrt(11 * 1001) - 1)   # ≈ 103.9, below 410
    assert not days.loc[0, "exceeds"] and days.loc[0, "n_replicates"] == 2


def test_single_replicate_keeps_exact_value_and_zero_is_allowed():
    s = pd.Series([0.0])
    assert marts.shifted_geomean(pd.Series([250.0])) == pytest.approx(250.0)
    assert marts.shifted_geomean(s) == 0.0


def test_only_primary_indicator_is_scored(rules):
    # marine site: enterococci scored; its e_coli and total_coliform rows ignored
    obs = site_obs("M", {2021: 2, 2022: 2, 2023: 2}, indicator="enterococci") \
        + site_obs("M", {2021: 10, 2022: 10, 2023: 10}, indicator="e_coli", start_obs=500) \
        + site_obs("M", {2021: 10}, indicator="total_coliform", start_obs=900)
    sm, _ = run(tables(obs, [("M", "marine", "R1")]), rules)
    assert sm["indicator_code"].tolist() == ["enterococci"]
    assert site_row(sm, "M")["n_samples"] == 30


# ---------------------------------------------------------------- trend and rain
def test_trend_slope_positive_when_worsening_and_null_when_too_few_years(rules):
    obs = site_obs("W", {2021: 0, 2022: 2, 2023: 4, 2024: 6}) + site_obs("S", {2021: 2, 2022: 2}, start_obs=900)
    sm, _ = run(tables(obs, [("W", "freshwater", "R1"), ("S", "freshwater", "R1")]), rules)
    assert site_row(sm, "W")["trend_slope"] == pytest.approx(0.2)
    assert pd.isna(site_row(sm, "S")["trend_slope"])


def test_wet_dry_rates_ignore_unknown_weather(rules):
    # 10 days: days 0-1 exceed. Wet flags: day0 wet, day1 unknown, days 2-4 wet, days 5-9 dry
    wet = {2021: [True, pd.NA, True, True, True, False, False, False, False, False]}
    obs = site_obs("A", {2021: 2}, wet=wet)
    sm, _ = run(tables(obs, [("A", "freshwater", "R1")]), rules)
    r = site_row(sm, "A")
    assert r["wet_exceedance_rate"] == pytest.approx(1 / 4)    # 4 wet days, 1 exceeds
    assert r["dry_exceedance_rate"] == pytest.approx(0.0)      # 5 dry days, none exceed


# ---------------------------------------------------------------- regions
def three_region_setup():
    """R1: 3 sites, 2 hotspots. R2: 3 sites, 0 hotspots. R3: 2 sites (below min 3)."""
    hot = {2021: 3, 2022: 3, 2023: 3}
    cold = {2021: 0, 2022: 0, 2023: 0}
    spec = [("a1", "R1", hot), ("a2", "R1", hot), ("a3", "R1", cold),
            ("b1", "R2", cold), ("b2", "R2", cold), ("b3", "R2", cold),
            ("c1", "R3", hot), ("c2", "R3", hot)]
    obs, sites = [], []
    for i, (s, r, y) in enumerate(spec):
        obs += site_obs(s, y, start_obs=i * 1000)
        sites.append((s, "freshwater", r))
    return obs, sites


def test_region_below_min_sites_is_unranked(rules):
    _, rm = run(tables(*three_region_setup()), rules)
    r3 = rm.set_index("region_code").loc["R3"]
    assert r3["status"] == "insufficient_sites"
    assert pd.isna(r3["priority_rank"]) and pd.isna(r3["priority_score"])


def test_ranking_and_scores(rules):
    _, rm = run(tables(*three_region_setup()), rules)
    ranked = rm[rm["status"] == "ranked"].set_index("region_code")
    assert ranked.loc["R1", "priority_rank"] == 1 and ranked.loc["R2", "priority_rank"] == 2
    assert ranked.loc["R1", "persistence_raw"] == pytest.approx(2 / 3)
    assert ranked["priority_score"].between(0, 1).all()
    assert ranked.loc["R1", "rank_in_country"] == 1
    # no weather in this data: rain component missing -> scored 0 and noted
    assert "rain_sensitivity missing" in ranked.loc["R1", "notes"]


def test_ties_broken_by_hotspots_then_region_code(rules):
    cold = {2021: 0, 2022: 0, 2023: 0}
    obs, sites = [], []
    for i, (s, r) in enumerate([("x1", "RB"), ("x2", "RB"), ("x3", "RB"), ("y1", "RA"), ("y2", "RA"), ("y3", "RA")]):
        obs += site_obs(s, cold, start_obs=i * 1000)
        sites.append((s, "freshwater", r))
    _, rm = run(tables(obs, sites, region_codes=("RA", "RB")), rules)
    assert rm["region_code"].tolist() == ["RA", "RB"]          # identical scores -> alphabetical
    assert rm["priority_rank"].tolist() == [1, 2]


# ---------------------------------------------------------------- config and inputs
def test_weights_must_sum_to_one(isolated_dirs):
    _, cfg = isolated_dirs
    p = cfg / "business_rules.yaml"
    p.write_text(p.read_text(encoding="utf-8").replace("persistence: 0.40", "persistence: 0.50"), encoding="utf-8")
    with pytest.raises(ConfigError, match="sum to 1.0"):
        marts.load_rules()


def test_missing_input_column_fails_with_clear_message(rules):
    t = tables(site_obs("A", {2021: 0}), [("A", "freshwater", "R1")])
    t["fact_observation"] = t["fact_observation"].rename(columns={"value_cfu_100ml": "value"})
    with pytest.raises(ConfigError, match="value_cfu_100ml"):
        run(t, rules)


# ---------------------------------------------------------------- end to end
def test_build_marts_writes_outputs_and_is_deterministic(isolated_dirs, monkeypatch, tmp_path):
    data, _ = isolated_dirs
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "outputs"))
    cur = data / "curated"
    cur.mkdir()
    for name, df in tables(*three_region_setup()).items():
        df.to_parquet(cur / f"{name}.parquet", index=False)

    marts.build_marts("cur-test")
    first = pd.read_parquet(cur / "mart_region_priority.parquet")
    marts.build_marts("cur-test")
    second = pd.read_parquet(cur / "mart_region_priority.parquet")
    pd.testing.assert_frame_equal(first, second)

    ranking = pd.read_csv(tmp_path / "outputs" / "priority_ranking.csv")
    assert ranking["region_code"].tolist() == ["R1", "R2"]
    assert (cur / "mart_site_hotspot.parquet").exists()


def test_build_marts_without_curated_inputs_fails_clearly():
    with pytest.raises(ConfigError, match="run CUR-1"):
        marts.build_marts("nothing")
