"""Tests for STG-1 (src/transform/sites.py). Synthetic data only: no network, no raw files."""
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from src.transform import sites as st
from src.utils.exceptions import ConfigError


# ---------------------------------------------------------------- helpers

def _regions(iso_a2="KE", adm0_a3="KEN"):
    """One square admin-1 region, 1 x 1 degree, at the equator."""
    return gpd.GeoDataFrame({"adm1_code": ["TST-1"], "iso_a2": [iso_a2], "adm0_a3": [adm0_a3]},
                            geometry=[box(0, 0, 1, 1)], crs="EPSG:4326")


def _point(lon, lat):
    return pd.DataFrame({"site_key": ["s1"], "longitude": [lon], "latitude": [lat]})


def _owq_rows(water_body, indicator, dates, lat="46.0", lon="17.0"):
    return pd.DataFrame({"date": dates, "indicator": indicator, "value": "10",
                         "unit": "CFU/100mL", "latitude": lat, "longitude": lon,
                         "source": "Eionet", "region": "Hungary", "water_body": water_body})


S_CFG = {
    "water_body_realm": {"freshwater": ["lake"], "marine": ["estuary"], "excluded": ["well"]},
    "realms": {"freshwater": {"primary_indicator": "e_coli"},
               "marine": {"primary_indicator": "enterococci"}},
    "eligibility": {"min_samples": 24, "min_distinct_years": 3},
    "sampling": {"take_all_if_fewer": True, "min_sites_to_keep": 2},
}
M_CFG = {"indicator_names": {"owq_eionet": {"E. coli": "e_coli", "Enterococci": "enterococci"}}}


# ---------------------------------------------------------------- spatial join

def test_point_inside_polygon_is_within():
    out = st.assign_regions(_point(0.5, 0.5), _regions())
    assert out.loc[0, "region_match"] == "within"
    assert out.loc[0, "region_code"] == "TST-1"
    assert out.loc[0, "country_iso"] == "KE"


def test_offshore_point_within_25km_is_nearest():
    # ~10 km east of the polygon edge (0.09 degrees of longitude at the equator)
    out = st.assign_regions(_point(1.09, 0.5), _regions())
    assert out.loc[0, "region_match"] == "nearest"
    assert out.loc[0, "region_code"] == "TST-1"


def test_far_point_is_none():
    # ~220 km east of the polygon edge
    out = st.assign_regions(_point(3.0, 0.5), _regions())
    assert out.loc[0, "region_match"] == "none"
    assert pd.isna(out.loc[0, "region_code"])
    assert pd.isna(out.loc[0, "country_iso"])


def test_iso_a2_minus_99_falls_back_to_adm0_a3():
    out = st.assign_regions(_point(0.5, 0.5), _regions(iso_a2="-99", adm0_a3="FRA"))
    assert out.loc[0, "country_iso"] == "FRA"


# ---------------------------------------------------------------- determinism + contract

def _pool(n=50):
    return pd.DataFrame({
        "site_key": [f"owq_eionet:{i:03d}" for i in range(n)],
        "source_code": "owq_eionet", "stratum": "europe",
        "realm": ["freshwater", "marine"] * (n // 2),
        "is_eligible": True, "latitude": 47.0, "longitude": 19.0,
    })


def _picked(df):
    return df[df["is_sampled"]].sort_values("site_key").reset_index(drop=True)


def test_same_inputs_and_seed_give_identical_sample():
    pool = _pool()
    a, _ = st.sample_owq(pool, S_CFG, k=5, seed=42)
    shuffled = pool.sample(frac=1, random_state=7)          # same sites, different row order
    b, _ = st.sample_owq(shuffled, S_CFG, k=5, seed=42)
    pd.testing.assert_frame_equal(_picked(a), _picked(b))
    assert len(_picked(a)) == 10                            # 5 per realm


def test_different_seed_gives_different_sample():
    pool = _pool()
    a, _ = st.sample_owq(pool, S_CFG, k=5, seed=42)
    c, _ = st.sample_owq(pool, S_CFG, k=5, seed=7)
    assert set(_picked(a)["site_key"]) != set(_picked(c)["site_key"])


def test_sampled_sites_has_exactly_the_ing4_columns():
    df = _pool(4).assign(is_sampled=True, extra="dropped")
    out = st.finalize_sampled(df)
    assert list(out.columns) == ["site_key", "source_code", "latitude", "longitude", "stratum", "realm"]


def test_duplicate_site_key_raises():
    df = _pool(4).assign(is_sampled=True)
    df.loc[1, "site_key"] = df.loc[0, "site_key"]
    with pytest.raises(ValueError, match="duplicate site_key"):
        st.finalize_sampled(df)


# ---------------------------------------------------------------- building blocks

def test_read_owq_csv_skips_comment_lines(tmp_path):
    path = tmp_path / "chunk.csv"
    path.write_text("# attribution line\n# another\n"
                    + ",".join(st.OWQ_COLUMNS) + "\n"
                    + "2015-01-02,E. coli,15,CFU/100mL,46.7874,17.1924,Eionet,Hungary,lake\n")
    df = st.read_owq_csv(path)
    assert list(df.columns) == st.OWQ_COLUMNS
    assert len(df) == 1


def test_owq_site_id_rounds_to_5_decimals():
    assert st.owq_site_id(46.7874, 17.1924) == "46.78740_17.19240"
    assert st.owq_site_id(-21.040004, 55.682301) == "-21.04000_55.68230"


def test_build_owq_sites_realm_and_eligibility():
    eligible_dates = [f"{2015 + i % 3}-01-{i // 3 + 1:02d}" for i in range(24)]  # 24 samples, 3 years
    rows = pd.concat([
        _owq_rows("lake", "E. coli", eligible_dates),                    # eligible freshwater site
        _owq_rows("lake", "Enterococci", ["2015-02-01"] * 5),            # wrong indicator: not counted
        _owq_rows("well", "E. coli", ["2015-01-01"], lat="10.0"),        # excluded water body
        _owq_rows("mystery", "E. coli", ["2015-01-01"], lat="20.0"),     # unmapped water body
    ], ignore_index=True)
    sites, drops = st.build_owq_sites(rows, "owq_eionet", S_CFG, M_CFG, 2015, 2025)
    assert len(sites) == 1
    site = sites.iloc[0]
    assert site["site_key"] == "owq_eionet:46.00000_17.00000"
    assert (site["realm"], site["n_samples"], site["n_years"]) == ("freshwater", 24, 3)
    assert bool(site["is_eligible"])
    assert drops["water_body_excluded"] == 1 and drops["water_body_unmapped"] == 1


@pytest.mark.parametrize("iso, expected", [
    ("HU", "Europe"), ("KE", "Africa"), ("BR", "South America"),
    ("FRA", "Europe"), ("ZZ", None), (None, None),
])
def test_continent_of(iso, expected):
    assert st.continent_of(iso, {}) == expected


def test_continent_override_wins():
    assert st.continent_of("XK", {"XK": "Europe"}) == "Europe"


STRATA = {
    "europe": {"source": "owq_eionet", "filter": {"continent": "Europe"}},
    "gemstat_latin_america": {"source": "owq_gemstat",
                              "filter": {"continent": ["South America", "North America"],
                                         "exclude_country": ["US", "CA"]}},
}


def test_assign_strata():
    sites = pd.DataFrame({
        "source_code": ["owq_eionet", "owq_gemstat", "owq_gemstat", "owq_gemstat"],
        "continent": ["Europe", "North America", "North America", "Asia"],
        "country_iso": ["HU", "MX", "US", "IN"],
    })
    assert st.assign_strata(sites, STRATA).tolist() == ["europe", "gemstat_latin_america", None, None]


def test_unknown_stratum_filter_key_is_a_config_error():
    sites = pd.DataFrame({"source_code": ["owq_eionet"], "continent": ["Europe"], "country_iso": ["HU"]})
    with pytest.raises(ConfigError):
        st.assign_strata(sites, {"europe": {"source": "owq_eionet", "filter": {"contnent": "Europe"}}})