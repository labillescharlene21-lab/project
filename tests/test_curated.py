"""CUR-1 tests on synthetic staging, weather and boundary inputs with known answers.

Setup: 3 sampled sites + 1 unsampled site.
  wqp:A  freshwater, region R1, cell C1      wqp:B  marine, region R1, cell C1
  owq_gemstat:C freshwater, region R2, cell C2 (cell C2 comes from a SECOND weather batch)
  wqp:Z  not sampled (its observations must not reach the curated layer)
Weather in cell C1: 2021-06-01 = 4 mm, 06-02 = 7 mm, 06-03 = 1 mm; 2021-06-05 missing.
"""
import json
from datetime import date

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from src.transform import curated
from src.utils.exceptions import ConfigError
from src.utils.manifest import file_entry, write_manifest


# ---------------------------------------------------------------- fixture builders
def obs_row(key, site, ind, d, value, source="wqp"):
    return {"obs_key": f"{source}:{key}", "source_code": source, "source_record_id": key, "site_key": site,
            "source_site_id": site.split(":", 1)[1], "activity_id": f"act-{key}", "activity_type": "Sample-Routine",
            "indicator_code": ind, "sample_date": d, "year": d.year, "value_cfu_100ml": value,
            "original_value": str(value), "original_unit": "MPN/100 ml", "is_censored": False,
            "censor_direction": None, "detection_limit": None, "result_status": "Accepted",
            "raw_batch_id": "wqp_results_x", "raw_file": "chunk_0001.csv"}


def write_staging(data):
    obs = pd.DataFrame([
        obs_row("1", "wqp:A", "e_coli", date(2021, 6, 2), 500.0),          # exceeds 410
        obs_row("2", "wqp:A", "e_coli", date(2021, 6, 3), 100.0),          # below
        obs_row("3", "wqp:A", "fecal_coliform", date(2021, 6, 3), 9000.0),  # supplementary -> null
        obs_row("4", "wqp:B", "enterococci", date(2021, 6, 2), 131.0),     # exceeds 130
        obs_row("5", "wqp:A", "e_coli", date(2021, 6, 6), 50.0),           # 06-05 missing -> 48h null
        obs_row("6", "owq_gemstat:C", "e_coli", date(2021, 6, 2), 10.0, source="owq_gemstat"),
        obs_row("7", "wqp:Z", "e_coli", date(2021, 6, 2), 10.0),           # unsampled site
    ])
    for (src, yr), part in obs.groupby(["source_code", "year"]):
        d = data / "staging" / "observations" / f"source_code={src}" / f"year={yr}"
        d.mkdir(parents=True)
        # partition columns removed from the file on purpose: the reader must restore them from the path
        part.drop(columns=["source_code", "year"]).to_parquet(d / "part-0000.parquet", index=False)

    sites = pd.DataFrame([
        ("wqp:A", "wqp", "A", "river/stream", "freshwater", "R1", "US", "North America", "us", True),
        ("wqp:B", "wqp", "B", "estuary", "marine", "R1", "US", "North America", "us", True),
        ("owq_gemstat:C", "owq_gemstat", "C", "lake", "freshwater", "R2", "KE", "Africa", "gemstat_africa", True),
        ("wqp:Z", "wqp", "Z", "lake", "freshwater", "R1", "US", "North America", "us", False),
    ], columns=["site_key", "source_code", "source_site_id", "water_body_type", "realm", "region_code",
                "country_iso", "continent", "stratum", "is_sampled"])
    sites["site_name"] = None
    sites["latitude"], sites["longitude"], sites["region_match"] = 1.0, 2.0, "within"
    (data / "staging" / "sites").mkdir(parents=True)
    sites.to_parquet(data / "staging" / "sites" / "sites.parquet", index=False)


def write_weather_batch(data, batch_id, cells, site_cells, days, retrieved_at):
    bdir = data / "raw" / "open_meteo" / f"batch_id={batch_id}"
    bdir.mkdir(parents=True)
    pd.DataFrame([{"cell_id": c, "cell_lat": 0.0, "cell_lon": 0.0, "n_sites": 1} for c in cells]) \
        .to_csv(bdir / "grid_cells.csv", index=False)
    pd.DataFrame(site_cells, columns=["site_key", "cell_id"]).to_csv(bdir / "site_cells.csv", index=False)
    files = []
    for c in cells:
        body = {"daily": {"time": [d for d, _, _ in days],
                          "precipitation_sum": [p for _, p, _ in days],
                          "temperature_2m_mean": [t for _, _, t in days]}}
        p = bdir / f"cell_{c}.json"
        p.write_text(json.dumps(body), encoding="utf-8")
        files.append(file_entry(p, "json", len(days)))
    m = write_manifest(bdir, source_code="open_meteo", batch_id=batch_id, params={}, requests_log=[],
                       files=files, status="success")
    data_m = json.loads(m.read_text(encoding="utf-8"))
    data_m["retrieved_at_utc"] = retrieved_at
    m.write_text(json.dumps(data_m), encoding="utf-8")


def write_boundaries(data):
    bdir = data / "raw" / "natural_earth" / "batch_id=ne_1" / "extracted"
    bdir.mkdir(parents=True)
    gdf = gpd.GeoDataFrame({"adm1_code": ["R1", "R2"], "name": ["Region One", "Region Two"],
                            "iso_a2": ["US", "KE"], "adm0_a3": ["USA", "KEN"],
                            "admin": ["United States of America", "Kenya"]},
                           geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326")
    gdf.to_file(bdir / "ne.shp")
    write_manifest(bdir.parent, source_code="natural_earth", batch_id="ne_1", params={}, requests_log=[],
                   files=[], status="success")


@pytest.fixture
def world(isolated_dirs, tmp_path, monkeypatch):
    data, cfg = isolated_dirs
    import shutil
    from pathlib import Path
    repo_cfg = Path(__file__).resolve().parents[1] / "config"
    for name in ("business_rules.yaml", "source_catalog.yaml"):
        if not (cfg / name).exists():
            shutil.copy(repo_cfg / name, cfg / name)
    write_staging(data)
    c1_days = [("2021-06-01", 4.0, 20.0), ("2021-06-02", 7.0, 21.0), ("2021-06-03", 1.0, 22.0),
               ("2021-06-04", 0.0, 23.0), ("2021-06-06", 0.0, 24.0)]               # 06-05 missing
    write_weather_batch(data, "om_us", ["C1"], [("wqp:A", "C1"), ("wqp:B", "C1")], c1_days, "2026-10-01T00:00:00Z")
    c2_days = [("2021-06-01", 30.0, 18.0), ("2021-06-02", 0.0, 19.0)]
    write_weather_batch(data, "om_rest", ["C2"], [("owq_gemstat:C", "C2")], c2_days, "2026-10-02T00:00:00Z")
    write_boundaries(data)
    return data


def read(data, name):
    return pd.read_parquet(data / "curated" / f"{name}.parquet")


# ---------------------------------------------------------------- tests
def test_builds_all_tables_and_drops_unsampled_site(world):
    curated.build_curated("stg-test")
    fo = read(world, "fact_observation")
    assert len(fo) == 6 and "wqp:Z" not in set(fo["site_key"])
    assert set(read(world, "dim_site")["site_key"]) == {"wqp:A", "wqp:B", "owq_gemstat:C"}
    manifest = json.loads((world / "curated" / "_manifest.json").read_text())
    assert manifest["drop_counts"]["observation_site_not_sampled"] == 1


def test_antecedent_rain_sums_and_missing_day(world):
    curated.build_curated("stg-test")
    fo = read(world, "fact_observation").set_index("obs_key")
    assert fo.loc["wqp:1", "rain_48h_mm"] == pytest.approx(11.0)   # 06-02 (7) + 06-01 (4)
    assert pd.isna(fo.loc["wqp:1", "rain_72h_mm"])                  # 05-31 missing -> null, not partial
    assert fo.loc["wqp:2", "rain_72h_mm"] == pytest.approx(12.0)    # 1 + 7 + 4
    assert fo.loc["wqp:2", "temp_mean_c"] == pytest.approx(22.0)
    assert pd.isna(fo.loc["wqp:5", "rain_48h_mm"])                  # 06-05 missing
    assert pd.isna(fo.loc["wqp:5", "is_wet"])                       # unknown, not dry


def test_wet_flag_uses_business_rule_threshold(world):
    curated.build_curated("stg-test")
    fo = read(world, "fact_observation").set_index("obs_key")
    assert bool(fo.loc["wqp:1", "is_wet"]) is True                  # 11 mm >= 10
    assert bool(fo.loc["wqp:2", "is_wet"]) is False                 # 8 mm  < 10


def test_exceedance_by_indicator_threshold(world):
    curated.build_curated("stg-test")
    fo = read(world, "fact_observation").set_index("obs_key")
    assert bool(fo.loc["wqp:1", "exceeds_threshold"]) is True       # 500 > 410
    assert bool(fo.loc["wqp:2", "exceeds_threshold"]) is False      # 100
    assert bool(fo.loc["wqp:4", "exceeds_threshold"]) is True       # enterococci 131 > 130
    assert pd.isna(fo.loc["wqp:3", "exceeds_threshold"])            # supplementary indicator


def test_weather_from_two_batches_is_combined(world):
    curated.build_curated("stg-test")
    fo = read(world, "fact_observation").set_index("obs_key")
    assert fo.loc["owq_gemstat:6", "rain_48h_mm"] == pytest.approx(30.0)   # C2 from the second batch
    assert set(read(world, "dim_grid_cell")["cell_id"]) == {"C1", "C2"}
    assert len(read(world, "fact_weather_daily")) == 7


def test_dimensions_content(world):
    curated.build_curated("stg-test")
    ind = read(world, "dim_indicator").set_index("indicator_code")
    assert ind.loc["e_coli", "threshold_cfu_100ml"] == 410 and bool(ind.loc["e_coli", "is_scored"])
    assert not bool(ind.loc["fecal_coliform", "is_scored"])
    reg = read(world, "dim_region").set_index("region_code")
    assert reg.loc["R2", "admin1_name"] == "Region Two" and reg.loc["R2", "continent"] == "Africa"
    assert set(read(world, "dim_source")["source_code"]) == {"wqp", "owq_gemstat"}
    assert read(world, "dim_site").set_index("site_key").loc["owq_gemstat:C", "cell_id"] == "C2"


def test_rerun_is_identical(world):
    a = curated.build_curated("stg-test")
    first = read(world, "fact_observation")
    b = curated.build_curated("stg-test")
    assert a == b
    pd.testing.assert_frame_equal(first, read(world, "fact_observation"))


def test_missing_staging_column_fails_clearly(world):
    p = next((world / "staging" / "observations").rglob("*.parquet"))
    pd.read_parquet(p).drop(columns=["value_cfu_100ml"]).to_parquet(p, index=False)
    with pytest.raises(ConfigError, match="value_cfu_100ml"):
        curated.build_curated("stg-test")


def test_no_staging_gives_clear_error(isolated_dirs):
    with pytest.raises(ConfigError, match="STG-2"):
        curated.build_curated("x")


def test_output_feeds_mart1(world):
    """Contract check: MART-1 accepts CUR-1's tables as they are."""
    marts = pytest.importorskip("src.transform.marts")
    curated.build_curated("stg-test")
    tables = {n: read(world, n) for n in ("fact_observation", "dim_site", "dim_indicator", "dim_region")}
    marts.check_inputs(tables)          # raises if any column MART-1 needs is missing
