"""Unit tests for ING-4 (no network: Open-Meteo is mocked with `responses`)."""
import json
import re

import pandas as pd
import pytest
import responses

from src.extract import weather
from src.utils.manifest import read_manifest

URL_RE = re.compile(r"https://archive-api\.open-meteo\.com/v1/archive.*")


def body(n_days: int) -> str:
    return json.dumps({
        "latitude": 14.5, "longitude": 121.0,
        "daily_units": {"time": "iso8601", "precipitation_sum": "mm", "temperature_2m_mean": "°C"},
        "daily": {
            "time": [f"d{i}" for i in range(n_days)],
            "precipitation_sum": [0.0] * n_days,
            "temperature_2m_mean": [25.0] * n_days,
        },
    })


@pytest.fixture
def sites_file(tmp_path):
    df = pd.DataFrame({
        "site_key": ["wqp:A", "wqp:B", "owq:C", "owq:D", "owq:E"],
        "source_code": ["wqp", "wqp", "owq_gemstat", "owq_gemstat", "owq_gemstat"],
        "latitude": [14.51, 14.49, -33.87, None, 0.0],          # A and B share a cell; D missing; E is (0, 0)
        "longitude": [121.02, 120.98, 151.21, 10.0, 0.0],
        "stratum": ["us", "us", "gemstat_asia", "gemstat_asia", "gemstat_africa"],
        "realm": ["freshwater"] * 5,
    })
    path = tmp_path / "sites.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr("src.extract.weather.time.sleep", lambda s: None)
    monkeypatch.setattr("src.utils.http.time.sleep", lambda s: None)


# ---------- pure helpers
@pytest.mark.parametrize("value,expected", [
    (14.51, 14.5), (14.63, 14.75), (-33.87, -33.75), (-33.88, -34.0),
    (0.125, 0.25), (-0.125, 0.0), (-0.1, 0.0), (179.99, 180.0),
])
def test_cell_centre_positive_negative_and_halves(value, expected):
    assert weather.cell_centre(value, 0.25) == expected


def test_cell_id_never_negative_zero():
    assert weather.make_cell_id(weather.cell_centre(-0.1, 0.25), 1.0) == "0.00_1.00"


def test_clean_and_dedupe_cells(sites_file):
    sites, dropped = weather.clean_sites(weather.load_sites(sites_file))
    assert dropped == 2                                     # missing coordinate + (0, 0)
    grid, site_cells = weather.assign_cells(sites, 0.25)
    assert grid["cell_id"].tolist() == ["-33.75_151.25", "14.50_121.00"]
    assert grid.set_index("cell_id").at["14.50_121.00", "n_sites"] == 2
    assert len(site_cells) == 3


def test_missing_contract_column(tmp_path):
    p = tmp_path / "bad.csv"
    pd.DataFrame({"site_key": ["x"], "latitude": [1.0]}).to_csv(p, index=False)
    with pytest.raises(weather.ConfigError):
        weather.load_sites(p)


def test_request_weight_matches_published_formula():
    assert weather.request_weight(4018, 2) == pytest.approx(4018 / 14 * 0.2)
    assert weather.request_weight(3, 2) == 1.0


# ---------- run()
@responses.activate
def test_plan_makes_no_calls(sites_file):
    path = weather.run(sites_file=str(sites_file), plan=True)
    assert path.name == "grid_cells.csv" and len(responses.calls) == 0


@responses.activate
def test_full_run_then_rerun_skips_everything(sites_file):
    n = weather.expected_days(2015, 2025)
    responses.add(responses.GET, URL_RE, body=body(n), status=200)
    m1 = weather.run(sites_file=str(sites_file))
    manifest = read_manifest(m1.parent)
    assert manifest["status"] == "success" and len(responses.calls) == 2
    assert manifest["warnings"] == []
    cell = next(f for f in manifest["files"] if f["name"].startswith("cell_"))
    assert cell["row_count"] == n

    weather.run(sites_file=str(sites_file))                 # rerun
    assert len(responses.calls) == 2                        # no new requests
    assert read_manifest(m1.parent)["status"] == "success"


@responses.activate
def test_short_response_records_warning_without_changing_file(sites_file):
    responses.add(responses.GET, URL_RE, body=body(10), status=200)
    m = weather.run(sites_file=str(sites_file))
    manifest = read_manifest(m.parent)
    assert any("expected" in w for w in manifest["warnings"])
    saved = next(m.parent.glob("cell_*.json"))
    assert saved.read_text(encoding="utf-8") == body(10)    # saved exactly as received


@responses.activate
def test_daily_budget_stops_cleanly_then_resumes(sites_file, isolated_dirs):
    _, cfg = isolated_dirs
    n = weather.expected_days(2015, 2025)
    responses.add(responses.GET, URL_RE, body=body(n), status=200)
    src = (cfg / "sources.yaml").read_text(encoding="utf-8")
    (cfg / "sources.yaml").write_text(src.replace("max_weighted_calls_per_day: 9000",
                                                  "max_weighted_calls_per_day: 60"), encoding="utf-8")
    m = weather.run(sites_file=str(sites_file))
    assert read_manifest(m.parent)["status"] == "partial" and len(responses.calls) == 1

    (cfg / "sources.yaml").write_text(src, encoding="utf-8")   # "next day": budget available again
    m = weather.run(sites_file=str(sites_file))
    assert read_manifest(m.parent)["status"] == "success" and len(responses.calls) == 2


def test_invalid_period_fails_before_anything(sites_file):
    with pytest.raises(weather.ConfigError):
        weather.run(sites_file=str(sites_file), start_year=1800)


def test_batch_id_independent_of_site_order(sites_file, tmp_path):
    df = pd.read_csv(sites_file)
    shuffled = tmp_path / "shuffled.csv"
    df.iloc[::-1].to_csv(shuffled, index=False)
    a = weather.run(sites_file=str(sites_file), plan=True).parent.name
    b = weather.run(sites_file=str(shuffled), plan=True).parent.name
    assert a == b
