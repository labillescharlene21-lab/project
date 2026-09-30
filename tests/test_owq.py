import json

import pytest
import responses

from src.extract import owq
from src.utils.exceptions import EmptyResponseError

ENDPOINT = "https://lume-inventory-api.evan-thomas-3d8.workers.dev/api/wq/export"


@pytest.fixture
def owq_config_dir(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir(exist_ok=True)
    (cfg_dir / "sampling.yaml").write_text(json.dumps({
        "period": {"start_year": 2015, "end_year": 2015}
    }))
    (cfg_dir / "sources.yaml").write_text(json.dumps({
        "owq": {
            "export_mode": "scripted",
            "export_endpoint": ENDPOINT,
            "row_cap": 100000,
            "sources": {"owq_gemstat": "GEMStat"},
            "indicators": {"total_coliform": "tc"},
            "loc_types": ["river/stream", "lake"],
        }
    }))
    monkeypatch.setenv("CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    return cfg_dir


def _mock_response(row_count, capped, body="id,value\n1,10\n"):
    return responses.Response(
        method="GET", url=ENDPOINT, body=body, status=200,
        headers={"X-Row-Count": str(row_count), "X-Row-Capped": "1" if capped else "0"},
        content_type="text/csv",
    )


@responses.activate
def test_run_success_uncapped(owq_config_dir):
    responses.add(_mock_response(500, capped=False))
    responses.add(_mock_response(500, capped=False, body='{"type": "FeatureCollection", "features": []}'))

    manifest_path = owq.run()

    assert manifest_path is not None
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    assert data["source_code"] == "owq_gemstat"
    assert data["status"] == "success"
    assert len(data["files"]) == 2


@responses.activate
def test_run_capped_response_splits_loc_types(owq_config_dir):
    responses.add(_mock_response(100000, capped=True))
    responses.add(_mock_response(400, capped=False))
    responses.add(_mock_response(300, capped=False))
    responses.add(_mock_response(200, capped=False, body='{"type": "FeatureCollection", "features": []}'))

    manifest_path = owq.run()
    data = json.loads(manifest_path.read_text())
    assert len(data["files"]) == 3


@responses.activate
def test_run_raises_on_empty_response_body(owq_config_dir):
    responses.add(responses.Response(
        method="GET", url=ENDPOINT, body="", status=200,
        headers={"X-Row-Count": "0", "X-Row-Capped": "0"},
        content_type="text/csv",
    ))
    with pytest.raises(EmptyResponseError):
        owq.run()


@responses.activate
def test_run_treats_zero_data_rows_as_expected_empty(owq_config_dir):
    header_only_body = (
        "# comment line 1\n# comment line 2\n# comment line 3\n"
        "# comment line 4\n# comment line 5\n# comment line 6\n"
        "date,indicator,value,unit,latitude,longitude,source,region,water_body\n"
    )
    responses.add(_mock_response(0, capped=False, body=header_only_body))
    responses.add(_mock_response(0, capped=False, body='{"type": "FeatureCollection", "features": []}'))

    manifest_path = owq.run()
    data = json.loads(manifest_path.read_text())
    assert data["status"] == "success"
    assert any("zero data rows" in w for w in data["warnings"])


@responses.activate
def test_run_is_resume_safe(owq_config_dir):
    responses.add(_mock_response(500, capped=False))
    responses.add(_mock_response(500, capped=False, body='{"type": "FeatureCollection", "features": []}'))

    owq.run()
    assert len(responses.calls) == 2

    owq.run()
    assert len(responses.calls) == 2  # no new calls made on second run


def test_zstandard_available():
    """zstandard must stay installed: some OWQ responses (e.g. GeoJSON) are zstd-compressed,
    and without this package requests/urllib3 pass the raw compressed bytes through silently
    instead of decompressing or erroring."""
    import zstandard  # noqa: F401 — import existing is the assertion