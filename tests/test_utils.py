import csv
from pathlib import Path

import pytest
import responses
import yaml

from src.utils import config, http, manifest, paths
from src.utils.exceptions import ConfigError, SourceRequestError


@pytest.fixture
def config_dir(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "sampling.yaml").write_text(yaml.dump({"period": {"start_year": 2015}}))
    (cfg_dir / "sources.yaml").write_text(
        yaml.dump(
            {
                "http": {
                    "timeout_seconds": 45,
                    "max_retries": 3,
                    "backoff_factor": 2,
                    "retry_on_status": [429, 500, 502, 503, 504],
                }
            }
        )
    )
    monkeypatch.setenv("CONFIG_DIR", str(cfg_dir))
    return cfg_dir


# ---- config.validate_period ----

def test_validate_period_rejects_year_before_minimum(config_dir):
    with pytest.raises(ConfigError):
        config.validate_period(1800, 2020)


def test_validate_period_rejects_start_after_end(config_dir):
    with pytest.raises(ConfigError):
        config.validate_period(2022, 2020)


def test_validate_period_rejects_future_end_year(config_dir):
    with pytest.raises(ConfigError):
        config.validate_period(2020, 2999)


def test_validate_period_accepts_valid_range(config_dir):
    config.validate_period(2016, 2020)  # should not raise


# ---- config/paths: repo-relative fallbacks when env vars are unset ----

def test_data_dir_falls_back_to_project_root_when_unset(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PROJECT_ROOT", tmp_path)  # empty dir, no .env inside
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    assert paths.data_dir() == tmp_path / "data"


def test_load_yaml_falls_back_to_project_root_when_config_dir_unset(monkeypatch):
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    # Real repo config/sources.yaml must exist at PROJECT_ROOT/config for this to pass.
    data = config.load_yaml("sources")
    assert isinstance(data, dict)


# ---- config.get_env: .env loading, real env var precedence ----

def test_get_env_reads_from_dotenv(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PROJECT_ROOT", tmp_path)
    (tmp_path / ".env").write_text("MY_TEST_VAR=from_dotenv\n")
    monkeypatch.delenv("MY_TEST_VAR", raising=False)
    assert config.get_env("MY_TEST_VAR") == "from_dotenv"


def test_get_env_real_env_var_wins_over_dotenv(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PROJECT_ROOT", tmp_path)
    (tmp_path / ".env").write_text("MY_TEST_VAR=from_dotenv\n")
    monkeypatch.setenv("MY_TEST_VAR", "from_real_env")
    assert config.get_env("MY_TEST_VAR") == "from_real_env"


# ---- paths.raw_batch_dir: naming convention + path traversal guard ----

def test_raw_batch_dir_uses_batch_id_equals_prefix(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    result = paths.raw_batch_dir("x", "x_abc", create=False)
    assert str(result).endswith("raw/x/batch_id=x_abc") or str(result).endswith("raw\\x\\batch_id=x_abc")


def test_raw_batch_dir_rejects_path_traversal_in_source_code(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    with pytest.raises(ConfigError):
        paths.raw_batch_dir("../etc", "x", create=False)


def test_raw_batch_dir_rejects_path_traversal_in_batch_id(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    with pytest.raises(ConfigError):
        paths.raw_batch_dir("x", "../../etc", create=False)


# ---- manifest.make_batch_id ----

def test_make_batch_id_is_deterministic():
    assert manifest.make_batch_id("wqp", {"a": 1, "b": 2}) == manifest.make_batch_id("wqp", {"a": 1, "b": 2})


def test_make_batch_id_is_key_order_independent():
    assert manifest.make_batch_id("wqp", {"a": 1, "b": 2}) == manifest.make_batch_id("wqp", {"b": 2, "a": 1})


# ---- manifest.count_csv_rows ----

def test_count_csv_rows_with_quoted_newline(tmp_path):
    csv_path = tmp_path / "sample.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "notes"])
        writer.writerow(["1", "line one\nline two"])
        writer.writerow(["2", "simple"])
    assert manifest.count_csv_rows(csv_path) == 2


def test_count_csv_rows_skips_leading_comment_lines(tmp_path):
    csv_path = tmp_path / "owq_sample.csv"
    csv_path.write_text(
        "# comment 1\n# comment 2\n# comment 3\n"
        "date,value\n2015-01-01,10\n2015-01-02,20\n"
    )
    assert manifest.count_csv_rows(csv_path) == 2


# ---- manifest round-trip ----

def test_manifest_round_trip(tmp_path):
    batch_dir = tmp_path / "raw" / "wqp" / "batch1"
    data_file = tmp_path / "data.csv"
    data_file.write_text("id,value\n1,10\n2,20\n")
    entry = manifest.file_entry(data_file, "csv", 2)

    manifest.write_manifest(
        batch_dir,
        source_code="wqp",
        batch_id="batch1",
        params={"start_year": 2015},
        requests_log=[{"url": "https://example.com"}],
        files=[entry],
        status="success",
    )

    loaded = manifest.read_manifest(batch_dir)
    assert loaded["source_code"] == "wqp"
    assert loaded["total_rows"] == 2
    assert loaded["files"][0]["name"] == "data.csv"


# ---- manifest.file_is_complete ----

def test_file_is_complete_true(tmp_path):
    batch_dir = tmp_path / "raw" / "wqp" / "batch2"
    batch_dir.mkdir(parents=True)
    data_file = batch_dir / "data.csv"
    data_file.write_text("id,value\n1,10\n")
    entry = manifest.file_entry(data_file, "csv", 1)
    manifest.write_manifest(
        batch_dir, source_code="wqp", batch_id="batch2", params={},
        requests_log=[], files=[entry], status="success",
    )
    assert manifest.file_is_complete(batch_dir, "data.csv") is True


def test_file_is_complete_false_when_file_missing(tmp_path):
    batch_dir = tmp_path / "raw" / "wqp" / "batch3"
    batch_dir.mkdir(parents=True)
    assert manifest.file_is_complete(batch_dir, "data.csv") is False


def test_file_is_complete_false_when_checksum_mismatch(tmp_path):
    batch_dir = tmp_path / "raw" / "wqp" / "batch4"
    batch_dir.mkdir(parents=True)
    data_file = batch_dir / "data.csv"
    data_file.write_text("id,value\n1,10\n")
    entry = manifest.file_entry(data_file, "csv", 1)
    manifest.write_manifest(
        batch_dir, source_code="wqp", batch_id="batch4", params={},
        requests_log=[], files=[entry], status="success",
    )
    data_file.write_text("id,value\n1,999\n")
    assert manifest.file_is_complete(batch_dir, "data.csv") is False


# ---- http.build_session: reads max_retries from config ----

def test_build_session_uses_max_retries_from_config(config_dir):
    session = http.build_session()
    adapter = session.get_adapter("https://example.com")
    assert adapter.max_retries.total == 3


# ---- http.request retries ----

@responses.activate
def test_request_retries_on_503_then_succeeds(config_dir):
    url = "https://example.com/api"
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, status=200, body="ok")

    session = http.build_session()
    response = http.request(session, "GET", url)

    assert response.status_code == 200
    assert len(responses.calls) == 2


@responses.activate
def test_request_raises_after_exhausting_retries(config_dir):
    url = "https://example.com/api"
    for _ in range(5):
        responses.add(responses.GET, url, status=503)

    session = http.build_session()
    with pytest.raises(SourceRequestError):
        http.request(session, "GET", url)