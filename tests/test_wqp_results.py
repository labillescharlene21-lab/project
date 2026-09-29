"""ING-3 unit tests. No network: WQP is mocked with `responses`."""
import io
import json
import zipfile

import pandas as pd
import pytest
import responses

from src.extract import wqp_results as wr
from src.utils.exceptions import ConfigError, EmptyResponseError
from src.utils.manifest import make_batch_id, read_manifest, write_manifest, file_entry

URL = "https://www.waterqualitydata.us/data/Result/search"
HEADER = "OrganizationIdentifier,MonitoringLocationIdentifier,ActivityStartDate,CharacteristicName,ResultMeasureValue,ResultMeasure/MeasureUnitCode\n"


def csv_body(n_rows: int, site="USGS-1") -> str:
    return HEADER + "".join(f"USGS,{site},2021-06-{(i % 28) + 1:02d},Escherichia coli,{10 * i},cfu/100ml\n"
                            for i in range(n_rows))


def zipped(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("resultphyschem.csv", text)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr("src.extract.wqp_results.time.sleep", lambda s: None)


@pytest.fixture
def sites_file(tmp_path):
    ids = [f"USGS-{i:03d}" for i in range(120, 0, -1)]            # 120 sites, unsorted on purpose
    p = tmp_path / "sites.csv"
    pd.DataFrame({"site_key": [f"wqp:{i}" for i in ids], "source_site_id": ids}).to_csv(p, index=False)
    return p


# ---------------------------------------------------------------- chunking
def test_chunks_are_sorted_deduplicated_and_deterministic():
    ids = ["C", "A", "B", "A", "D", "E"]
    assert wr.make_chunks(ids, 2) == [["A", "B"], ["C", "D"], ["E"]]
    assert wr.make_chunks(list(reversed(ids)), 2) == wr.make_chunks(ids, 2)


def test_missing_source_site_id_column(tmp_path):
    p = tmp_path / "bad.csv"
    pd.DataFrame({"site": ["x"]}).to_csv(p, index=False)
    with pytest.raises(ConfigError, match="source_site_id"):
        wr.load_site_ids(p)


# ---------------------------------------------------------------- request shape
@responses.activate
def test_request_uses_post_json_body_and_mm_dd_yyyy_dates(sites_file):
    responses.add(responses.POST, URL, body=zipped(csv_body(3)), status=200)
    wr.run(sites_file=str(sites_file), max_chunks=1)
    call = responses.calls[0]
    body = json.loads(call.request.body)
    assert len(body["siteid"]) == 50 and body["siteid"][0] == "USGS-001"
    assert len(body["characteristicName"]) == 4
    assert body["startDateLo"] == "01-01-2015" and body["startDateHi"] == "12-31-2025"
    assert "startDate" not in call.request.url                       # filters only in the body
    assert "mimeType=csv" in call.request.url and "sorted=no" in call.request.url


# ---------------------------------------------------------------- full run, resume
@responses.activate
def test_full_run_then_rerun_skips_everything(sites_file):
    responses.add(responses.POST, URL, body=zipped(csv_body(5)), status=200,
                  headers={"Total-Result-Count": "5"})
    m = wr.run(sites_file=str(sites_file))
    manifest = read_manifest(m.parent)
    assert manifest["status"] == "success" and len(responses.calls) == 3      # 120 sites / 50
    assert manifest["total_rows"] == 15 and manifest["warnings"] == []
    names = [f["name"] for f in manifest["files"]]
    assert "chunk_0001.zip" in names and "chunk_0003.csv" in names

    wr.run(sites_file=str(sites_file))
    assert len(responses.calls) == 3                                         # nothing re-downloaded


@responses.activate
def test_interrupted_run_resumes(sites_file):
    responses.add(responses.POST, URL, body=zipped(csv_body(2)), status=200)
    m = wr.run(sites_file=str(sites_file), max_chunks=1)                     # simulate stopping early
    assert read_manifest(m.parent)["status"] == "partial" and len(responses.calls) == 1
    wr.run(sites_file=str(sites_file))
    assert read_manifest(m.parent)["status"] == "success" and len(responses.calls) == 3


# ---------------------------------------------------------------- retries
@responses.activate
def test_503_then_success_is_retried(sites_file):
    responses.add(responses.POST, URL, status=503, body="busy")
    responses.add(responses.POST, URL, body=zipped(csv_body(4)), status=200)
    m = wr.run(sites_file=str(sites_file), max_chunks=1)
    assert len(responses.calls) >= 2
    assert read_manifest(m.parent)["total_rows"] == 4


# ---------------------------------------------------------------- empty chunks
@responses.activate
def test_some_empty_chunks_are_allowed_and_recorded(sites_file):
    responses.add(responses.POST, URL, body=zipped(csv_body(0)), status=200)
    responses.add(responses.POST, URL, body=zipped(csv_body(3)), status=200)
    responses.add(responses.POST, URL, body=zipped(csv_body(0)), status=200)
    m = wr.run(sites_file=str(sites_file))
    manifest = read_manifest(m.parent)
    assert manifest["status"] == "success" and manifest["total_rows"] == 3
    assert manifest["params"]["_empty_chunks"] == ["chunk_0001.csv", "chunk_0003.csv"]


@responses.activate
def test_all_empty_raises(sites_file):
    responses.add(responses.POST, URL, body=zipped(csv_body(0)), status=200)
    with pytest.raises(EmptyResponseError):
        wr.run(sites_file=str(sites_file))


# ---------------------------------------------------------------- response handling
@responses.activate
def test_plain_csv_response_and_warning_headers(sites_file):
    responses.add(responses.POST, URL, body=csv_body(2), status=200,
                  headers={"Warning": "299 WQP \"NWIS temporarily unavailable\"", "Total-Result-Count": "7"})
    m = wr.run(sites_file=str(sites_file), max_chunks=1)
    manifest = read_manifest(m.parent)
    assert not (m.parent / "chunk_0001.zip").exists() and (m.parent / "chunk_0001.csv").exists()
    assert any("NWIS temporarily unavailable" in w for w in manifest["warnings"])
    assert any("Total-Result-Count" in w for w in manifest["warnings"])


# ---------------------------------------------------------------- finding ING-2's output
def test_finds_latest_successful_ing2_batch(isolated_dirs):
    data, _ = isolated_dirs
    for i, status in enumerate(["success", "partial"]):
        d = data / "raw" / "wqp_summary" / f"b{i}"
        d.mkdir(parents=True)
        f = d / "sampled_sites.csv"
        f.write_text("source_site_id\nUSGS-1\n", encoding="utf-8")
        write_manifest(d, source_code="wqp_summary", batch_id=f"wqp_summary_b{i}", params={},
                       requests_log=[], files=[file_entry(f, "csv", 1)], status=status)
    path, batch_id = wr.find_latest_sites_file()
    assert batch_id == "wqp_summary_b0" and path.name == "sampled_sites.csv"


def test_no_ing2_batch_gives_clear_error():
    with pytest.raises(ConfigError, match="Run ING-2 first"):
        wr.find_latest_sites_file()


@responses.activate
def test_rows_outside_period_are_flagged(sites_file):
    text = HEADER + "USGS,USGS-1,2007-12-16,Escherichia coli,11000,MPN/100 ml\n" \
                  + "USGS,USGS-1,2021-06-01,Escherichia coli,50,MPN/100 ml\n"
    responses.add(responses.POST, URL, body=zipped(text), status=200)
    m = wr.run(sites_file=str(sites_file), max_chunks=1)
    w = read_manifest(m.parent)["warnings"]
    assert any("1 rows dated outside 2015-2025" in x and "2007-12-16" in x for x in w)


def test_request_format_change_gives_new_batch_id(sites_file):
    # guards against reusing downloads made before the date fix
    old = make_batch_id("wqp_results", {"x": 1})
    new = make_batch_id("wqp_results", {"x": 1, "request_format": "v2-dates-in-body"})
    assert old != new
