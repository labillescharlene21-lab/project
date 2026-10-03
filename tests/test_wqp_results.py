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
    # 4 indicators, 5 spellings: enterococci has an _alt spelling that ING-2 also sampled with
    assert body["characteristicName"] == ["Enterococci", "Enterococcus", "Escherichia coli",
                                          "Fecal Coliform", "Total Coliform"]
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
    assert "_empty_chunks" not in manifest["params"]                  # params == batch_id inputs only
    empty = [r["file"] for r in manifest["requests_log"] if r["empty"]]
    assert empty == ["chunk_0001.csv", "chunk_0003.csv"]


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


@responses.activate
def test_zero_byte_response_is_recorded_as_empty_not_fatal(sites_file):
    responses.add(responses.POST, URL, body=b"", status=200)
    responses.add(responses.POST, URL, body=zipped(csv_body(2)), status=200)
    responses.add(responses.POST, URL, body=zipped(csv_body(2)), status=200)
    m = wr.run(sites_file=str(sites_file))
    manifest = read_manifest(m.parent)
    assert manifest["status"] == "success" and manifest["total_rows"] == 4
    assert next(f for f in manifest["files"] if f["name"] == "chunk_0001.csv")["row_count"] == 0


@responses.activate
def test_completed_chunk_is_not_requested_again(sites_file):
    """Pre-existing chunk 1 (valid in the manifest) must be skipped; only chunks 2-3 hit the API."""
    responses.add(responses.POST, URL, body=zipped(csv_body(2)), status=200)
    wr.run(sites_file=str(sites_file), max_chunks=1)
    n_first = len(responses.calls)
    wr.run(sites_file=str(sites_file))
    bodies = [json.loads(c.request.body)["siteid"][0] for c in responses.calls[n_first:]]
    assert bodies == ["USGS-051", "USGS-101"]                        # chunk 1 (USGS-001..050) skipped


@responses.activate
def test_ctrl_c_mid_run_then_rerun_finishes(sites_file, monkeypatch):
    """KeyboardInterrupt on the 2nd request leaves a partial manifest; the rerun skips chunk 1."""
    responses.add(responses.POST, URL, body=zipped(csv_body(2)), status=200)
    real_post = wr.post_with_retries
    calls = {"n": 0}

    def interrupting(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt
        return real_post(*a, **kw)

    monkeypatch.setattr(wr, "post_with_retries", interrupting)
    with pytest.raises(KeyboardInterrupt):
        wr.run(sites_file=str(sites_file))
    monkeypatch.setattr(wr, "post_with_retries", real_post)

    batch_dir = next((wr.data_dir() / "raw" / "wqp_results").iterdir())
    assert read_manifest(batch_dir)["status"] == "partial"
    before = len(responses.calls)
    m = wr.run(sites_file=str(sites_file))
    assert read_manifest(m.parent)["status"] == "success"
    assert len(responses.calls) - before == 2                        # chunks 2 and 3 only