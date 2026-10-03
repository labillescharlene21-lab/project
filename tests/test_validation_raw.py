"""VAL-1 tests: synthetic raw batches written with the real manifest helpers, then tampered with."""
import json

import pytest

from src.utils.manifest import count_csv_rows, file_entry, write_manifest
from src.validation import raw_checks
from src.validation.core import DataQualityError

WQP_HEADER = ("MonitoringLocationIdentifier,ActivityIdentifier,ActivityTypeCode,ActivityMediaName,ActivityStartDate,"
              "CharacteristicName,ResultSampleFractionText,ResultMeasureValue,ResultMeasure/MeasureUnitCode,"
              "ResultDetectionConditionText,DetectionQuantitationLimitMeasure/MeasureValue,"
              "DetectionQuantitationLimitMeasure/MeasureUnitCode,ResultStatusIdentifier,ProviderName")


def wqp_row(i):
    return f"USGS-1,act{i},Sample-Routine,Water,2021-06-0{i},Escherichia coli,,{10 * i},MPN/100 ml,,,,Accepted,NWIS"


def make_batch(data, source_code, files: dict[str, str], status="success", fmt=None):
    """files: name -> text. Row counts recorded with the real count_csv_rows."""
    bdir = data / "raw" / source_code / f"batch_id={source_code}_test"
    bdir.mkdir(parents=True)
    entries = []
    for name, text in files.items():
        p = bdir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        kind = name.rsplit(".", 1)[-1]
        entries.append(file_entry(p, kind, count_csv_rows(p) if kind == "csv" else None))
    return write_manifest(bdir, source_code=source_code, batch_id=f"{source_code}_test", params={},
                          requests_log=[], files=entries, status=status)


def by_name(results):
    return {r.check_name: r for r in results}


@pytest.fixture
def wqp(isolated_dirs):
    data, _ = isolated_dirs
    return make_batch(data, "wqp_results", {
        "chunk_0001.csv": WQP_HEADER + "\n" + "\n".join(wqp_row(i) for i in range(1, 4)) + "\n",
        "chunk_0002.zip": "PK-not-really-a-zip",
        "sampled_sites.csv": "site_key,source_site_id\nwqp:USGS-1,USGS-1\n",   # helper file: no column check
    })


# ---------------------------------------------------------------- happy path
def test_all_good_passes(wqp):
    results = raw_checks.run_checks(wqp)
    assert all(r.status == "pass" for r in results), [r for r in results if r.status == "fail"]
    raw_checks.validate_raw([str(wqp)], run_id="t1")          # must not raise


def test_helper_files_are_not_checked_for_data_columns(wqp):
    r = by_name(raw_checks.run_checks(wqp))["required_columns_present"]
    assert r.status == "pass"                                   # sampled_sites.csv lacks WQP columns, but is skipped


# ---------------------------------------------------------------- failures
def test_tampered_file_fails_checksum(wqp):
    p = wqp.parent / "chunk_0002.zip"
    p.write_text("tampered", encoding="utf-8")
    r = by_name(raw_checks.run_checks(wqp))["checksum_matches"]
    assert r.status == "fail" and "chunk_0002.zip" in r.details


def test_deleted_column_fails(wqp):
    p = wqp.parent / "chunk_0001.csv"
    lines = p.read_text(encoding="utf-8").splitlines()
    p.write_text("\n".join(",".join(l.split(",")[:-1]) for l in lines) + "\n", encoding="utf-8")   # drop ProviderName
    r = by_name(raw_checks.run_checks(wqp))["required_columns_present"]
    assert r.status == "fail" and "ProviderName" in r.details


def test_row_count_mismatch_fails(wqp):
    p = wqp.parent / "chunk_0001.csv"
    p.write_text(p.read_text(encoding="utf-8") + wqp_row(9) + "\n", encoding="utf-8")
    r = by_name(raw_checks.run_checks(wqp))["row_count_matches"]
    assert r.status == "fail" and "4 rows on disk, manifest says 3" in r.details


def test_missing_and_empty_files_fail(wqp):
    (wqp.parent / "chunk_0002.zip").unlink()
    (wqp.parent / "sampled_sites.csv").write_text("", encoding="utf-8")
    r = by_name(raw_checks.run_checks(wqp))["files_exist_non_empty"]
    assert r.status == "fail" and r.failed_count == 2


def test_partial_manifest_fails(isolated_dirs):
    data, _ = isolated_dirs
    m = make_batch(data, "wqp_results", {"chunk_0001.csv": WQP_HEADER + "\n" + wqp_row(1) + "\n"}, status="partial")
    assert by_name(raw_checks.run_checks(m))["manifest_status_success"].status == "fail"


def test_missing_manifest_fails(isolated_dirs):
    data, _ = isolated_dirs
    results = raw_checks.run_checks(data / "raw" / "nowhere" / "manifest.json")
    assert results[0].check_name == "manifest_readable" and results[0].status == "fail"


def test_critical_failure_raises_naming_the_check(wqp):
    (wqp.parent / "chunk_0002.zip").write_text("tampered", encoding="utf-8")
    with pytest.raises(DataQualityError, match="checksum_matches"):
        raw_checks.validate_raw([str(wqp)], run_id="t2")


# ---------------------------------------------------------------- source-specific
def test_owq_comment_lines_are_skipped(isolated_dirs):
    data, _ = isolated_dirs
    preamble = "".join(f"# attribution line {i}\n" for i in range(7))
    header = "date,indicator,value,unit,latitude,longitude,source,region,water_body\n"
    rows = "2015-01-02,E. coli,15,CFU/100mL,46.78,17.19,Eionet,Hungary,lake\n"
    m = make_batch(data, "owq_eionet", {
        "owq_eionet_ecoli_2015_chunk0001.csv": preamble + header + rows,
        "owq_eionet_fecal_coliform_2015_chunk0002.csv": preamble + header,     # expected-empty file
        "owq_eionet_sites.geojson": '{"type": "FeatureCollection", "features": []}',
    })
    results = raw_checks.run_checks(m)
    assert all(r.status == "pass" for r in results), [r for r in results if r.status == "fail"]


def test_open_meteo_json_shape(isolated_dirs):
    data, _ = isolated_dirs
    good = {"daily_units": {"time": "iso8601"},
            "daily": {"time": ["2021-01-01", "2021-01-02"], "precipitation_sum": [0.1, 0.0],
                      "temperature_2m_mean": [20.0, 21.0]}}
    bad = {"daily_units": {}, "daily": {"time": ["2021-01-01", "2021-01-02"], "precipitation_sum": [0.1],
                                        "temperature_2m_mean": [20.0, 21.0]}}
    m = make_batch(data, "open_meteo", {
        "grid_cells.csv": "cell_id,cell_lat,cell_lon,n_sites\n1.00_2.00,1.0,2.0,1\n",
        "cell_1.00_2.00.json": json.dumps(good),
        "cell_3.00_4.00.json": json.dumps(bad),
    })
    r = by_name(raw_checks.run_checks(m))["json_shape"]
    assert r.status == "fail" and r.failed_count == 1 and "precipitation_sum" in r.details


# ---------------------------------------------------------------- report
def test_report_written_with_summary(wqp, monkeypatch, tmp_path):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    raw_checks.validate_raw([str(wqp)], run_id="run:42")
    report = json.loads((tmp_path / "out" / "dq" / "dq_report_raw_run_42.json").read_text())
    assert report["checks"] == 5 and report["failed_critical"] == 0
    assert {r["check_name"] for r in report["results"]} >= {"checksum_matches", "row_count_matches"}


def test_cli_table(wqp, capsys, monkeypatch):
    monkeypatch.setattr("sys.argv", ["raw_checks", "--manifest", str(wqp)])
    raw_checks.main()
    out = capsys.readouterr().out
    assert "PASS" in out and "All critical checks passed." in out


def test_natural_earth_shapefile_fields(isolated_dirs):
    gpd = pytest.importorskip("geopandas")
    pytest.importorskip("pyogrio")
    from shapely.geometry import Point
    data, _ = isolated_dirs
    bdir = data / "raw" / "natural_earth" / "batch_id=ne_test"
    (bdir / "extracted").mkdir(parents=True)
    gpd.GeoDataFrame({"adm1_code": ["R1"], "name": ["One"], "iso_a2": ["US"], "adm0_a3": ["USA"]},
                     geometry=[Point(0, 0)], crs="EPSG:4326").to_file(bdir / "extracted" / "ne.shp")   # no "admin"
    entries = []
    for p in sorted((bdir / "extracted").iterdir()):
        e = file_entry(p, p.suffix.lstrip("."), None)
        e["name"] = f"extracted/{p.name}"
        entries.append(e)
    m = write_manifest(bdir, source_code="natural_earth", batch_id="ne_test", params={}, requests_log=[],
                       files=entries, status="success")
    r = by_name(raw_checks.run_checks(m))["required_columns_present"]
    assert r.status == "fail" and "admin" in r.details
