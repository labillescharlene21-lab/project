"""VAL-2 tests. Staging is written with STG-2's real writer (write_partitions), so types and
folder layout match production; then single defects are planted one at a time."""
import json
from datetime import date

import pandas as pd
import pytest

from src.transform.staging import OBS_COLUMNS, OBS_DTYPES, write_partitions
from src.validation import staging_checks as sc
from src.validation.core import DataQualityError

SITE_DTYPES = {"site_key": "string", "source_code": "string", "source_site_id": "string", "site_name": "string",
               "source_region": "string", "water_body_type": "string", "realm": "string", "latitude": "float64",
               "longitude": "float64", "country_iso": "string", "continent": "string", "region_code": "string",
               "region_match": "string", "stratum": "string", "n_samples": "int32", "n_years": "int16",
               "is_eligible": "bool", "is_sampled": "bool"}


def site(key, stratum, realm, sampled=True, lat=10.0, lon=20.0):
    src = key.split(":")[0]
    return {"site_key": key, "source_code": src, "source_site_id": key.split(":", 1)[1], "site_name": None,
            "source_region": None, "water_body_type": "lake", "realm": realm, "latitude": lat, "longitude": lon,
            "country_iso": "US", "continent": "North America", "region_code": "R1", "region_match": "within",
            "stratum": stratum, "n_samples": 30, "n_years": 4, "is_eligible": True, "is_sampled": sampled}


def obs(i, site_key, ind="e_coli", d=date(2021, 6, 1), value=100.0, censor=None):
    src = site_key.split(":")[0]
    return {"obs_key": f"{src}:{i}", "source_code": src, "source_record_id": str(i), "site_key": site_key,
            "source_site_id": site_key.split(":", 1)[1], "activity_id": f"a{i}", "activity_type": "Sample-Routine",
            "indicator_code": ind, "sample_date": d, "year": d.year, "value_cfu_100ml": value,
            "original_value": str(value), "original_unit": "MPN/100 ml", "is_censored": censor is not None,
            "censor_direction": censor, "detection_limit": None, "result_status": "Accepted",
            "raw_batch_id": "b", "raw_file": "chunk_0001.csv"}


def write(data, observations, sites):
    o = pd.DataFrame(observations)[OBS_COLUMNS].astype(OBS_DTYPES)
    write_partitions(o, data / "staging" / "observations", {"batch_id": "staging_test"})
    s = pd.DataFrame(sites).astype(SITE_DTYPES)
    (data / "staging" / "sites").mkdir(parents=True, exist_ok=True)
    s.to_parquet(data / "staging" / "sites" / "sites.parquet", index=False)


def good_sites():
    # 10 sampled sites per realm for us/europe so stratum_coverage only flags the empty strata
    rows = [site(f"wqp:F{i}", "us", "freshwater") for i in range(10)] + \
           [site(f"wqp:M{i}", "us", "marine") for i in range(10)]
    rows.append(site("wqp:UNSAMPLED", None, None, sampled=False))
    return rows


def good_obs():
    return [obs(1, "wqp:F0"), obs(2, "wqp:M0", "enterococci", date(2019, 3, 2), 5.0, "<"),
            obs(3, "wqp:F1", "fecal_coliform", date(2025, 12, 31), 2000.0)]


@pytest.fixture
def data(isolated_dirs):
    return isolated_dirs[0]


def results(ref="t"):
    return {r.check_name: r for r in sc.run_checks(ref)}


# ---------------------------------------------------------------- happy path
def test_clean_staging_passes_all_critical_checks(data):
    write(data, good_obs(), good_sites())
    r = results()
    critical_fails = [x for x in r.values() if x.is_critical_failure]
    assert not critical_fails, critical_fails
    assert r["schema_matches"].status == "pass"             # hive year reads back as int32: accepted
    assert r["stratum_coverage"].status == "fail"            # europe + gemstat strata empty -> warning only
    sc.validate_staging("t", run_id="ok")                    # must not raise


# ---------------------------------------------------------------- required tests
def test_duplicate_obs_key_fails(data):
    rows = good_obs()
    rows.append({**obs(99, "wqp:F2", d=date(2020, 1, 1)), "obs_key": rows[0]["obs_key"]})   # same key, other partition
    write(data, rows, good_sites())
    assert results()["obs_key_unique"].status == "fail"


def test_bad_indicator_code_fails(data):
    write(data, good_obs() + [obs(9, "wqp:F2", ind="salmonella")], good_sites())
    r = results()["accepted_values"]
    assert r.status == "fail" and "salmonella" in r.details


def test_orphan_site_key_fails(data):
    write(data, good_obs() + [obs(9, "wqp:GHOST")], good_sites())
    r = results()["observation_site_exists"]
    assert r.status == "fail" and "wqp:GHOST" in r.details


# ---------------------------------------------------------------- other checks
def test_date_out_of_period_fails(data):
    write(data, good_obs() + [obs(9, "wqp:F2", d=date(2012, 5, 5))], good_sites())
    assert results()["date_in_period"].status == "fail"


def test_bad_censor_direction_and_unsampled_realm(data):
    rows = good_obs() + [obs(9, "wqp:F2", censor="~")]
    write(data, rows, good_sites())                          # unsampled site has null realm: allowed
    r = results()["accepted_values"]
    assert r.status == "fail" and "censor_direction" in r.details and "realm" not in r.details


def test_null_key_fails(data):
    rows = good_obs()
    rows[0]["indicator_code"] = None
    write(data, rows, good_sites())
    assert "indicator_code" in results()["keys_not_null"].details


def test_invalid_coordinates_fail(data):
    sites = good_sites()
    sites[0]["latitude"] = 95.0
    write(data, good_obs(), sites)
    assert results()["coordinates_valid"].status == "fail"


def test_huge_value_is_only_a_warning(data):
    write(data, good_obs() + [obs(9, "wqp:F2", value=5_000_000.0)], good_sites())
    r = results()["value_range"]
    assert r.status == "fail" and r.severity == "warning"
    sc.validate_staging("t", run_id="warn")                  # warnings never raise


def test_missing_column_fails_schema(data):
    write(data, good_obs(), good_sites())
    p = data / "staging" / "sites" / "sites.parquet"
    pd.read_parquet(p).drop(columns=["continent"]).to_parquet(p, index=False)
    r = results()["schema_matches"]
    assert r.status == "fail" and "sites.continent missing" in r.details


def test_no_staging_is_a_critical_failure(data):
    r = sc.run_checks("t")
    assert r[0].check_name == "staging_readable" and r[0].is_critical_failure


def test_critical_failure_raises_naming_check(data):
    write(data, good_obs() + [obs(9, "wqp:GHOST")], good_sites())
    with pytest.raises(DataQualityError, match="observation_site_exists"):
        sc.validate_staging("t", run_id="bad")


def test_report_and_manifest_ref(data, monkeypatch, tmp_path):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    write(data, good_obs(), good_sites())
    assert sc.staging_batch_id() == "staging_test"
    sc.validate_staging("staging_test", run_id="r1")
    rep = json.loads((tmp_path / "out" / "dq" / "dq_report_staging_r1.json").read_text())
    assert rep["checks"] == 9 and rep["failed_critical"] == 0
