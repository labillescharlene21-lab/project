"""EDA report: runs on synthetic staging + curated data written with the real writers."""
from datetime import date

import numpy as np
import pandas as pd

from src.analysis import eda
from src.transform.staging import OBS_COLUMNS, OBS_DTYPES, write_partitions


def make_staging(data):
    rng = np.random.default_rng(0)
    rows = []
    for i in range(200):
        src = "wqp" if i % 2 else "owq_eionet"
        site = f"{src}:S{i % 6}"
        ind = "e_coli" if i % 3 else "enterococci"
        d = date(2016 + i % 6, 1 + i % 12, 1 + i % 27)
        v = float(rng.lognormal(4, 1.5))
        rows.append({"obs_key": f"{src}:{i}", "source_code": src, "source_record_id": str(i), "site_key": site,
                     "source_site_id": f"S{i % 6}", "activity_id": None, "activity_type": None, "indicator_code": ind,
                     "sample_date": d, "year": d.year, "value_cfu_100ml": v, "original_value": str(v),
                     "original_unit": "CFU/100mL", "is_censored": i % 10 == 0, "censor_direction": "<" if i % 10 == 0 else None,
                     "detection_limit": None, "result_status": None, "raw_batch_id": "b", "raw_file": "f"})
    obs = pd.DataFrame(rows)[OBS_COLUMNS].astype(OBS_DTYPES)
    write_partitions(obs, data / "staging" / "observations",
                     {"batch_id": "staging_t", "row_counts": {"wqp": {"rows_in": 120}, "owq_eionet": {"rows_in": 150}},
                      "unmapped_units": {"wqp": {"MPN": 7}}, "params": {"inputs": {}}})
    sites = pd.DataFrame([{"site_key": f"{s}:S{j}", "source_code": s, "stratum": "us" if s == "wqp" else "europe",
                           "realm": "freshwater" if j % 2 else "marine", "is_eligible": True, "is_sampled": True}
                          for s in ("wqp", "owq_eionet") for j in range(6)])
    (data / "staging" / "sites").mkdir(parents=True)
    sites.to_parquet(data / "staging" / "sites" / "sites.parquet", index=False)
    pd.DataFrame([{"stage": "observations", "source_code": "wqp", "reason": "unit_unmapped", "row_count": 7,
                   "batch_id": "staging_t"}]).to_parquet(data / "staging" / "_drop_log.parquet", index=False)
    return obs, sites


def test_report_tables_and_figures(isolated_dirs, tmp_path, monkeypatch):
    data, _ = isolated_dirs
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    make_staging(data)
    report = eda.run(docs_dir=tmp_path / "docs")
    text = report.read_text(encoding="utf-8")
    for heading in ("## 3. Volume by layer", "## 6. Values", "## 8. Data quality"):
        assert heading in text
    assert "200 harmonized observations" in text
    assert len(list((tmp_path / "docs" / "figures").glob("eda_*.png"))) == 4        # no curated data -> no hotspot figure
    assert (tmp_path / "out" / "eda" / "values.csv").exists()
    assert "## 10." not in text                                                   # hotspots only when marts exist


def test_with_curated_marts(isolated_dirs, tmp_path, monkeypatch):
    data, _ = isolated_dirs
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    obs, sites = make_staging(data)
    cur = data / "curated"
    cur.mkdir()
    pd.DataFrame({"site_key": sites["site_key"], "stratum": sites["stratum"]}).to_parquet(cur / "dim_site.parquet")
    pd.DataFrame({"site_key": sites["site_key"], "realm": sites["realm"], "is_persistent_hotspot": [True, False] * 6,
                  "median_ratio_to_threshold": 0.5, "exceedance_rate": 0.2}).to_parquet(cur / "mart_site_hotspot.parquet")
    fo = obs[["obs_key"]].assign(rain_48h_mm=np.where(np.arange(len(obs)) % 2, 5.0, np.nan),
                                 is_wet=pd.array([True, None] * (len(obs) // 2), dtype="boolean"))
    fo.to_parquet(cur / "fact_observation.parquet")
    text = eda.run(docs_dir=tmp_path / "docs").read_text(encoding="utf-8")
    assert "## 9. Weather coverage" in text and "## 10. Persistent hotspots" in text
    hs = pd.read_csv(tmp_path / "out" / "eda" / "hotspots.csv", encoding="utf-8-sig")
    assert set(hs["stratum"]) == {"us", "europe"} and hs["hotspots"].sum() == 6 and hs["sites"].sum() == 12


def test_values_table_threshold_share():
    obs = pd.DataFrame({"source_code": "wqp", "indicator_code": "e_coli",
                        "value_cfu_100ml": [100.0, 500.0, 1000.0, 50.0], "is_censored": False})
    t = eda.table_value_stats(obs, {"e_coli": 410.0})
    assert t.loc[0, "pct_above_threshold"] == 50.0 and t.loc[0, "median"] == 300.0
