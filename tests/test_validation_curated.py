"""VAL-3 tests.

Pure functions (reconciliation, ranks, hotspot share) are tested without a database.
Database tests apply DB-1's real DDL (sql/init/01_schema.sql) to a throwaway schema in Postgres and
are skipped automatically when no Postgres is configured (POSTGRES_HOST etc. unset or unreachable).
"""
import json
import os
import uuid
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from src.transform.sites import write_drop_log
from src.utils.manifest import file_entry, write_manifest
from src.validation import curated_checks as cc
from src.validation.core import DataQualityError

DDL = Path(__file__).resolve().parents[1] / "sql" / "init" / "01_schema.sql"


# ================================================================ pure functions
def drops_df(rows):
    return pd.DataFrame(rows, columns=["stage", "source_code", "reason", "row_count", "batch_id"])


def test_reconcile_balanced():
    drops = drops_df([("observations", "wqp", "unit_unmapped", 3, "b"), ("observations", "wqp", "qc_blank", 2, "b")])
    table, problems = cc.reconcile({"wqp": 15}, drops, {"wqp": 10}, {"wqp": 10})
    assert problems == []
    row = table.iloc[0]
    assert row["dropped_unit_unmapped"] == 3 and row["dropped_total"] == 5 and bool(row["staging_to_curated_ok"])


def test_reconcile_detects_both_kinds_of_mismatch():
    _, problems = cc.reconcile({"wqp": 15, "owq_eionet": 4}, drops_df([]), {"wqp": 14, "owq_eionet": 4},
                               {"wqp": 14, "owq_eionet": 3})
    assert any("wqp: raw 15" in p for p in problems)
    assert any("owq_eionet: staging has 4 rows, fact_observation has 3" in p for p in problems)


def regions(rows):
    return pd.DataFrame(rows, columns=["status", "priority_rank", "rank_in_country", "country_iso"])


def test_ranks_ok():
    assert cc.rank_problems(regions([("ranked", 1, 1, "US"), ("ranked", 2, 1, "MX"), ("ranked", 3, 2, "US"),
                                     ("insufficient_sites", None, None, "US")])) == []


def test_rank_gap_and_unranked_with_rank():
    p = cc.rank_problems(regions([("ranked", 1, 1, "US"), ("ranked", 3, 2, "US"),
                                  ("insufficient_sites", 4, None, "US")]))
    assert any("not 1..2" in x for x in p) and any("unranked" in x for x in p)


@pytest.mark.parametrize("n,h,bad", [(10, 3, False), (10, 0, True), (10, 10, True), (0, 0, True)])
def test_hotspot_share(n, h, bad):
    assert bool(cc.hotspot_share_problems(n, h)) is bad


# ================================================================ database tests
def pg_url():
    if not os.environ.get("POSTGRES_HOST"):
        return None
    return URL.create("postgresql+psycopg2", username=os.environ.get("POSTGRES_USER"),
                      password=os.environ.get("POSTGRES_PASSWORD"), host=os.environ["POSTGRES_HOST"],
                      port=int(os.environ.get("POSTGRES_PORT", "5432")), database=os.environ.get("POSTGRES_DB"))


@pytest.fixture
def db(monkeypatch):
    """A fresh schema with DB-1's DDL; dropped afterwards. Skips if Postgres isn't available."""
    url = pg_url()
    if url is None:
        pytest.skip("no Postgres configured (set POSTGRES_HOST/... or run inside Docker)")
    schema = "val3_" + uuid.uuid4().hex[:8]
    admin = create_engine(url)
    try:
        with admin.begin() as c:
            c.execute(text(f"CREATE SCHEMA {schema}"))
    except Exception as exc:  # unreachable server
        pytest.skip(f"Postgres not reachable: {exc}")
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    raw = engine.raw_connection()
    raw.cursor().execute(DDL.read_text(encoding="utf-8"))
    raw.commit()
    raw.close()
    monkeypatch.setenv("PGOPTIONS", f"-csearch_path={schema}")      # VAL-1's dq_results insert lands here too
    yield engine
    engine.dispose()
    with admin.begin() as c:
        c.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def load_world(engine, data: Path):
    """2 regions, 4 sites, 6 observations in Postgres + matching raw manifest, staging manifest and drop log."""
    stmts = """
    INSERT INTO etl_batch_log (batch_id, stage, status, rows_in, rows_out) VALUES ('load_1', 'load', 'success', 1, 1);
    INSERT INTO dim_source (source_code, source_name, provider, access_method, url)
      VALUES ('wqp', 'WQP', 'USGS', 'api', 'https://x'), ('owq_eionet', 'Eionet', 'EEA', 'export', 'https://y');
    INSERT INTO dim_indicator (indicator_code, indicator_name, realm, threshold_cfu_100ml, is_scored)
      VALUES ('e_coli', 'E. coli', 'freshwater', 410, true), ('enterococci', 'Enterococci', 'marine', 130, true);
    INSERT INTO dim_region (region_code, admin1_name, country_iso, country_name, continent)
      VALUES ('R1', 'One', 'US', 'United States', 'North America'), ('R2', 'Two', 'HU', 'Hungary', 'Europe');
    INSERT INTO dim_grid_cell (cell_id, cell_lat, cell_lon) VALUES ('1.00_2.00', 1, 2);
    INSERT INTO dim_site (site_key, source_code, source_site_id, region_code, cell_id, realm, stratum,
                          latitude, longitude, region_match)
      VALUES ('wqp:A', 'wqp', 'A', 'R1', '1.00_2.00', 'freshwater', 'us', 1, 2, 'within'),
             ('wqp:B', 'wqp', 'B', 'R1', '1.00_2.00', 'freshwater', 'us', 1, 2, 'within'),
             ('owq_eionet:C', 'owq_eionet', 'C', 'R2', '1.00_2.00', 'marine', 'europe', 1, 2, 'within'),
             ('owq_eionet:D', 'owq_eionet', 'D', 'R2', '1.00_2.00', 'marine', 'europe', 1, 2, 'within');
    INSERT INTO fact_observation (obs_key, site_key, indicator_code, source_record_id, sample_date,
                                  value_cfu_100ml, is_censored, rain_48h_mm, raw_batch_id, raw_file, load_batch_id)
      VALUES ('wqp:1', 'wqp:A', 'e_coli', '1', '2021-06-01', 500, false, 3, 'r', 'f', 'load_1'),
             ('wqp:2', 'wqp:A', 'e_coli', '2', '2021-06-02', 50, false, 0, 'r', 'f', 'load_1'),
             ('wqp:3', 'wqp:B', 'e_coli', '3', '2021-06-03', 10, false, 12, 'r', 'f', 'load_1'),
             ('owq_eionet:4', 'owq_eionet:C', 'enterococci', '4', '2019-07-01', 200, false, 1, 'r', 'f', 'load_1'),
             ('owq_eionet:5', 'owq_eionet:D', 'enterococci', '5', '2019-07-02', 20, false, 2, 'r', 'f', 'load_1'),
             ('owq_eionet:6', 'owq_eionet:D', 'enterococci', '6', '2019-07-03', 30, false, 4, 'r', 'f', 'load_1');
    INSERT INTO mart_site_hotspot (site_key, indicator_code, region_code, realm, n_observations, n_samples,
                                   years_monitored, insufficient_years, poor_years, is_persistent_hotspot,
                                   exceedance_rate, median_ratio_to_threshold, batch_id, load_batch_id)
      VALUES ('wqp:A', 'e_coli', 'R1', 'freshwater', 2, 2, 3, 0, 3, true, 0.5, 0.7, 'm', 'load_1'),
             ('wqp:B', 'e_coli', 'R1', 'freshwater', 1, 1, 3, 0, 0, false, 0, 0.02, 'm', 'load_1'),
             ('owq_eionet:C', 'enterococci', 'R2', 'marine', 1, 1, 3, 0, 3, true, 1, 1.5, 'm', 'load_1'),
             ('owq_eionet:D', 'enterococci', 'R2', 'marine', 2, 2, 3, 0, 0, false, 0, 0.2, 'm', 'load_1');
    INSERT INTO mart_region_priority (region_code, admin1_name, country_iso, country_name, status, n_sites,
                                      n_hotspots, priority_score, priority_rank, rank_in_country, batch_id, load_batch_id)
      VALUES ('R1', 'One', 'US', 'United States', 'ranked', 2, 1, 0.8, 1, 1, 'm', 'load_1'),
             ('R2', 'Two', 'HU', 'Hungary', 'ranked', 2, 1, 0.4, 2, 1, 'm', 'load_1');
    """
    with engine.begin() as c:
        for s in stmts.split(";"):
            if s.strip():
                c.execute(text(s))

    # raw batches STG-2 read: wqp 5 data rows (2 dropped), eionet 1003 rows (1000 not at sampled sites)
    for raw_code, batch, n in (("wqp_results", "wqp_results_b", 5), ("owq_eionet", "owq_eionet_b", 1003)):
        bdir = data / "raw" / raw_code / f"batch_id={batch}"
        bdir.mkdir(parents=True)
        name = "chunk_0001.csv" if raw_code == "wqp_results" else "owq_eionet_ecoli_2019_chunk0001.csv"
        p = bdir / name
        p.write_text("h\n" + "x\n" * n, encoding="utf-8")
        helper = bdir / "sampled_sites.csv"                     # not a data file: must not be counted
        helper.write_text("h\n1\n2\n", encoding="utf-8")
        write_manifest(bdir, source_code=raw_code, batch_id=batch, params={}, requests_log=[],
                       files=[file_entry(p, "csv", n), file_entry(helper, "csv", 2)], status="success")

    obs_dir = data / "staging" / "observations"
    obs_dir.mkdir(parents=True)
    (obs_dir / "_manifest.json").write_text(json.dumps({
        "batch_id": "staging_b", "params": {"inputs": {"wqp": "wqp_results_b", "owq_eionet": "owq_eionet_b"}},
        "partitions": [{"source_code": "wqp", "year": 2021, "rows": 3},
                       {"source_code": "owq_eionet", "year": 2019, "rows": 3}]}), encoding="utf-8")
    from collections import Counter
    write_drop_log({"wqp": Counter({"unit_unmapped": 2}), "owq_eionet": Counter({"site_not_sampled": 1000})},
                   "staging_b", stage="observations")


def by_name(results):
    return {r.check_name: r for r in results}


def test_clean_warehouse_passes_and_writes_outputs(db, isolated_dirs, monkeypatch, tmp_path):
    data, _ = isolated_dirs
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    load_world(db, data)
    results, table = cc.run_checks("cur_test", db)
    failed = [r for r in results if r.status == "fail"]
    assert not failed, failed
    assert "foreign keys checked, 0 orphans" in by_name(results)["referential_integrity"].details

    cc.validate_curated("cur_test", run_id="ok", engine=db)
    rec = pd.read_csv(tmp_path / "out" / "reconciliation.csv").set_index("source_code")
    assert rec.loc["wqp", "raw_rows"] == 5 and rec.loc["wqp", "dropped_unit_unmapped"] == 2
    assert rec.loc["owq_eionet", "curated_rows"] == 3
    with db.connect() as c:                                   # results also landed in dq_results
        assert c.execute(text("SELECT count(*) FROM dq_results WHERE stage = 'curated'")).scalar() == len(results)


def test_orphan_rows_fail(db, isolated_dirs):
    load_world(db, isolated_dirs[0])
    with db.begin() as c:
        c.execute(text("ALTER TABLE fact_observation DROP CONSTRAINT fk_fact_observation_site"))
        c.execute(text("INSERT INTO fact_observation (obs_key, site_key, indicator_code, source_record_id, "
                       "sample_date, is_censored, raw_batch_id, raw_file) "
                       "VALUES ('wqp:9', 'wqp:GHOST', 'e_coli', '9', '2021-01-01', false, 'r', 'f')"))
    r = by_name(cc.run_checks("t", db)[0])
    assert r["reconciliation"].status == "fail"              # the extra (orphan) row breaks reconciliation
    assert "wqp: staging has 3 rows, fact_observation has 4" in r["reconciliation"].details


def test_orphan_detected_through_existing_fk(db, isolated_dirs):
    load_world(db, isolated_dirs[0])
    with db.begin() as c:                                     # NOT VALID keeps the FK but skips existing rows
        c.execute(text("ALTER TABLE mart_site_hotspot DROP CONSTRAINT fk_mart_site_hotspot_site"))
        c.execute(text("UPDATE mart_site_hotspot SET site_key = 'wqp:GHOST' WHERE site_key = 'wqp:B'"))
        c.execute(text("ALTER TABLE mart_site_hotspot ADD CONSTRAINT fk_mart_site_hotspot_site "
                       "FOREIGN KEY (site_key) REFERENCES dim_site (site_key) NOT VALID"))
    r = by_name(cc.run_checks("t", db)[0])["referential_integrity"]
    assert r.status == "fail" and "fk_mart_site_hotspot_site" in r.details


def test_duplicate_key_detected_even_without_pk_constraint(db, isolated_dirs):
    load_world(db, isolated_dirs[0])
    with db.begin() as c:
        pk = c.execute(text("SELECT conname FROM pg_constraint WHERE conrelid = 'dim_grid_cell'::regclass "
                            "AND contype = 'p'")).scalar()
        c.execute(text(f'ALTER TABLE dim_grid_cell DROP CONSTRAINT "{pk}" CASCADE'))
        c.execute(text("INSERT INTO dim_grid_cell (cell_id, cell_lat, cell_lon) VALUES ('1.00_2.00', 1, 2)"))
    r = by_name(cc.run_checks("t", db)[0])["pk_unique_in_db"]
    assert r.status == "fail" and "dim_grid_cell" in r.details


def test_rank_gap_fails_and_raises(db, isolated_dirs):
    load_world(db, isolated_dirs[0])
    with db.begin() as c:
        c.execute(text("UPDATE mart_region_priority SET priority_rank = 3 WHERE region_code = 'R2'"))
    with pytest.raises(DataQualityError, match="mart_rank_consistency"):
        cc.validate_curated("t", run_id="bad", engine=db)


def test_low_weather_coverage_is_only_a_warning(db, isolated_dirs):
    load_world(db, isolated_dirs[0])
    with db.begin() as c:
        c.execute(text("UPDATE fact_observation SET rain_48h_mm = NULL WHERE site_key LIKE 'owq_eionet:%'"))
    r = by_name(cc.run_checks("t", db)[0])["weather_coverage"]
    assert r.status == "fail" and r.severity == "warning" and "owq_eionet 0%" in r.details
    cc.validate_curated("t", run_id="warn", engine=db)        # warnings never raise


def test_missing_tables_is_critical(db, isolated_dirs):
    with db.begin() as c:
        c.execute(text("DROP TABLE dq_results"))
    r = cc.run_checks("t", db)[0]
    assert r[0].check_name == "tables_present" and r[0].is_critical_failure
