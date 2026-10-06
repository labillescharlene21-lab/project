"""Tests for LOAD-1 (src/load/postgres.py).

Database tests run against the real Postgres service, inside a throwaway schema created
per test (the warehouse tables are never touched). Skipped when no database is reachable.
"""
import json
import uuid

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.load import init_db, postgres
from src.load.db import database_url
from src.utils.exceptions import ConfigError


# ---------------------------------------------------------------- fixtures

@pytest.fixture
def engine():
    """Engine whose search_path is a fresh schema with the warehouse DDL applied."""
    schema = f"test_load_{uuid.uuid4().hex[:8]}"
    try:
        admin = create_engine(database_url())
        with admin.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    except Exception as exc:
        pytest.skip(f"no database reachable: {exc}")
    eng = create_engine(database_url(), connect_args={"options": f"-csearch_path={schema}"})
    init_db.apply_schema(eng)
    yield eng
    eng.dispose()
    with admin.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    admin.dispose()


def write_curated(root, *, obs_value=10.0, extra_obs=False, hotspots=("wqp:1", "wqp:2")):
    """Tiny but valid curated layer: 2 sites, 2 observations, marts."""
    root.mkdir(parents=True, exist_ok=True)
    obs = {"obs_key": ["wqp:a", "wqp:b"], "site_key": ["wqp:1", "wqp:2"],
           "source_record_id": ["a", "b"], "value_cfu_100ml": [obs_value, 20.0],
           "sample_date": pd.to_datetime(["2020-06-01", "2020-06-02"]).date}
    if extra_obs:  # a valid new row next to a bad one: rollback must remove it too
        for col, value in [("obs_key", "wqp:c"), ("site_key", "wqp:1"), ("source_record_id", "c"),
                           ("value_cfu_100ml", 5.0), ("sample_date", pd.Timestamp("2020-06-03").date())]:
            obs[col] = list(obs[col]) + [value]
    tables = {
        "dim_source": pd.DataFrame({"source_code": ["wqp"], "source_name": ["Water Quality Portal"]}),
        "dim_indicator": pd.DataFrame({"indicator_code": ["e_coli"], "realm": ["freshwater"],
                                       "threshold_cfu_100ml": [410.0], "is_scored": [True]}),
        "dim_region": pd.DataFrame({"region_code": ["R1"], "admin1_name": ["Region 1"]}),
        "dim_grid_cell": pd.DataFrame({"cell_id": ["0.00_0.00"], "cell_lat": [0.0], "cell_lon": [0.0]}),
        "dim_site": pd.DataFrame({"site_key": ["wqp:1", "wqp:2"], "source_code": "wqp",
                                  "source_site_id": ["1", "2"], "region_code": "R1",
                                  "cell_id": "0.00_0.00", "realm": "freshwater",
                                  "latitude": [0.1, 0.2], "longitude": [0.1, 0.2],
                                  "region_match": "within"}),
        "fact_weather_daily": pd.DataFrame({"cell_id": "0.00_0.00", "precipitation_mm": [1.0, 0.0],
                                            "weather_date": pd.to_datetime(["2020-06-01", "2020-06-02"]).date}),
        "fact_observation": pd.DataFrame({**obs, "indicator_code": "e_coli", "is_censored": False,
                                          "raw_batch_id": "rb", "raw_file": "chunk_0001.csv"}),
        "mart_site_hotspot": pd.DataFrame({"site_key": list(hotspots), "indicator_code": "e_coli",
                                           "realm": "freshwater",
                                           "poor_years": [3.0] * len(hotspots)}),  # float -> smallint
        "mart_region_priority": pd.DataFrame({"region_code": ["R1"], "status": ["ranked"],
                                              "priority_rank": [1.0], "rank_in_country": [None]}),
    }
    for name, df in tables.items():
        df.to_parquet(root / f"{name}.parquet", index=False)
    (root / "_manifest.json").write_text(json.dumps({"batch_id": "curated_test"}), encoding="utf-8")


def counts(engine) -> dict:
    with engine.connect() as conn:
        return {t: conn.execute(text(f"SELECT count(*) FROM {t}")).scalar() for t in postgres.LOAD_ORDER}


# ---------------------------------------------------------------- database tests

def test_load_twice_same_counts_and_second_run_inserts_nothing(engine, tmp_path):
    write_curated(tmp_path)
    first = postgres.run_load("curated_test", "run_1", engine=engine, root=tmp_path)
    after_first = counts(engine)
    second = postgres.run_load("curated_test", "run_2", engine=engine, root=tmp_path)
    assert counts(engine) == after_first
    assert all(r.inserted == r.rows_in for r in first)
    assert sum(r.inserted for r in second) == 0
    assert all(r.updated == r.rows_in for r in second)


def test_forced_failure_rolls_back_the_whole_table(engine, tmp_path):
    write_curated(tmp_path)
    postgres.run_load("curated_test", "good", engine=engine, root=tmp_path)
    before = counts(engine)

    write_curated(tmp_path, obs_value=-1.0, extra_obs=True)   # bad row + a valid new row
    with pytest.raises(Exception, match="ck_fact_observation_value"):
        postgres.run_load("curated_test", "bad", engine=engine, root=tmp_path)

    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM fact_observation")).scalar() == before["fact_observation"]
        assert conn.execute(text("SELECT count(*) FROM fact_observation WHERE obs_key = 'wqp:c'")).scalar() == 0
        assert conn.execute(text("SELECT value_cfu_100ml FROM fact_observation WHERE obs_key = 'wqp:a'")).scalar() == 10
        log = conn.execute(text("SELECT status, params->>'table' FROM etl_batch_log "
                                "WHERE dag_run_id = 'bad' ORDER BY started_at")).all()
    assert tuple(log[-1]) == ("failed", "fact_observation")
    assert all(status == "success" for status, _ in log[:-1])   # earlier tables committed


def test_mart_rows_missing_from_new_data_are_deleted(engine, tmp_path):
    write_curated(tmp_path, hotspots=("wqp:1", "wqp:2"))
    postgres.run_load("curated_test", "run_1", engine=engine, root=tmp_path)
    write_curated(tmp_path, hotspots=("wqp:1",))
    results = {r.table: r for r in postgres.run_load("curated_test", "run_2", engine=engine, root=tmp_path)}
    assert results["mart_site_hotspot"].deleted == 1
    with engine.connect() as conn:
        assert conn.execute(text("SELECT site_key FROM mart_site_hotspot")).scalars().all() == ["wqp:1"]


def test_load_batch_id_links_to_the_batch_log(engine, tmp_path):
    write_curated(tmp_path)
    postgres.run_load("curated_test", "run_1", engine=engine, root=tmp_path)
    with engine.connect() as conn:
        orphans = conn.execute(text(
            "SELECT count(*) FROM fact_observation o LEFT JOIN etl_batch_log b ON b.batch_id = o.load_batch_id "
            "WHERE b.batch_id IS NULL OR b.params->>'table' <> 'fact_observation'")).scalar()
    assert orphans == 0


# ---------------------------------------------------------------- no database needed

def test_curated_ref_must_match_the_manifest(tmp_path):
    write_curated(tmp_path)
    with pytest.raises(ConfigError, match="does not match"):
        postgres.check_curated("curated_other", tmp_path)
    postgres.check_curated("curated_test", tmp_path)
    postgres.check_curated("manual", tmp_path)          # manual runs skip the check


def test_missing_curated_file_is_refused(tmp_path):
    write_curated(tmp_path)
    (tmp_path / "mart_region_priority.parquet").unlink()
    with pytest.raises(ConfigError, match="mart_region_priority"):
        postgres.check_curated("manual", tmp_path)


def test_to_copy_frame_converts_integers_and_fills_missing_columns():
    out = postgres.to_copy_frame(pd.DataFrame({"rank": [3.0, None]}), [("rank", "integer"), ("notes", "text")])
    assert list(out.columns) == ["rank", "notes"]
    assert str(out["rank"].dtype) == "Int64" and out["rank"].iloc[0] == 3 and pd.isna(out["rank"].iloc[1])
    assert out["notes"].isna().all()