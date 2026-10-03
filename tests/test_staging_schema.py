"""STG-0: checks that the staging schema and mapping configs are well formed and consistent."""
from pathlib import Path

import pytest
import yaml

CONFIG = Path(__file__).resolve().parents[1] / "config"
ALLOWED_DTYPES = {"string", "float64", "int16", "int32", "bool", "date32"}


def load(name):
    return yaml.safe_load((CONFIG / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schema():
    return load("staging_schema.yaml")


def cols(schema, table):
    return [c["name"] for c in schema["tables"][table]["columns"]]


def test_every_column_is_well_formed(schema):
    for table, spec in schema["tables"].items():
        names = cols(schema, table)
        assert len(names) == len(set(names)), f"{table}: duplicate column names"
        for c in spec["columns"]:
            assert c["dtype"] in ALLOWED_DTYPES, f"{table}.{c['name']}: unknown dtype {c['dtype']}"
            assert isinstance(c["nullable"], bool) and c.get("description"), f"{table}.{c['name']}"
        for k in spec["primary_key"]:
            assert k in names, f"{table}: primary key {k} is not a column"


def test_primary_keys_are_not_nullable(schema):
    for table, spec in schema["tables"].items():
        by_name = {c["name"]: c for c in spec["columns"]}
        for k in spec["primary_key"]:
            assert by_name[k]["nullable"] is False, f"{table}.{k} is a key but nullable"


def test_partition_columns_exist(schema):
    obs = schema["tables"]["observations"]
    assert set(obs["partition_by"]) <= set(cols(schema, "observations"))


def test_sampled_sites_matches_ing4_contract(schema):
    assert cols(schema, "sampled_sites") == ["site_key", "source_code", "latitude", "longitude", "stratum", "realm"]
    assert set(cols(schema, "sampled_sites")) <= set(cols(schema, "sites"))


def test_indicator_codes_and_units(schema):
    m = load("mappings.yaml")
    codes = {code for source in m["indicator_names"].values() for code in source.values()}
    assert codes == {"e_coli", "enterococci", "fecal_coliform", "total_coliform"}
    for unit, factor in m["units"].items():
        assert unit == unit.lower().replace(" ", ""), f"unit key not normalized: {unit}"
        assert factor > 0


def test_raw_schemas_load():
    for p in (CONFIG / "schemas").glob("*.yaml"):
        spec = yaml.safe_load(p.read_text(encoding="utf-8"))
        assert "expected_format" in spec, p.name
