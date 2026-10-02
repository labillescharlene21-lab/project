"""Unit tests for src/utils/sampling.py (no network)."""
import pandas as pd
import pytest

from src.utils.sampling import stratified_sample

KW = dict(stratum_col="stratum", realm_col="realm", take_all_if_fewer=True, min_sites_to_keep=10)


def make_sites(n_fresh=100, n_marine=5) -> pd.DataFrame:
    rows = [{"site_key": f"wqp:F{i:03d}", "stratum": "us", "realm": "freshwater"} for i in range(n_fresh)]
    rows += [{"site_key": f"wqp:M{i:03d}", "stratum": "us", "realm": "marine"} for i in range(n_marine)]
    return pd.DataFrame(rows)


def test_same_seed_same_input_identical():
    sites = make_sites()
    a, _ = stratified_sample(sites, k=10, seed=42, **KW)
    b, _ = stratified_sample(sites, k=10, seed=42, **KW)
    pd.testing.assert_frame_equal(a, b)


def test_input_row_order_does_not_matter():
    sites = make_sites()
    shuffled = sites.sample(frac=1, random_state=7).reset_index(drop=True)
    a, _ = stratified_sample(sites, k=10, seed=42, **KW)
    b, _ = stratified_sample(shuffled, k=10, seed=42, **KW)
    pd.testing.assert_frame_equal(a, b)


def test_different_seed_different_selection():
    sites = make_sites()
    a, _ = stratified_sample(sites, k=10, seed=1, **KW)
    b, _ = stratified_sample(sites, k=10, seed=2, **KW)
    assert set(a["site_key"]) != set(b["site_key"])


def test_group_smaller_than_k_takes_all():
    sites = make_sites(n_fresh=100, n_marine=5)
    sampled, summary = stratified_sample(sites, k=10, seed=42, **KW)
    assert (sampled["realm"] == "marine").sum() == 5
    row = summary[summary["realm"] == "marine"].iloc[0]
    assert (row["eligible"], row["sampled"]) == (5, 5)


def test_low_coverage_flag_uses_min_sites_to_keep():
    sites = make_sites(n_fresh=100, n_marine=5)
    _, summary = stratified_sample(sites, k=10, seed=42, **KW)
    flags = dict(zip(summary["realm"], summary["low_coverage"]))
    assert flags == {"freshwater": False, "marine": True}


def test_group_smaller_than_k_without_take_all_samples_none():
    sites = make_sites(n_fresh=100, n_marine=5)
    sampled, summary = stratified_sample(
        sites, k=10, seed=42, stratum_col="stratum", realm_col="realm",
        take_all_if_fewer=False, min_sites_to_keep=10,
    )
    assert (sampled["realm"] == "marine").sum() == 0
    row = summary[summary["realm"] == "marine"].iloc[0]
    assert row["sampled"] == 0 and bool(row["low_coverage"])


def test_k_caps_each_group():
    sampled, summary = stratified_sample(make_sites(), k=10, seed=42, **KW)
    assert (sampled["realm"] == "freshwater").sum() == 10
    assert summary["sampled"].sum() == len(sampled)


def test_changing_k_changes_output():
    sites = make_sites()
    a, _ = stratified_sample(sites, k=10, seed=42, **KW)
    b, _ = stratified_sample(sites, k=20, seed=42, **KW)
    assert len(a) != len(b)


def test_empty_input_returns_empty_frames():
    empty = make_sites().iloc[0:0]
    sampled, summary = stratified_sample(empty, k=10, seed=42, **KW)
    assert sampled.empty
    assert list(summary.columns) == ["stratum", "realm", "eligible", "sampled", "low_coverage"]


def test_invalid_inputs_raise():
    with pytest.raises(ValueError):
        stratified_sample(make_sites(), k=0, seed=42, **KW)
    with pytest.raises(ValueError):
        stratified_sample(make_sites().drop(columns=["site_key"]), k=10, seed=42, **KW)