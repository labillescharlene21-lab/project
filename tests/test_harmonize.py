"""Tests for STG-2's pure value/unit/censoring rules (src/transform/harmonize.py)."""
import pytest

from src.transform import harmonize as h
from src.utils import config as cfg

# Test config: same shape as mappings.yaml, plus "ND" to test the generic non-detect rule
CENSORING = {
    "value_prefixes": {"<": "<", ">": ">"},
    "wqp_detection_condition": {"Not Detected": "<", "Present Above Quantification Limit": ">"},
    "non_detect_values": ["ND", "*Non-detect"],
}
UNITS = {"cfu/100ml": 1.0, "mpn/100ml": 1.0, "cfu/ml": 100.0}


def parse(value, condition="", limit=""):
    return h.parse_value(value, condition, limit, CENSORING)


# ---------------------------------------------------------------- acceptance cases

def test_less_than_10_is_left_censored_half_the_limit():
    p = parse("<10")
    assert (p.value, p.is_censored, p.direction, p.limit) == (5.0, True, "<", 10.0)


def test_greater_than_is_right_censored_at_the_limit():
    p = parse(">2419.6")
    assert (p.value, p.is_censored, p.direction) == (2419.6, True, ">")


def test_nd_with_detection_limit_is_half_the_limit():
    p = parse("ND", limit="10")
    assert (p.value, p.is_censored, p.direction) == (5.0, True, "<")
    assert p.from_detection_limit


def test_unit_multiplier():
    assert h.unit_multiplier("CFU/100mL", UNITS) == 1.0
    assert h.unit_multiplier("MPN/100 ml", UNITS) == 1.0    # spaces and case ignored
    assert h.unit_multiplier("cfu/mL", UNITS) == 100.0      # per mL -> per 100 mL


def test_unmapped_unit_is_none_so_the_row_is_dropped():
    assert h.unit_multiplier("MPN", UNITS) is None
    assert h.unit_multiplier("hours", UNITS) is None
    assert h.unit_multiplier("", UNITS) is None


# ---------------------------------------------------------------- value text variants

@pytest.mark.parametrize("text, value, direction", [
    ("< 1", 0.5, "<"), ("<  2", 1.0, "<"), ("<1.0", 0.5, "<"), ("ND <1", 0.5, "<"),
    ("> 2419.2", 2419.2, ">"), (">>2420", 2420.0, ">"),
])
def test_prefix_variants_seen_in_wqp(text, value, direction):
    p = parse(text)
    assert (p.value, p.direction, p.is_censored) == (value, direction, True)


def test_plain_number_and_thousands_separator():
    assert parse("15").value == 15.0 and not parse("15").is_censored
    assert parse("5,794.0").value == 5794.0


@pytest.mark.parametrize("text", ["Not Reported", "N/A", "NA", "*LA", "", "nan", "inf"])
def test_non_numeric_is_dropped(text):
    p = parse(text)
    assert p.value is None and p.reason == "non_numeric_value"


def test_negative_value_is_dropped():
    assert parse("-3").reason == "negative_value"
    assert parse("<-3").reason == "negative_value"


# ---------------------------------------------------------------- detection condition column

def test_condition_not_detected_uses_detection_limit_when_value_empty():
    p = parse("", condition="Not Detected", limit="1")
    assert (p.value, p.direction, p.from_detection_limit) == (0.5, "<", True)


def test_condition_with_numeric_value_uses_the_value():
    p = parse("2419.6", condition="Present Above Quantification Limit")
    assert (p.value, p.direction, p.from_detection_limit) == (2419.6, ">", False)


def test_prefix_wins_over_condition():
    p = parse(">100", condition="Not Detected", limit="1")
    assert (p.value, p.direction) == (100.0, ">")


def test_censored_without_any_limit_is_dropped():
    assert parse("", condition="Not Detected").reason == "non_numeric_value"
    assert parse("ND").reason == "non_numeric_value"


# ---------------------------------------------------------------- units

@pytest.mark.parametrize("result_unit, limit_unit, from_dl, expected", [
    ("cfu/100mL", "MPN/100mL", False, "cfu/100mL"),   # result unit wins
    ("", "MPN/100mL", False, "MPN/100mL"),            # blank result unit -> limit unit
    ("cfu/100mL", "MPN/100mL", True, "MPN/100mL"),    # value from limit -> limit unit
    ("cfu/100mL", "", True, "cfu/100mL"),             # no limit unit -> result unit
])
def test_pick_unit(result_unit, limit_unit, from_dl, expected):
    assert h.pick_unit(result_unit, limit_unit, from_dl) == expected


def test_normalize_unit():
    assert h.normalize_unit(" MPN/100 mL ") == "mpn/100ml"
    assert h.normalize_unit(None) == ""


# ---------------------------------------------------------------- the real mappings.yaml

def test_real_mappings_handle_observed_wqp_values():
    m = cfg.load_yaml("mappings")
    p = h.parse_value("*Non-detect", "", "2", m["censoring"])
    assert (p.value, p.direction) == (1.0, "<")
    p = h.parse_value("", "Below Detection Limit", "10", m["censoring"])
    assert (p.value, p.direction) == (5.0, "<")
    assert h.unit_multiplier("#/100mL", m["units"]) == 1.0
    assert h.unit_multiplier("MPN", m["units"]) is None    # no volume: unmapped until the PM decides