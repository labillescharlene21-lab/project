"""Pure value, unit and censoring rules for STG-2.

No file or config access here: callers pass the mappings in, so every rule is
unit-tested in tests/test_harmonize.py.

Censoring (same rule as OWQ's harmonization, so both sources match):
  left-censored ('<x', 'Not Detected', '*Non-detect') -> value = limit / 2
  right-censored ('>x', 'Present Above Quantification Limit') -> value = limit
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass

# "<10", "< 1", ">>2420", "ND <1": optional ND, a direction, then the number
_PREFIXED = re.compile(r"^(?:nd\s*)?([<>])[<>]*\s*(\S+)$", re.IGNORECASE)
# "5,794.0": thousands separators only (never decimal commas)
_THOUSANDS = re.compile(r"^\d{1,3}(,\d{3})+(\.\d+)?$")


def to_number(text) -> float | None:
    """'15' -> 15.0, '5,794.0' -> 5794.0; anything else ('', 'NA', 'nan', 'inf') -> None."""
    if text is None:
        return None
    s = str(text).strip()
    if _THOUSANDS.match(s):
        s = s.replace(",", "")
    try:
        x = float(s)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def normalize_unit(unit) -> str:
    """mappings.yaml key format: lowercase, all whitespace removed ('MPN/100 ml' -> 'mpn/100ml')."""
    return re.sub(r"\s+", "", str(unit or "")).lower()


def unit_multiplier(unit, units_cfg: dict) -> float | None:
    """Multiplier to CFU/100 mL from mappings.yaml › units; None if the unit is unmapped."""
    m = units_cfg.get(normalize_unit(unit))
    return float(m) if m is not None else None


def pick_unit(result_unit, limit_unit, from_detection_limit: bool) -> str:
    """The unit that applies to a parsed value.

    A limit taken from the detection-limit column uses that column's unit; otherwise
    the result unit, falling back to the limit unit when the result unit is blank.
    """
    result_unit = str(result_unit or "").strip()
    limit_unit = str(limit_unit or "").strip()
    if from_detection_limit:
        return limit_unit or result_unit
    return result_unit or limit_unit


@dataclass(frozen=True)
class Parsed:
    value: float | None          # in the reported unit; None means the row is dropped
    is_censored: bool
    direction: str | None        # "<", ">" or None
    limit: float | None          # limit used for a censored value, in the reported unit
    from_detection_limit: bool   # True if the limit came from the detection-limit column
    reason: str | None           # drop reason when value is None


def _drop(reason: str) -> Parsed:
    return Parsed(None, False, None, None, False, reason)


def _censored(direction: str, limit: float, from_dl: bool) -> Parsed:
    if limit < 0:
        return _drop("negative_value")
    value = limit / 2 if direction == "<" else limit
    return Parsed(value, True, direction, limit, from_dl, None)


def parse_value(value_text, condition_text, detection_limit_text, censoring_cfg: dict) -> Parsed:
    """Parse one reported value (mappings.yaml › censoring). Value prefixes are checked first."""
    prefixes = censoring_cfg.get("value_prefixes") or {}
    conditions = censoring_cfg.get("wqp_detection_condition") or {}
    non_detect = {str(s).strip().lower() for s in censoring_cfg.get("non_detect_values") or []}
    raw = "" if value_text is None else str(value_text).strip()

    # 1. Direction prefix in the value text: "<10", "> 2419.6", ">>2420", "ND <1"
    m = _PREFIXED.match(raw)
    if m and m.group(1) in prefixes:
        limit = to_number(m.group(2))
        if limit is not None:
            return _censored(prefixes[m.group(1)], limit, from_dl=False)

    # 2. Detection condition column, or a non-detect wording in the value itself
    direction = conditions.get(str(condition_text or "").strip())
    if direction is None and raw.lower() in non_detect:
        direction = "<"
    if direction is not None:
        number = to_number(raw)
        if number is not None:
            return _censored(direction, number, from_dl=False)
        dl = to_number(detection_limit_text)
        if dl is not None:
            return _censored(direction, dl, from_dl=True)
        return _drop("non_numeric_value")

    # 3. Plain number
    number = to_number(raw)
    if number is None:
        return _drop("non_numeric_value")
    if number < 0:
        return _drop("negative_value")
    return Parsed(number, False, None, None, False, None)