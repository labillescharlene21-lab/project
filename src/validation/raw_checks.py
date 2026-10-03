"""VAL-1: raw-layer checks. Run on every extractor's manifest before staging reads the data.

Checks (one function each, all critical):
    manifest_status_success   manifest status == "success"
    files_exist_non_empty     every listed file exists and is not empty
    checksum_matches          SHA-256 on disk == manifest
    row_count_matches         CSV data rows (comment lines and header excluded) == manifest row_count
    required_columns_present  data files' header ⊇ config/schemas/{source_code}.yaml › required_columns
    json_shape                Open-Meteo JSON: required keys present, every daily series as long as daily.time

The schema file's `data_files` glob decides which files the column/shape checks apply to, so helper
files (sampled_sites.csv, grid_cells.csv, ...) are not checked against the source's data columns.

Run:
    python -m src.validation.raw_checks --manifest data/raw/wqp_results/<batch>/manifest.json [--manifest ...]
    python -m src.validation.raw_checks --all          # every manifest under data/raw
"""
from __future__ import annotations

import argparse
import csv
import fnmatch
import json
import sys
from pathlib import Path

from src.utils.config import load_yaml
from src.utils.exceptions import ConfigError
from src.utils.manifest import count_csv_rows, sha256_file
from src.utils.paths import data_dir
from src.validation.core import (
    CheckResult, DataQualityError, format_table, make_result, raise_on_critical, write_report,
)

STAGE = "raw"


# ---------------------------------------------------------------- helpers
def _schema(source_code: str) -> dict | None:
    try:
        return load_yaml(f"schemas/{source_code}")
    except ConfigError:
        return None


def _data_files(manifest: dict, schema: dict | None) -> list[str]:
    pattern = (schema or {}).get("data_files")
    names = [f["name"] for f in manifest.get("files", [])]
    return [n for n in names if fnmatch.fnmatch(n, pattern)] if pattern else names


def _csv_header(path: Path, comment_prefix: str = "#") -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.reader(line for line in f if not line.startswith(comment_prefix)):
            return [c.strip() for c in row]
    return []


# ---------------------------------------------------------------- checks
def check_manifest_status(m: dict, batch_dir: Path) -> CheckResult:
    problems = [] if m.get("status") == "success" else [f"status is {m.get('status')!r}, expected 'success'"]
    return make_result("manifest_status_success", STAGE, m.get("source_code", "?"), m.get("batch_id", "?"),
                       "critical", problems)


def check_files_exist(m: dict, batch_dir: Path) -> CheckResult:
    problems = []
    for f in m.get("files", []):
        p = batch_dir / f["name"]
        if not p.exists():
            problems.append(f"{f['name']} missing")
        elif p.stat().st_size == 0:
            problems.append(f"{f['name']} is empty (0 bytes)")
    return make_result("files_exist_non_empty", STAGE, m["source_code"], m["batch_id"], "critical", problems)


def check_checksums(m: dict, batch_dir: Path) -> CheckResult:
    problems = []
    for f in m.get("files", []):
        p = batch_dir / f["name"]
        if p.exists() and f.get("sha256") and sha256_file(p) != f["sha256"]:
            problems.append(f"{f['name']} checksum differs from manifest")
    return make_result("checksum_matches", STAGE, m["source_code"], m["batch_id"], "critical", problems)


def check_row_counts(m: dict, batch_dir: Path) -> CheckResult:
    problems = []
    for f in m.get("files", []):
        p = batch_dir / f["name"]
        if f["name"].lower().endswith(".csv") and f.get("row_count") is not None and p.exists():
            actual = count_csv_rows(p)
            if actual != f["row_count"]:
                problems.append(f"{f['name']}: {actual} rows on disk, manifest says {f['row_count']}")
    return make_result("row_count_matches", STAGE, m["source_code"], m["batch_id"], "critical", problems)


def check_required_columns(m: dict, batch_dir: Path) -> CheckResult:
    schema = _schema(m["source_code"])
    required = (schema or {}).get("required_columns") or []
    if schema is None or not required:
        return make_result("required_columns_present", STAGE, m["source_code"], m["batch_id"], "warning",
                           [] if schema is not None else [f"no config/schemas/{m['source_code']}.yaml"])
    prefix = schema.get("comment_prefix", "#")
    problems = []
    for name in _data_files(m, schema):
        p = batch_dir / name
        if not p.exists():
            continue                                   # reported by files_exist_non_empty
        if name.lower().endswith(".csv"):
            columns = set(_csv_header(p, prefix))
        elif name.lower().endswith(".shp"):
            try:
                import pyogrio                         # heavy; only for shapefiles
                columns = set(pyogrio.read_info(p)["fields"])
            except ImportError:
                problems.append(f"{name}: pyogrio not installed, cannot read shapefile fields")
                continue
        else:
            continue
        missing = [c for c in required if c not in columns]
        if missing:
            problems.append(f"{name} missing {missing}")
    return make_result("required_columns_present", STAGE, m["source_code"], m["batch_id"], "critical", problems)


def check_json_shape(m: dict, batch_dir: Path) -> CheckResult:
    schema = _schema(m["source_code"]) or {}
    required_keys = schema.get("required_keys", ["daily.time"])
    problems = []
    for name in _data_files(m, schema):
        if not name.lower().endswith(".json") or not (batch_dir / name).exists():
            continue
        try:
            body = json.loads((batch_dir / name).read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            problems.append(f"{name}: invalid JSON ({exc.msg})")
            continue
        for key in required_keys:
            node = body
            for part in key.split("."):
                node = node.get(part) if isinstance(node, dict) else None
            if node is None:
                problems.append(f"{name}: missing {key}")
        daily = body.get("daily") or {}
        n = len(daily.get("time") or [])
        uneven = [k for k, v in daily.items() if isinstance(v, list) and len(v) != n]
        if uneven:
            problems.append(f"{name}: daily series {uneven} differ in length from daily.time ({n})")
    return make_result("json_shape", STAGE, m["source_code"], m["batch_id"], "critical", problems)


CHECKS = [check_manifest_status, check_files_exist, check_checksums, check_row_counts, check_required_columns]


# ---------------------------------------------------------------- entry points
def run_checks(manifest_path: Path) -> list[CheckResult]:
    """All checks for one batch. Never raises for data problems; returns results."""
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        return [make_result("manifest_readable", STAGE, "?", manifest_path.parent.name, "critical",
                            [f"{manifest_path} not found"])]
    try:
        m = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [make_result("manifest_readable", STAGE, "?", manifest_path.parent.name, "critical",
                            [f"{manifest_path.name}: invalid JSON ({exc.msg})"])]
    batch_dir = manifest_path.parent
    checks = CHECKS + ([check_json_shape] if m.get("source_code") == "open_meteo" else [])
    return [check(m, batch_dir) for check in checks]


def validate_raw(manifest_paths: list[str], run_id: str = "manual") -> None:
    """Validate raw batches, write the report, and raise DataQualityError on any critical failure.
    ING-6 calls this twice: sources before STG-1, weather after ING-4."""
    results: list[CheckResult] = []
    for p in manifest_paths:
        results.extend(run_checks(Path(p)))
    write_report(results, STAGE, run_id)
    raise_on_critical(results)


def all_manifests() -> list[str]:
    return sorted(str(p) for p in (data_dir() / "raw").rglob("manifest.json"))


def main() -> None:
    parser = argparse.ArgumentParser(description="VAL-1: raw-layer data-quality checks")
    parser.add_argument("--manifest", action="append", default=[], help="manifest.json path (repeatable)")
    parser.add_argument("--all", action="store_true", help="check every manifest under data/raw")
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args()
    paths = args.manifest + (all_manifests() if args.all else [])
    if not paths:
        parser.error("give --manifest <path> or --all")
    results = [r for p in paths for r in run_checks(Path(p))]
    print(format_table(results))
    report = write_report(results, STAGE, args.run_id)
    print(f"\nReport: {report}")
    try:
        raise_on_critical(results)
    except DataQualityError as exc:
        print(f"\nFAILED: {exc}")
        sys.exit(1)
    print("\nAll critical checks passed.")


if __name__ == "__main__":
    main()
