"""Shared data-quality framework (VAL-1), reused by VAL-2 (staging) and VAL-3 (curated).

Every check returns a CheckResult. A stage collects its results, writes one JSON report
(outputs/dq/dq_report_{stage}_{run_id}.json), optionally inserts them into Postgres (dq_results),
and then calls raise_on_critical(): any failed *critical* check raises DataQualityError, which
fails the Airflow task. Failed *warnings* are recorded but never stop the pipeline.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from src.utils.log import get_logger
from src.utils.paths import data_dir


class DataQualityError(Exception):
    """A critical data-quality check failed. Defined here (not in utils/exceptions.py) on purpose."""


@dataclass
class CheckResult:
    check_name: str
    stage: str
    source_code: str
    batch_id: str
    severity: Literal["critical", "warning"]
    status: Literal["pass", "fail"]
    failed_count: int = 0
    details: str = ""
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

    @property
    def is_critical_failure(self) -> bool:
        return self.status == "fail" and self.severity == "critical"


def make_result(check_name: str, stage: str, source_code: str, batch_id: str, severity: str,
                problems: list[str]) -> CheckResult:
    """Build a result from a list of problem descriptions (empty list = pass)."""
    shown = "; ".join(problems[:5]) + (f"; ... and {len(problems) - 5} more" if len(problems) > 5 else "")
    return CheckResult(check_name, stage, source_code, batch_id, severity,
                       "fail" if problems else "pass", len(problems), shown)


def outputs_dir() -> Path:
    """OUTPUT_DIR if set, else <repo>/outputs (next to the data folder), same rule as MART-1."""
    value = os.environ.get("OUTPUT_DIR")
    path = Path(value) if value else data_dir().resolve().parent / "outputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _insert_into_db(results: list[CheckResult], run_id: str, log) -> None:
    """Best effort: insert into dq_results if Postgres is configured and reachable (DB-1 creates the table)."""
    host = os.environ.get("POSTGRES_HOST")
    if not host:
        log.info("POSTGRES_HOST not set; dq_results insert skipped (JSON report written)")
        return
    try:
        import psycopg2  # noqa: imported lazily; added to requirements by DB-1/LOAD-1
    except ImportError:
        log.warning("psycopg2 not installed; dq_results insert skipped (JSON report written)")
        return
    try:
        conn = psycopg2.connect(
            host=host, port=os.environ.get("POSTGRES_PORT", "5432"), dbname=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("POSTGRES_USER"), password=os.environ.get("POSTGRES_PASSWORD"), connect_timeout=3)
        with conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO dq_results (run_id, batch_id, check_name, stage, source_code, severity, status,"
                " failed_count, details, checked_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(run_id, r.batch_id, r.check_name, r.stage, r.source_code, r.severity, r.status,
                  r.failed_count, r.details, r.checked_at) for r in results])
        conn.close()
        log.info(f"inserted {len(results)} rows into dq_results")
    except Exception as exc:  # DB down or table missing: the JSON report is still the record
        log.warning(f"dq_results insert skipped: {exc}")


def write_report(results: list[CheckResult], stage: str, run_id: str) -> Path:
    """Write outputs/dq/dq_report_{stage}_{run_id}.json and try the dq_results insert."""
    log = get_logger("validation", stage, run_id)
    out = outputs_dir() / "dq"
    out.mkdir(parents=True, exist_ok=True)
    safe_run = "".join(c if c.isalnum() or c in "-_." else "_" for c in run_id)
    path = out / f"dq_report_{stage}_{safe_run}.json"
    summary = {
        "stage": stage, "run_id": run_id,
        "checks": len(results),
        "passed": sum(r.status == "pass" for r in results),
        "failed_critical": sum(r.is_critical_failure for r in results),
        "failed_warning": sum(r.status == "fail" and r.severity == "warning" for r in results),
        "results": [asdict(r) for r in results],
    }
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    log.info(f"{summary['checks']} checks: {summary['passed']} passed, {summary['failed_critical']} critical failed, "
             f"{summary['failed_warning']} warnings failed; report {path}")
    _insert_into_db(results, run_id, log)
    return path


def format_table(results: list[CheckResult]) -> str:
    """Plain-text pass/fail table for the CLI and logs."""
    rows = [("STATUS", "SEVERITY", "CHECK", "SOURCE", "BATCH", "FAILED", "DETAILS")]
    for r in results:
        rows.append(("PASS" if r.status == "pass" else "FAIL", r.severity, r.check_name, r.source_code,
                     r.batch_id, str(r.failed_count), r.details[:80]))
    widths = [max(len(row[i]) for row in rows) for i in range(6)]
    return "\n".join("  ".join(row[i].ljust(widths[i]) for i in range(6)) + "  " + row[6] for row in rows)


def raise_on_critical(results: list[CheckResult]) -> None:
    """Raise DataQualityError naming every failed critical check (warnings never raise)."""
    failed = [r for r in results if r.is_critical_failure]
    if failed:
        names = ", ".join(f"{r.check_name} [{r.source_code} {r.batch_id}]: {r.details}" for r in failed)
        raise DataQualityError(f"{len(failed)} critical data-quality check(s) failed: {names}")
