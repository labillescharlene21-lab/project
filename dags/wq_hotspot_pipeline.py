"""Global Water Quality Hotspot Pipeline: one DAG from raw extraction to marts.

Airflow 3.3.2 (pinned in docker-compose.yml and the Dockerfile). Imports come from
the Airflow 3 Task SDK (`airflow.sdk`), not from the Airflow 2 `airflow.models` /
`airflow.decorators` paths.

This file only orders tasks. Every task calls one function from `src/` and returns its
manifest path or batch id through XCom. No business logic lives here.

Task graph:

    extract_boundaries ─┐
    extract_owq ────────┤
    sample_wqp_sites → extract_wqp_results ─┤
                                            ├→ build_sampled_sites → extract_weather → validate_raw
    validate_raw → build_staging → validate_staging → build_curated → load_postgres
                 → validate_curated → build_marts
"""
from __future__ import annotations

import importlib
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from src.utils.config import load_yaml
from src.utils.exceptions import ConfigError

log = logging.getLogger(__name__)

DAG_ID = "wq_hotspot_pipeline"
PARAM_NAMES = ("start_year", "end_year", "k", "seed")

# Errors that a retry cannot fix (bad parameters, failed data-quality checks).
# They fail the task at once instead of waiting for the retries.
_NO_RETRY_ERRORS = ("ConfigError", "DataQualityError")

_BATCH_IN_PATH = re.compile(r"batch_id=([^/\\]+)")
_BATCH_ID = re.compile(r"^[a-z0-9_]+_[0-9a-f]{12}$")


# ---------------------------------------------------------------- defaults from config/

_sampling = load_yaml("sampling")
DEFAULT_PARAMS = {
    "start_year": _sampling["period"]["start_year"],
    "end_year": _sampling["period"]["end_year"],
    "k": _sampling["sampling"]["k_per_stratum_realm"],
    "seed": _sampling["random_seed"],
}


# ---------------------------------------------------------------- failure handling

def _batch_id_from(value: Any) -> str | None:
    """Return a batch id from an XCom value: a manifest path or a batch id string."""
    if not isinstance(value, str):
        return None
    match = _BATCH_IN_PATH.search(value)
    if match:
        return match.group(1)
    return value if _BATCH_ID.match(value) else None


def _known_batch_id(context: dict) -> str:
    """Batch id of the failed task if known, else the ids its upstream tasks produced."""
    ti = context.get("ti") or context.get("task_instance")
    task_obj = context.get("task")
    try:
        own = _batch_id_from(ti.xcom_pull(task_ids=ti.task_id))
        if own:
            return own
        upstream = sorted(getattr(task_obj, "upstream_task_ids", []) or [])
        found = {}
        for upstream_id in upstream:
            batch = _batch_id_from(ti.xcom_pull(task_ids=upstream_id))
            if batch:
                found[upstream_id] = batch
        if found:
            return "unknown (upstream: " + ", ".join(f"{t}={b}" for t, b in found.items()) + ")"
    except Exception as exc:  # the callback must never hide the original failure
        return f"unknown (lookup failed: {exc!r})"
    return "unknown (task failed before it produced a batch)"


def log_task_failure(context: dict) -> None:
    """on_failure_callback: one greppable ERROR line in the task log with the failure context."""
    ti = context.get("ti") or context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    log.error(
        "TASK FAILED | dag_id=%s | task_id=%s | run_id=%s | try_number=%s | batch_id=%s | exception=%s: %s",
        getattr(ti, "dag_id", None) or getattr(dag_run, "dag_id", DAG_ID),
        getattr(ti, "task_id", "?"),
        context.get("run_id") or getattr(dag_run, "run_id", "?"),
        getattr(ti, "try_number", "?"),
        _known_batch_id(context),
        type(exception).__name__,
        exception,
        exc_info=exception if isinstance(exception, BaseException) else None,
    )


# ---------------------------------------------------------------- helpers used by tasks

def _run_params() -> dict:
    """The four DAG params of this run, as plain ints."""
    params = get_current_context()["params"]
    return {name: int(params[name]) for name in PARAM_NAMES}


def _run_id() -> str:
    """Airflow run id, passed to the load and validation steps for their reports and logs."""
    return get_current_context()["run_id"]


def _call(target: str, *args: Any, **kwargs: Any) -> Any:
    """Import `module:function` when the task runs, call it, and make failures easy to read.

    The import is lazy on purpose: the DAG file parses even while a callable is not merged
    yet, and the task then fails with a message that names the missing callable.
    """
    module_name, func_name = target.split(":")
    try:
        func = getattr(importlib.import_module(module_name), func_name)
    except (ImportError, AttributeError) as exc:
        raise AirflowFailException(
            f"Callable {target} is not available (not merged yet, or an import in it failed): {exc!r}"
        ) from exc
    log.info("Calling %s with args=%s kwargs=%s", target, args, kwargs)
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        if type(exc).__name__ in _NO_RETRY_ERRORS:
            log.error("%s in %s: %s (not retried)", type(exc).__name__, target, exc)
            raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc
        raise


def _manifest(target: str) -> str:
    """Run an extractor with the DAG params and return its manifest path as a string."""
    return str(_call(target, **_run_params()))


# ---------------------------------------------------------------- DAG

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": log_task_failure,
}

with DAG(
    dag_id=DAG_ID,
    description="Raw to marts: extract, validate, stage, curate, load, build marts.",
    schedule="@monthly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        # Defaults come from config/sampling.yaml. No min/max here on purpose: bad values
        # such as start_year=1800 must reach the extract task and fail in validate_period.
        "start_year": Param(DEFAULT_PARAMS["start_year"], type="integer", title="Start year",
                            description="First year of the period (default: sampling.yaml period.start_year)."),
        "end_year": Param(DEFAULT_PARAMS["end_year"], type="integer", title="End year",
                          description="Last year of the period (default: sampling.yaml period.end_year)."),
        "k": Param(DEFAULT_PARAMS["k"], type="integer", title="K per stratum and realm",
                   description="Max sites per stratum and realm (default: sampling.yaml sampling.k_per_stratum_realm)."),
        "seed": Param(DEFAULT_PARAMS["seed"], type="integer", title="Random seed",
                      description="Sampling seed (default: sampling.yaml random_seed)."),
    },
    tags=["water-quality", "pipeline"],
    doc_md=__doc__,
) as dag:

    # ----- raw extraction: each task returns its manifest path

    @task
    def extract_boundaries() -> str:
        return _manifest("src.extract.boundaries:run")

    @task
    def extract_owq() -> str:
        return _manifest("src.extract.owq:run")

    @task
    def sample_wqp_sites() -> str:
        return _manifest("src.extract.wqp_sites:run")

    @task
    def extract_wqp_results() -> str:
        return _manifest("src.extract.wqp_results:run")

    # ----- sampled sites (DE2, STG-1): input for weather

    @task
    def build_sampled_sites() -> str:
        return _call("src.transform.sites:build_sampled_sites", _run_params())

    @task
    def extract_weather() -> str:
        return _manifest("src.extract.weather:run")

    # ----- raw validation, staging, curated, load, marts

    @task
    def validate_raw(manifest_paths: list[str]) -> None:
        _call("src.validation.raw_checks:validate_raw", manifest_paths, run_id=_run_id())

    @task
    def build_staging() -> str:
        return _call("src.transform.staging:build_staging", _run_params())

    @task
    def validate_staging(staging_ref: str) -> None:
        _call("src.validation.staging_checks:validate_staging", staging_ref, run_id=_run_id())

    @task
    def build_curated(staging_ref: str) -> str:
        return _call("src.transform.curated:build_curated", staging_ref, _run_params())

    @task
    def load_postgres(curated_ref: str) -> None:
        _call("src.load.postgres:load_postgres", curated_ref, run_id=_run_id())

    @task
    def validate_curated(curated_ref: str) -> None:
        _call("src.validation.curated_checks:validate_curated", curated_ref, run_id=_run_id())

    @task
    def build_marts(curated_ref: str) -> None:
        _call("src.transform.marts:build_marts", curated_ref)

    # ----- wiring

    boundaries = extract_boundaries()
    owq = extract_owq()
    wqp_sites = sample_wqp_sites()
    wqp_results = extract_wqp_results()
    sampled_sites = build_sampled_sites()
    weather = extract_weather()

    wqp_sites >> wqp_results                      # results reads the sites file written by sample_wqp_sites
    [boundaries, owq, wqp_results] >> sampled_sites
    sampled_sites >> weather                      # weather reads data/staging/sampled_sites/sampled_sites.parquet

    raw_ok = validate_raw([boundaries, owq, wqp_sites, wqp_results, weather])

    staging_ref = build_staging()
    raw_ok >> staging_ref

    staging_ok = validate_staging(staging_ref)
    curated_ref = build_curated(staging_ref)
    staging_ok >> curated_ref

    marts = build_marts(curated_ref)
    loaded = load_postgres(curated_ref)
    curated_ok = validate_curated(curated_ref)

    marts >> loaded >> curated_ok
