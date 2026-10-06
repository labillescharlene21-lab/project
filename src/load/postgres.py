"""LOAD-1: UPSERT the curated tables and marts into the PostgreSQL warehouse.

Reads data/curated/<table>.parquet (CUR-1 + MART-1) and loads the tables in FK-safe order.
Per table, in ONE transaction:
  1. insert this table's etl_batch_log row (facts and marts reference it via load_batch_id)
  2. CREATE TEMP TABLE ... (LIKE target) and COPY the Parquet rows into it
  3. INSERT INTO target SELECT ... FROM temp ON CONFLICT (pk) DO UPDATE SET <non-key columns>
     RETURNING (xmax = 0) counts inserted vs updated rows
  4. marts only: DELETE target rows whose key is not in the new data
  5. finish the etl_batch_log row (finished_at, rows_out, counts)
On any error that table's transaction rolls back completely; a separate transaction then
logs status='failed', and the error is re-raised.

Run:  python -m src.load.postgres [--curated-ref <CUR-1 batch id>] [--run-id <id>]
"""
from __future__ import annotations

import argparse
import io
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from psycopg2 import sql
from sqlalchemy.engine import Engine

from src.load.db import get_engine
from src.utils.exceptions import ConfigError
from src.utils.log import get_logger
from src.utils.manifest import make_batch_id
from src.utils.paths import curated_dir

STAGE = "load"
LOAD_ORDER = [  # FK-safe: dimensions, then facts, then marts
    "dim_source", "dim_indicator", "dim_region", "dim_grid_cell", "dim_site",
    "fact_weather_daily", "fact_observation", "mart_site_hotspot", "mart_region_priority",
]
MART_TABLES = {"mart_site_hotspot", "mart_region_priority"}
LOAD_BATCH_TABLES = {"fact_observation", "mart_site_hotspot", "mart_region_priority"}
INT_TYPES = {"smallint", "integer", "bigint"}
NULL = "\\N"  # COPY's marker for NULL


@dataclass
class TableResult:
    table: str
    batch_id: str
    rows_in: int
    inserted: int
    updated: int
    deleted: int
    rows_in_table: int


# ---------------------------------------------------------------- inputs

def check_curated(curated_ref: str, root: Path) -> None:
    """All curated files must exist, and match curated_ref unless it is 'manual'."""
    missing = [t for t in LOAD_ORDER if not (root / f"{t}.parquet").exists()]
    if missing:
        raise ConfigError(f"Missing curated files in {root}: {missing}; run CUR-1 and MART-1 first")
    manifest_path = root / "_manifest.json"
    on_disk = (json.loads(manifest_path.read_text(encoding="utf-8")).get("batch_id")
               if manifest_path.exists() else None)
    if curated_ref != "manual" and on_disk != curated_ref:
        raise ConfigError(f"curated_ref {curated_ref!r} does not match the curated data on disk ({on_disk!r})")


# ---------------------------------------------------------------- database catalog

def table_columns(cur, table: str) -> list[tuple[str, str]]:
    """(column, data_type) of a warehouse table, in table order (current schema)."""
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s ORDER BY ordinal_position",
        (table,))
    columns = cur.fetchall()
    if not columns:
        raise ConfigError(f"Table {table} not found: run python -m src.load.init_db first")
    return columns


def primary_key(cur, table: str) -> list[str]:
    """Primary-key columns of a table, in key order."""
    cur.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary "
        "ORDER BY array_position(i.indkey::int2[], a.attnum)",
        (table,))
    return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------- one table

def to_copy_frame(df: pd.DataFrame, columns: list[tuple[str, str]]) -> pd.DataFrame:
    """Parquet rows in the table's column order, ready for COPY.

    Table columns missing from the file become NULL. Integer columns become nullable
    integers: a column stored as 3.0 (because it has nulls) is rejected by an integer column.
    """
    out = {}
    for name, dtype in columns:
        col = df[name] if name in df.columns else pd.Series(pd.NA, index=df.index, dtype="object")
        if dtype in INT_TYPES:
            col = pd.to_numeric(col).round().astype("Int64")
        out[name] = col
    return pd.DataFrame(out, index=df.index)


def copy_into(cur, table: sql.Identifier, frame: pd.DataFrame) -> None:
    """Bulk-load a DataFrame with COPY (CSV, NULL marked as \\N)."""
    buf = io.StringIO()
    frame.to_csv(buf, index=False, header=False, na_rep=NULL)
    buf.seek(0)
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL {})").format(
        table, sql.SQL(", ").join(map(sql.Identifier, frame.columns)), sql.Literal(NULL))
    cur.copy_expert(statement.as_string(cur), buf)


def load_table(cur, table: str, df: pd.DataFrame, batch_id: str, run_id: str,
               curated_ref: str, started_at: datetime) -> TableResult:
    """Upsert one table inside the caller's transaction."""
    target, tmp = sql.Identifier(table), sql.Identifier(f"tmp_{table}")
    columns = table_columns(cur, table)
    names = [name for name, _ in columns]
    pk = primary_key(cur, table)
    non_key = [name for name in names if name not in pk]

    # 1. Batch log row first: facts and marts reference it through load_batch_id (FK)
    cur.execute(
        "INSERT INTO etl_batch_log (batch_id, dag_run_id, stage, source_code, started_at, status, rows_in, params) "
        "VALUES (%s, %s, %s, NULL, %s, 'success', %s, %s)",
        (batch_id, run_id, STAGE, started_at, len(df),
         json.dumps({"table": table, "curated_ref": curated_ref})))

    # 2. Temp copy of the target table, filled with the Parquet rows
    if table in LOAD_BATCH_TABLES:
        df = df.assign(load_batch_id=batch_id)
    cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP")
                .format(tmp, target))
    copy_into(cur, tmp, to_copy_frame(df, columns))

    # 3. Upsert; xmax = 0 means the row was newly inserted, otherwise it was updated
    cols = sql.SQL(", ").join(map(sql.Identifier, names))
    conflict = sql.SQL(", ").join(map(sql.Identifier, pk))
    updates = sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in non_key)
    cur.execute(sql.SQL(
        "WITH upserted AS ("
        " INSERT INTO {target} ({cols}) SELECT {cols} FROM {tmp}"
        " ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        " RETURNING (xmax = 0) AS inserted) "
        "SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) FROM upserted"
    ).format(target=target, cols=cols, tmp=tmp, conflict=conflict, updates=updates))
    inserted, updated = cur.fetchone()

    # 4. Marts are fully derived: remove rows that are no longer in the new data
    deleted = 0
    if table in MART_TABLES:
        match = sql.SQL(" AND ").join(sql.SQL("n.{0} = t.{0}").format(sql.Identifier(c)) for c in pk)
        cur.execute(sql.SQL("DELETE FROM {target} t WHERE NOT EXISTS (SELECT 1 FROM {tmp} n WHERE {match})")
                    .format(target=target, tmp=tmp, match=match))
        deleted = cur.rowcount

    # 5. Finish the batch log row
    cur.execute(sql.SQL("SELECT count(*) FROM {}").format(target))
    rows_in_table = cur.fetchone()[0]
    cur.execute(
        "UPDATE etl_batch_log SET finished_at = now(), rows_out = %s, params = params || %s::jsonb "
        "WHERE batch_id = %s",
        (inserted + updated,
         json.dumps({"inserted": inserted, "updated": updated, "deleted": deleted,
                     "rows_in_table": rows_in_table}),
         batch_id))
    return TableResult(table, batch_id, len(df), inserted, updated, deleted, rows_in_table)


def log_failure(conn, logger, batch_id: str, table: str, run_id: str, curated_ref: str,
                started_at: datetime, rows_in: int, exc: Exception) -> None:
    """Record status='failed' in its own transaction (the table's transaction was rolled back)."""
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO etl_batch_log (batch_id, dag_run_id, stage, source_code, started_at, "
                    "finished_at, status, rows_in, rows_out, params) "
                    "VALUES (%s, %s, %s, NULL, %s, now(), 'failed', %s, 0, %s)",
                    (batch_id, run_id, STAGE, started_at, rows_in,
                     json.dumps({"table": table, "curated_ref": curated_ref, "error": str(exc)[:1000]})))
    except Exception as log_exc:  # never hide the original error
        logger.error("%s: could not record the failure in etl_batch_log (%s)", table, log_exc)


# ---------------------------------------------------------------- entry points

def run_load(curated_ref: str = "manual", run_id: str = "manual",
             engine: Engine | None = None, root: Path | None = None) -> list[TableResult]:
    """Load every table in LOAD_ORDER; returns one TableResult per table."""
    root = Path(root) if root else curated_dir()
    check_curated(curated_ref, root)
    engine = engine or get_engine()
    logger = get_logger(STAGE, "postgres", curated_ref)
    results = []
    raw = engine.raw_connection()
    conn = raw.driver_connection
    try:
        for table in LOAD_ORDER:
            df = pd.read_parquet(root / f"{table}.parquet")
            started_at = datetime.now(timezone.utc)
            batch_id = make_batch_id(f"load_{table}", {"curated_ref": curated_ref, "run_id": run_id,
                                                       "started_at": started_at.isoformat()})
            try:
                with conn:  # one transaction per table: commit on success, roll back on error
                    with conn.cursor() as cur:
                        result = load_table(cur, table, df, batch_id, run_id, curated_ref, started_at)
            except Exception as exc:
                logger.error("%s: load failed and was rolled back (%s)", table, exc)
                log_failure(conn, logger, batch_id, table, run_id, curated_ref, started_at, len(df), exc)
                raise
            logger.info("%s: rows in %d, inserted %d, updated %d, deleted %d, rows in table %d",
                        table, result.rows_in, result.inserted, result.updated,
                        result.deleted, result.rows_in_table)
            results.append(result)
    finally:
        raw.close()
    return results


def load_postgres(curated_ref: str, run_id: str = "manual") -> None:
    """LOAD-1 entry point (ING-6 calls this)."""
    run_load(curated_ref, run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="LOAD-1: upsert curated tables and marts into PostgreSQL")
    parser.add_argument("--curated-ref", default="manual", help="curated batch id (from CUR-1)")
    parser.add_argument("--run-id", default="manual", help="Airflow run id, or a label for manual runs")
    args = parser.parse_args()
    results = run_load(args.curated_ref, args.run_id)
    table = pd.DataFrame([asdict(r) for r in results]).drop(columns=["batch_id"])
    print(f"Load run '{args.run_id}' at {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC, "
          f"curated_ref={args.curated_ref}")
    print(table.to_string(index=False))
    print(f"Total inserted: {int(table['inserted'].sum())}, updated: {int(table['updated'].sum())}, "
          f"deleted: {int(table['deleted'].sum())}\n")


if __name__ == "__main__":
    main()