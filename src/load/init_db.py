"""Apply the warehouse schema and list the tables (DB-1).

Run:  python -m src.load.init_db

Idempotent: sql/init/01_schema.sql uses CREATE ... IF NOT EXISTS, so running it again
changes nothing. The same file also runs automatically when the postgres volume is first
created (docker-compose.yml mounts sql/init to /docker-entrypoint-initdb.d).
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from src.load.db import get_engine
from src.utils import config as cfg

SCHEMA_FILE = cfg.PROJECT_ROOT / "sql" / "init" / "01_schema.sql"
EXPECTED_TABLES = {
    "dim_source", "dim_indicator", "dim_region", "dim_grid_cell", "dim_site",
    "fact_observation", "fact_weather_daily", "mart_site_hotspot", "mart_region_priority",
    "etl_batch_log", "dq_results",
}


def apply_schema(engine: Engine, path: Path = SCHEMA_FILE) -> list[str]:
    """Run the DDL file and return the sorted table names in the public schema."""
    sql = Path(path).read_text(encoding="utf-8")
    conn = engine.raw_connection()
    try:
        # The file has its own BEGIN/COMMIT, so don't wrap it in a second transaction
        conn.driver_connection.autocommit = True
        with conn.driver_connection.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.driver_connection.autocommit = False
        conn.close()
    return sorted(inspect(engine).get_table_names())


def main() -> None:
    tables = apply_schema(get_engine())
    print(f"Applied {SCHEMA_FILE.relative_to(cfg.PROJECT_ROOT)}. {len(tables)} tables:")
    for name in tables:
        print(f"  {name}")
    missing = EXPECTED_TABLES - set(tables)
    if missing:
        raise SystemExit(f"Missing tables: {sorted(missing)}")


if __name__ == "__main__":
    main()