"""Warehouse database connection (DB-1).

Settings come from environment variables (.env locally; docker-compose.yml sets them in
the containers): POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
The password has no default on purpose: if it is missing, get_env raises ConfigError.
"""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

from src.utils import config as cfg


def database_url() -> URL:
    """Connection URL built from the environment (URL.create escapes special characters)."""
    return URL.create(
        drivername="postgresql+psycopg2",
        username=cfg.get_env("POSTGRES_USER"),
        password=cfg.get_env("POSTGRES_PASSWORD"),  # required: no default
        host=cfg.get_env("POSTGRES_HOST"),
        port=int(cfg.get_env("POSTGRES_PORT", default="5432")),
        database=cfg.get_env("POSTGRES_DB"),
    )


def get_engine() -> Engine:
    """SQLAlchemy engine for the warehouse. pool_pre_ping replaces dropped connections."""
    return create_engine(database_url(), pool_pre_ping=True)