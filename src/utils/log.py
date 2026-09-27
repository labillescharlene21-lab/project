"""Shared logger (ING-0).

Every message is prefixed with stage, source and batch id, e.g.
2026-09-27 08:15:02 | INFO | stage=extract | source=open_meteo | batch=open_meteo_ab12 | 3 cells

The prefix is added to the message text itself, so it works with any log format,
including Airflow's task-log handler and third-party libraries' log records.
"""
from __future__ import annotations

import logging
import os
import sys

_LOGGER_NAME = "wq"
_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _base_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False   # avoid printing every line twice
    return logger


class _ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        e = self.extra
        return f"stage={e['stage']} | source={e['source']} | batch={e['batch_id']} | {msg}", kwargs


def get_logger(stage: str, source_code: str, batch_id: str | None = None) -> logging.LoggerAdapter:
    """Return a logger that tags every message with stage, source and batch id."""
    return _ContextAdapter(_base_logger(), {"stage": stage, "source": source_code, "batch_id": batch_id or "-"})