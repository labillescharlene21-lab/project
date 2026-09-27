import logging
import sys

_CONFIGURED = False

# Adjust this format string to match AGENTS.md §5.4 exactly if it differs.
_LOG_FORMAT = "%(asctime)s | %(levelname)s | stage=%(stage)s source=%(source)s batch_id=%(batch_id)s | %(message)s"


def _configure_root_logger() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    _CONFIGURED = True


class _PipelineLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return msg, kwargs


def get_logger(stage: str, source_code: str, batch_id: str | None = None) -> logging.LoggerAdapter:
    _configure_root_logger()
    base_logger = logging.getLogger(f"pipeline.{stage}.{source_code}")
    extra = {
        "stage": stage,
        "source": source_code,
        "batch_id": batch_id if batch_id is not None else "-",
    }
    return _PipelineLoggerAdapter(base_logger, extra)