import re
from pathlib import Path

from . import config
from .exceptions import ConfigError

_UNSAFE_PATH_PATTERN = re.compile(r"(\.\.|[/\\])")


def _validate_path_component(field_name: str, value: str) -> None:
    if _UNSAFE_PATH_PATTERN.search(value):
        raise ConfigError(
            f"{field_name} contains unsafe path characters (/, \\, or ..): {value!r}"
        )


def data_dir() -> Path:
    configured = config.get_env("DATA_DIR", required=False)
    if configured:
        return Path(configured)
    return config.PROJECT_ROOT / "data"


def raw_batch_dir(source_code: str, batch_id: str, create: bool = True) -> Path:
    _validate_path_component("source_code", source_code)
    _validate_path_component("batch_id", batch_id)
    path = data_dir() / "raw" / source_code / f"batch_id={batch_id}"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def landing_dir(subdir: str) -> Path:
    path = data_dir() / "landing" / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path


def staging_dir() -> Path:
    path = data_dir() / "staging"
    path.mkdir(parents=True, exist_ok=True)
    return path


def curated_dir() -> Path:
    path = data_dir() / "curated"
    path.mkdir(parents=True, exist_ok=True)
    return path