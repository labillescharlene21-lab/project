from pathlib import Path

from .config import get_env


def data_dir() -> Path:
    return Path(get_env("DATA_DIR"))


def raw_batch_dir(source_code: str, batch_id: str, create: bool = True) -> Path:
    path = data_dir() / "raw" / source_code / batch_id
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