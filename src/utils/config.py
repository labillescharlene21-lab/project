import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from .exceptions import ConfigError


def load_env() -> None:
    """Load variables from a .env file if present. No error if it's absent (Docker sets env vars directly)."""
    load_dotenv(override=False)


def get_env(name: str, default: str | None = None, required: bool = True) -> str:
    value = os.environ.get(name)
    if value is not None and value != "":
        return value
    if default is not None:
        return default
    if required:
        raise ConfigError(f"Missing required environment variable: {name}")
    return None


def _config_dir() -> Path:
    return Path(get_env("CONFIG_DIR", default="config", required=False))


def load_yaml(name: str) -> dict:
    path = _config_dir() / f"{name}.yaml"
    if not path.exists():
        raise ConfigError(f"YAML config file not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML file {path}: {exc}") from exc
    if data is None:
        raise ConfigError(f"YAML config file is empty: {path}")
    return data


def validate_period(start_year: int, end_year: int) -> None:
    if start_year > end_year:
        raise ConfigError(
            f"start_year ({start_year}) must not be greater than end_year ({end_year})"
        )

    sampling_config = load_yaml("sampling")
    try:
        min_start_year = sampling_config["period"]["start_year"]
    except (KeyError, TypeError) as exc:
        raise ConfigError("sampling.yaml is missing 'period.start_year'") from exc

    if start_year < min_start_year:
        raise ConfigError(
            f"start_year ({start_year}) is earlier than the configured minimum ({min_start_year})"
        )

    current_year = datetime.now(timezone.utc).year
    if end_year > current_year:
        raise ConfigError(f"end_year ({end_year}) is in the future (current year: {current_year})")