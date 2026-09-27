"""Shared test setup: every test gets its own temporary DATA_DIR and a copy of config/."""
import shutil
from pathlib import Path

import pytest

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"


@pytest.fixture(autouse=True)
def isolated_dirs(tmp_path, monkeypatch):
    data = tmp_path / "data"
    cfg = tmp_path / "config"
    data.mkdir()
    shutil.copytree(REPO_CONFIG, cfg)
    monkeypatch.setenv("DATA_DIR", str(data))
    monkeypatch.setenv("CONFIG_DIR", str(cfg))
    return data, cfg