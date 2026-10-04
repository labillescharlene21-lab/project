"""Tests for the warehouse connection settings (src/load/db.py). No database needed."""
import pytest

from src.load import db
from src.utils import config as cfg
from src.utils.exceptions import ConfigError

ENV = {"POSTGRES_HOST": "dbhost", "POSTGRES_PORT": "5433", "POSTGRES_DB": "wq",
       "POSTGRES_USER": "wq_user", "POSTGRES_PASSWORD": "p@ss/word"}


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(cfg, "load_env", lambda: None)   # ignore any local .env file
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_database_url_comes_from_env(env):
    url = db.database_url()
    assert url.drivername == "postgresql+psycopg2"
    assert (url.host, url.port, url.database, url.username) == ("dbhost", 5433, "wq", "wq_user")
    assert url.password == "p@ss/word"           # special characters kept intact


def test_password_has_no_default(env):
    env.delenv("POSTGRES_PASSWORD")
    with pytest.raises(ConfigError, match="POSTGRES_PASSWORD"):
        db.database_url()


def test_port_defaults_to_5432(env):
    env.delenv("POSTGRES_PORT")
    assert db.database_url().port == 5432


def test_get_engine_uses_the_url(env):
    assert db.get_engine().url.host == "dbhost"   # creating an engine does not connect yet