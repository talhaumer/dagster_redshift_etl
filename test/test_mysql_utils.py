import pytest
from resources.mysql_utils import get_mysql_engine
from utils.env_loader import load_env


def test_get_mysql_engine_valid_config():
    config = {
        "MYSQL_USER": "user",
        "MYSQL_PASSWORD": "pass",
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": 3306,
        "MYSQL_DATABASE": "test_db",
    }
    engine = get_mysql_engine(config)
    assert engine is not None
    assert "mysql" in str(engine.url)


def test_get_mysql_engine_from_env(monkeypatch):
    """
    Test get_mysql_engine using config loaded from environment variables.
    """
    # Set environment variables for the test
    monkeypatch.setenv("MYSQL_USER", "user")
    monkeypatch.setenv("MYSQL_PASSWORD", "pass")
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_PORT", "3306")
    monkeypatch.setenv("MYSQL_DATABASE", "test_db")

    config = load_env()
    engine = get_mysql_engine(config)
    assert engine is not None
    assert "mysql" in str(engine.url)
