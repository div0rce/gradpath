from __future__ import annotations

from app import db as dbmod
from app.core import config as configmod


def _clear_caches() -> None:
    configmod.get_settings.cache_clear()
    dbmod.get_engine.cache_clear()
    dbmod.get_sessionmaker.cache_clear()


def test_database_url_default_when_env_unset(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    _clear_caches()
    assert configmod.get_settings().database_url == "sqlite:///./gradpath.db"


def test_database_url_env_override_for_settings(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:////tmp/gradpath-config.db")
    _clear_caches()
    assert configmod.get_settings().database_url == "sqlite:////tmp/gradpath-config.db"


def test_database_url_env_override_for_engine(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:////tmp/gradpath-engine.db")
    _clear_caches()
    assert str(dbmod.get_engine().url) == "sqlite:////tmp/gradpath-engine.db"


def test_database_url_changes_after_cache_clear(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:////tmp/gradpath-first.db")
    _clear_caches()
    first = str(dbmod.get_engine().url)
    assert first == "sqlite:////tmp/gradpath-first.db"

    monkeypatch.setenv("DATABASE_URL", "sqlite:////tmp/gradpath-second.db")
    _clear_caches()
    second = str(dbmod.get_engine().url)
    assert second == "sqlite:////tmp/gradpath-second.db"
