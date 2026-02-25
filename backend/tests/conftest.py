from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import db as dbmod
from app.core import config as configmod
from app.db import Base, SessionLocal, get_engine
from app.main import app
from app.enums import UserRole
from app.models import User


@pytest.fixture(autouse=True)
def _reset_settings_and_db_caches() -> None:
    configmod.get_settings.cache_clear()
    dbmod.get_engine.cache_clear()
    dbmod.get_sessionmaker.cache_clear()
    yield
    configmod.get_settings.cache_clear()
    dbmod.get_engine.cache_clear()
    dbmod.get_sessionmaker.cache_clear()


@pytest.fixture(autouse=True)
def reset_db(_reset_settings_and_db_caches) -> None:
    Base.metadata.drop_all(bind=get_engine())
    Base.metadata.create_all(bind=get_engine())


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture()
def user_id() -> str:
    with SessionLocal() as db:
        user = User(net_id="abc123", email="abc123@rutgers.edu", role=UserRole.STUDENT)
        db.add(user)
        db.commit()
        db.refresh(user)
        return user.id
