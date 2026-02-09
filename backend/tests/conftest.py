from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import User
from app.enums import UserRole


@pytest.fixture(autouse=True)
def reset_db() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


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
