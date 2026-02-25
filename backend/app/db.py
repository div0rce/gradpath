from __future__ import annotations

from collections.abc import Generator
from functools import lru_cache
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from app.core.config import get_settings

Base = declarative_base()


def _sqlite_connect_args(url: str) -> dict[str, Any]:
    return {"check_same_thread": False} if url.startswith("sqlite") else {}


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = get_settings().database_url
    connect_args = _sqlite_connect_args(url)
    return create_engine(url, connect_args=connect_args)


@lru_cache(maxsize=1)
def get_sessionmaker() -> sessionmaker:
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def SessionLocal() -> Session:
    return get_sessionmaker()()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
