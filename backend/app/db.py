from __future__ import annotations

from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from app.core.config import get_settings

Base = declarative_base()


def _sqlite_connect_args(url: str) -> dict[str, bool]:
    return {"check_same_thread": False} if url.startswith("sqlite") else {}


@lru_cache(maxsize=1)
def get_engine():
    url = get_settings().database_url
    connect_args = _sqlite_connect_args(url)
    if connect_args:
        return create_engine(url, connect_args=connect_args, future=True)
    return create_engine(url, future=True)


@lru_cache(maxsize=1)
def get_sessionmaker():
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def SessionLocal() -> Session:
    return get_sessionmaker()()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
