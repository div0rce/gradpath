from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    app_name: str = "GradPath API"
    database_url: str = "sqlite:///./gradpath.db"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
