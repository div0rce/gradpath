from pydantic import BaseModel


class Settings(BaseModel):
    app_name: str = "GradPath API"
    database_url: str = "sqlite:///./gradpath.db"


settings = Settings()
