from fastapi import FastAPI

from app.api.routes import catalog, plans
from app.core.config import settings
from app.db import Base, engine

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.app_name)
app.include_router(catalog.router)
app.include_router(plans.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
