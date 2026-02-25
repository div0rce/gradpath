from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import catalog, plans
from app.core.config import get_settings
from app.db import Base, get_engine

Base.metadata.create_all(bind=get_engine())

app = FastAPI(title=get_settings().app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(catalog.router)
app.include_router(plans.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
