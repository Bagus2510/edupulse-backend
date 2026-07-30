from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.database import check_db_connection
from app.routers import superset, airflow, ai, settings as settings_router, dashboards, activity, pipeline, auth, pipelines, home, datasets

app = FastAPI(
    title="EduPulse Backend",
    description="Backend API for EduPulse Analytics Platform",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.CORS_ORIGINS.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(superset.router)
app.include_router(airflow.router)
app.include_router(ai.router)
app.include_router(settings_router.router)
app.include_router(dashboards.router)
app.include_router(activity.router)
app.include_router(pipeline.router)
app.include_router(auth.router)
app.include_router(pipelines.router)
app.include_router(home.router)
app.include_router(datasets.router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/health/db")
async def health_db():
    return await check_db_connection()
