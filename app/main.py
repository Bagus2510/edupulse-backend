from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.database import check_db_connection
from app.routers import superset, airflow, ai, settings, dashboards, activity, pipeline

app = FastAPI(
    title="EduPulse Backend",
    description="Backend API for EduPulse Analytics Platform",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(superset.router)
app.include_router(airflow.router)
app.include_router(ai.router)
app.include_router(settings.router)
app.include_router(dashboards.router)
app.include_router(activity.router)
app.include_router(pipeline.router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/health/db")
async def health_db():
    return await check_db_connection()
