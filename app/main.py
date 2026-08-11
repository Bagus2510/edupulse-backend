from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.core.config import settings
from app.core.database import check_db_connection, async_session
from app.core.rate_limit import limiter
from app.routers import superset, airflow, ai, settings as settings_router, dashboards, activity, pipeline, auth, pipelines, home, datasets, lineage, domains, metadata, admin_users, quality_rules
from app.services.data_quality import backfill_asset_metadata

app = FastAPI(
    title="Datapulse Backend",
    description="Backend API for Datapulse Analytics Platform",
    version="1.0.0",
)

app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request, exc):
    return JSONResponse(
        status_code=429,
        content={"detail": f"Terlalu banyak request. Coba lagi dalam {exc.detail}."},
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
app.include_router(lineage.router)
app.include_router(domains.router)
app.include_router(metadata.router)
app.include_router(admin_users.router)
app.include_router(quality_rules.router)


@app.on_event("startup")
async def ensure_schemas():
    async with async_session() as session:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        # Add new columns if they don't exist
        await session.execute(text("ALTER TABLE app.users DROP CONSTRAINT IF EXISTS users_role_check"))
        await session.execute(text("ALTER TABLE app.users ADD CONSTRAINT users_role_check CHECK (role IN ('admin', 'editor', 'viewer'))"))
        await session.execute(text("ALTER TABLE app.pipelines ADD COLUMN IF NOT EXISTS max_active_runs INTEGER DEFAULT 1"))
        await session.execute(text("ALTER TABLE app.pipelines ADD COLUMN IF NOT EXISTS on_failure_callback VARCHAR(255) DEFAULT ''"))
        await session.execute(text("ALTER TABLE app.pipelines ADD COLUMN IF NOT EXISTS dag_id TEXT DEFAULT ''"))
        await session.execute(text("ALTER TABLE app.pipelines ADD COLUMN IF NOT EXISTS schedule_interval TEXT DEFAULT ''"))
        await session.execute(text("ALTER TABLE app.pipeline_steps ADD COLUMN IF NOT EXISTS execution_timeout INTEGER DEFAULT 300"))
        await session.execute(text("ALTER TABLE app.pipeline_steps ADD COLUMN IF NOT EXISTS retries INTEGER DEFAULT 1"))
        await session.execute(text("ALTER TABLE app.pipeline_steps ADD COLUMN IF NOT EXISTS retry_delay INTEGER DEFAULT 5"))
        # Phase 1: Metadata foundation
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.mart_table_metadata (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(200) NOT NULL UNIQUE,
                schema_name VARCHAR(50) DEFAULT 'mart',
                producing_pipeline_id INTEGER REFERENCES app.pipelines(id) ON DELETE SET NULL,
                producing_step_id INTEGER REFERENCES app.pipeline_steps(id) ON DELETE SET NULL,
                last_built_at TIMESTAMP,
                row_count INTEGER DEFAULT 0,
                column_info JSONB DEFAULT '[]'::jsonb,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.dashboard_dependencies (
                id SERIAL PRIMARY KEY,
                dashboard_id INTEGER REFERENCES app.dashboards(id) ON DELETE CASCADE,
                mart_table_name VARCHAR(200) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(dashboard_id, mart_table_name)
            )
        """))
        # Unified metadata and data-quality plane
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.data_assets (
                id SERIAL PRIMARY KEY,
                schema_name VARCHAR(50) NOT NULL,
                table_name VARCHAR(200) NOT NULL,
                asset_type VARCHAR(20) NOT NULL DEFAULT 'raw',
                owner_id INTEGER REFERENCES app.users(id) ON DELETE SET NULL,
                description TEXT DEFAULT '',
                row_count BIGINT DEFAULT 0,
                column_count INTEGER DEFAULT 0,
                last_loaded_at TIMESTAMP,
                last_built_at TIMESTAMP,
                freshness_status VARCHAR(20) DEFAULT 'never_built',
                quality_status VARCHAR(20) DEFAULT 'unknown',
                quality_score NUMERIC(5,2),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(schema_name, table_name)
            )
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.data_columns (
                id SERIAL PRIMARY KEY,
                asset_id INTEGER REFERENCES app.data_assets(id) ON DELETE CASCADE,
                column_name VARCHAR(200) NOT NULL,
                data_type VARCHAR(100) NOT NULL,
                nullable BOOLEAN DEFAULT true,
                null_count BIGINT DEFAULT 0,
                distinct_count BIGINT DEFAULT 0,
                is_primary_key BOOLEAN DEFAULT false,
                is_metric BOOLEAN DEFAULT false,
                description TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(asset_id, column_name)
            )
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.data_quality_results (
                id SERIAL PRIMARY KEY,
                asset_id INTEGER REFERENCES app.data_assets(id) ON DELETE CASCADE,
                check_name VARCHAR(200) NOT NULL,
                check_type VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                expected_value TEXT,
                actual_value TEXT,
                severity VARCHAR(20) DEFAULT 'info',
                message TEXT NOT NULL,
                checked_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.metric_definitions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(150) NOT NULL UNIQUE,
                label VARCHAR(200) NOT NULL,
                description TEXT DEFAULT '',
                formula TEXT NOT NULL,
                source_asset VARCHAR(255) NOT NULL,
                grain VARCHAR(100) DEFAULT '',
                unit VARCHAR(50) DEFAULT '',
                target_value NUMERIC,
                owner_id INTEGER REFERENCES app.users(id) ON DELETE SET NULL,
                last_verified_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.execute(text("ALTER TABLE app.pipeline_runs ADD COLUMN IF NOT EXISTS rows_read BIGINT DEFAULT 0"))
        await session.execute(text("ALTER TABLE app.pipeline_runs ADD COLUMN IF NOT EXISTS rows_written BIGINT DEFAULT 0"))
        await session.execute(text("ALTER TABLE app.pipeline_runs ADD COLUMN IF NOT EXISTS duration_ms BIGINT DEFAULT 0"))
        await session.execute(text("ALTER TABLE app.pipeline_runs ADD COLUMN IF NOT EXISTS quality_status VARCHAR(20) DEFAULT 'unknown'"))
        await session.execute(text("ALTER TABLE app.pipeline_runs ADD COLUMN IF NOT EXISTS quality_summary JSONB DEFAULT '{}'::jsonb"))
        await session.execute(text("ALTER TABLE app.pipeline_runs ADD COLUMN IF NOT EXISTS error_message TEXT DEFAULT ''"))
        # Phase 3: Multi-domain support
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.domains (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                icon VARCHAR(50) DEFAULT 'folder',
                color VARCHAR(20) DEFAULT 'pulse',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.execute(text("ALTER TABLE app.pipelines ADD COLUMN IF NOT EXISTS domain_id INTEGER REFERENCES app.domains(id)"))
        await session.execute(text("ALTER TABLE app.dashboards ADD COLUMN IF NOT EXISTS domain_id INTEGER REFERENCES app.domains(id)"))
        # Phase 4: Pipeline scheduling
        await session.execute(text("ALTER TABLE app.pipelines ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true"))
        # Phase 5: Configurable quality rules
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS app.quality_rules (
                id SERIAL PRIMARY KEY,
                asset_id INTEGER REFERENCES app.data_assets(id) ON DELETE CASCADE,
                rule_type VARCHAR(50) NOT NULL,
                column_name VARCHAR(200),
                parameters JSONB DEFAULT '{}'::jsonb,
                severity VARCHAR(20) DEFAULT 'medium',
                enabled BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        # Phase 6: Persist AI evidence
        await session.execute(text("ALTER TABLE app.chat_messages ADD COLUMN IF NOT EXISTS evidence JSONB DEFAULT NULL"))
        await session.commit()
        await backfill_asset_metadata(session)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/health/db")
async def health_db():
    return await check_db_connection()
