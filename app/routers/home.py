from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db

router = APIRouter(prefix="/api/home", tags=["home"])


@router.get("/summary")
async def home_summary(db: AsyncSession = Depends(get_db)):
    pipeline_count = await db.execute(text("SELECT COUNT(*) FROM app.pipelines"))
    pipeline_total = pipeline_count.scalar() or 0

    pipeline_running = await db.execute(
        text("SELECT COUNT(*) FROM app.pipelines WHERE status = 'running'")
    )
    pipeline_active = pipeline_running.scalar() or 0

    dashboard_count = await db.execute(text("SELECT COUNT(*) FROM app.dashboards"))
    dashboard_total = dashboard_count.scalar() or 0



    success_runs = await db.execute(
        text("SELECT COUNT(*) FROM app.pipeline_runs WHERE status = 'success'")
    )
    success_total = success_runs.scalar() or 0

    total_runs = await db.execute(text("SELECT COUNT(*) FROM app.pipeline_runs"))
    total_runs_count = total_runs.scalar() or 0

    success_rate = round((success_total / total_runs_count * 100), 1) if total_runs_count > 0 else 0

    activity_result = await db.execute(
        text("SELECT action, status, created_at FROM app.activity_log ORDER BY created_at DESC LIMIT 5")
    )
    activities = [
        {
            "action": r.action,
            "status": r.status,
            "created_at": str(r.created_at),
        }
        for r in activity_result.fetchall()
    ]

    pipeline_health_result = await db.execute(
        text("SELECT status, COUNT(*) AS count FROM app.pipelines GROUP BY status ORDER BY status")
    )
    pipeline_health = {r.status: r.count for r in pipeline_health_result.fetchall()}

    mart_result = await db.execute(
        text("""
            SELECT table_name, row_count, last_built_at,
                   CASE
                     WHEN last_built_at IS NULL THEN 'never_built'
                     WHEN last_built_at < NOW() - INTERVAL '24 hours' THEN 'stale'
                     ELSE 'fresh'
                   END AS freshness
            FROM app.data_assets
            WHERE asset_type = 'mart'
            ORDER BY last_built_at NULLS FIRST, table_name
            LIMIT 6
        """)
    )
    mart_freshness = [
        {
            "table_name": r.table_name,
            "row_count": r.row_count or 0,
            "last_built_at": str(r.last_built_at) if r.last_built_at else None,
            "freshness": r.freshness,
        }
        for r in mart_result.fetchall()
    ]

    failed_result = await db.execute(
        text("""
            SELECT name, status, last_run_at
            FROM app.pipelines
            WHERE status = 'failed'
            ORDER BY updated_at DESC
            LIMIT 5
        """)
    )
    failed_pipelines = [
        {"name": r.name, "status": r.status, "last_run_at": str(r.last_run_at) if r.last_run_at else None}
        for r in failed_result.fetchall()
    ]

    quality_result = await db.execute(
        text("""
            SELECT table_name, row_count, quality_status,
                   CASE
                     WHEN quality_status = 'failed' THEN 'Quality check gagal'
                     WHEN quality_status = 'warning' THEN 'Quality check perlu ditinjau'
                   END AS issue
            FROM app.data_assets
            WHERE asset_type = 'mart'
              AND quality_status IN ('warning', 'failed')
            ORDER BY CASE quality_status WHEN 'failed' THEN 0 ELSE 1 END, table_name
            LIMIT 8
        """)
    )
    data_quality_issues = [
        {
            "table_name": r.table_name,
            "issue": r.issue,
            "row_count": r.row_count or 0,
        }
        for r in quality_result.fetchall()
    ]

    impacted_result = await db.execute(
        text("""
            SELECT d.title, COUNT(*) AS issue_count
            FROM app.dashboard_dependencies dep
            JOIN app.dashboards d ON d.id = dep.dashboard_id
            JOIN app.data_assets m ON m.table_name = REPLACE(dep.mart_table_name, 'mart.', '') AND m.asset_type = 'mart'
            WHERE m.last_built_at IS NULL
               OR COALESCE(m.row_count, 0) = 0
               OR m.last_built_at < NOW() - INTERVAL '24 hours'
            GROUP BY d.id, d.title
            ORDER BY issue_count DESC, d.title
            LIMIT 6
        """)
    )
    impacted_dashboards = [
        {"title": r.title, "issue_count": r.issue_count}
        for r in impacted_result.fetchall()
    ]

    dashboard_ready_result = await db.execute(
        text("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE superset_uuid IS NOT NULL AND superset_uuid <> '') AS configured
            FROM app.dashboards
        """)
    )
    dashboard_ready = dashboard_ready_result.fetchone()
    ai_checks = [
        {"label": "Dashboard terhubung Superset", "passed": bool(dashboard_ready and dashboard_ready.configured)},
        {"label": "Mart tersedia", "passed": bool(mart_freshness)},
        {"label": "Tidak ada issue kualitas aktif", "passed": not data_quality_issues},
    ]

    return {
        "stats": {
            "pipelines": {"value": pipeline_total, "label": "Pipeline"},
            "active": {"value": pipeline_active, "label": "Sedang Berjalan"},
            "dashboards": {"value": dashboard_total, "label": "Dashboard"},
            "success_rate": {"value": f"{success_rate}%", "label": "Tingkat Keberhasilan"},
        },
        "activities": activities,
        "control_tower": {
            "pipeline_health": pipeline_health,
            "mart_freshness": mart_freshness,
            "impacted_dashboards": impacted_dashboards,
            "failed_pipelines": failed_pipelines,
            "data_quality_issues": data_quality_issues,
            "ai_readiness": {
                "ready": all(check["passed"] for check in ai_checks),
                "checks": ai_checks,
                "dashboard_total": dashboard_ready.total if dashboard_ready else 0,
                "dashboard_configured": dashboard_ready.configured if dashboard_ready else 0,
            },
        },
    }
