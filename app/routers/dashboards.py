from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_db
from app.models.schemas import DashboardCreate

router = APIRouter(prefix="/api/dashboards", tags=["dashboards"])


@router.get("")
async def list_dashboards(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT id, title, description, superset_uuid, status, is_default, created_at, updated_at FROM app.dashboards ORDER BY id")
    )
    rows = result.fetchall()
    return [
        {
            "id": r.id,
            "title": r.title,
            "description": r.description,
            "superset_uuid": r.superset_uuid,
            "status": r.status,
            "is_default": r.is_default,
            "created_at": str(r.created_at),
            "updated_at": str(r.updated_at),
        }
        for r in rows
    ]


@router.get("/{dashboard_id}")
async def get_dashboard(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT * FROM app.dashboards WHERE id = :id"), {"id": dashboard_id}
    )
    r = result.fetchone()
    if not r:
        return {"error": "Dashboard not found"}
    return {
        "id": r.id,
        "title": r.title,
        "description": r.description,
        "superset_uuid": r.superset_uuid,
        "status": r.status,
        "is_default": r.is_default,
        "created_at": str(r.created_at),
        "updated_at": str(r.updated_at),
    }


@router.post("")
async def create_dashboard(payload: DashboardCreate, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text(
            "INSERT INTO app.dashboards (title, description, superset_uuid, status) VALUES (:t, :d, :u, :s)"
        ),
        {"t": payload.title, "d": payload.description, "u": payload.superset_uuid, "s": payload.status},
    )
    await db.commit()
    return {"message": "Dashboard created"}


@router.delete("/{dashboard_id}")
async def delete_dashboard(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text("DELETE FROM app.dashboards WHERE id = :id"), {"id": dashboard_id}
    )
    await db.commit()
    return {"message": "Dashboard deleted"}
