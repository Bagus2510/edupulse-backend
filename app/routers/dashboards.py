from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.schemas import DashboardCreate, DashboardDependencyCreate

router = APIRouter(prefix="/api/dashboards", tags=["dashboards"])


@router.get("")
async def list_dashboards(
    domain_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    where = ""
    params = {}
    if domain_id:
        where = "WHERE d.domain_id = :did"
        params = {"did": domain_id}

    result = await db.execute(
        text(f"""
            SELECT d.*,
                (SELECT COUNT(*) FROM app.dashboard_dependencies WHERE dashboard_id = d.id) AS dependency_count
            FROM app.dashboards d {where}
            ORDER BY d.id
        """),
        params,
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
            "domain_id": r.domain_id,
            "dependency_count": r.dependency_count,
            "created_at": str(r.created_at),
            "updated_at": str(r.updated_at),
        }
        for r in rows
    ]


@router.get("/{dashboard_id}")
async def get_dashboard(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT d.*,
                (SELECT COUNT(*) FROM app.dashboard_dependencies WHERE dashboard_id = d.id) AS dependency_count
            FROM app.dashboards d WHERE d.id = :id
        """),
        {"id": dashboard_id},
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
        "domain_id": r.domain_id,
        "dependency_count": r.dependency_count,
        "created_at": str(r.created_at),
        "updated_at": str(r.updated_at),
    }


@router.post("")
async def create_dashboard(payload: DashboardCreate, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text(
            "INSERT INTO app.dashboards (title, description, superset_uuid, status, domain_id) VALUES (:t, :d, :u, :s, :di)"
        ),
        {"t": payload.title, "d": payload.description, "u": payload.superset_uuid, "s": payload.status, "di": payload.domain_id},
    )
    await db.commit()
    return {"message": "Dashboard created"}


@router.put("/{dashboard_id}")
async def update_dashboard(dashboard_id: int, payload: DashboardCreate, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text(
            "UPDATE app.dashboards SET title = :t, description = :d, superset_uuid = :u, status = :s, domain_id = :di, updated_at = NOW() WHERE id = :id"
        ),
        {"t": payload.title, "d": payload.description, "u": payload.superset_uuid, "s": payload.status, "di": payload.domain_id, "id": dashboard_id},
    )
    await db.commit()
    return {"message": "Dashboard updated"}


@router.delete("/{dashboard_id}")
async def delete_dashboard(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    await db.execute(text("DELETE FROM app.dashboards WHERE id = :id"), {"id": dashboard_id})
    await db.commit()
    return {"message": "Dashboard deleted"}


# --- Dashboard Dependencies ---

@router.get("/{dashboard_id}/dependencies")
async def list_dependencies(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT * FROM app.dashboard_dependencies WHERE dashboard_id = :did ORDER BY mart_table_name"),
        {"did": dashboard_id},
    )
    return [{"id": r.id, "dashboard_id": r.dashboard_id, "mart_table_name": r.mart_table_name} for r in result.fetchall()]


@router.post("/{dashboard_id}/dependencies")
async def add_dependency(dashboard_id: int, payload: DashboardDependencyCreate, db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(
            text("INSERT INTO app.dashboard_dependencies (dashboard_id, mart_table_name) VALUES (:did, :mn)"),
            {"did": dashboard_id, "mn": payload.mart_table_name},
        )
        await db.commit()
    except Exception:
        raise HTTPException(status_code=409, detail="Dependensi sudah ada")
    return {"message": "Dependensi ditambahkan"}


@router.delete("/{dashboard_id}/dependencies/{mart_table}")
async def remove_dependency(dashboard_id: int, mart_table: str, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text("DELETE FROM app.dashboard_dependencies WHERE dashboard_id = :did AND mart_table_name = :mn"),
        {"did": dashboard_id, "mn": mart_table},
    )
    await db.commit()
    return {"message": "Dependensi dihapus"}


# --- Pipeline consumers for a dashboard ---

@router.get("/{dashboard_id}/consumers")
async def dashboard_consumers(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT DISTINCT p.id, p.name, p.status, p.dag_id
            FROM app.pipelines p
            JOIN app.pipeline_steps ps ON ps.pipeline_id = p.id
            JOIN app.dashboard_dependencies dd ON dd.mart_table_name = ps.dest_table
            WHERE dd.dashboard_id = :did
            ORDER BY p.name
        """),
        {"did": dashboard_id},
    )
    return [{"id": r.id, "name": r.name, "status": r.status, "dag_id": r.dag_id} for r in result.fetchall()]
