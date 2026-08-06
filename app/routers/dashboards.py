from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import AdminUserDep, EditorUserDep
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
async def create_dashboard(
    payload: DashboardCreate,
    current_user: EditorUserDep,
    db: AsyncSession = Depends(get_db),
):
    await db.execute(
        text(
            "INSERT INTO app.dashboards (title, description, superset_uuid, status, domain_id) VALUES (:t, :d, :u, :s, :di)"
        ),
        {"t": payload.title, "d": payload.description, "u": payload.superset_uuid, "s": payload.status, "di": payload.domain_id},
    )
    await db.commit()
    return {"message": "Dashboard created"}


@router.put("/{dashboard_id}")
async def update_dashboard(
    dashboard_id: int,
    payload: DashboardCreate,
    current_user: EditorUserDep,
    db: AsyncSession = Depends(get_db),
):
    await db.execute(
        text(
            "UPDATE app.dashboards SET title = :t, description = :d, superset_uuid = :u, status = :s, domain_id = :di, updated_at = NOW() WHERE id = :id"
        ),
        {"t": payload.title, "d": payload.description, "u": payload.superset_uuid, "s": payload.status, "di": payload.domain_id, "id": dashboard_id},
    )
    await db.commit()
    return {"message": "Dashboard updated"}


@router.delete("/{dashboard_id}")
async def delete_dashboard(
    dashboard_id: int,
    current_user: AdminUserDep,
    db: AsyncSession = Depends(get_db),
):
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
async def add_dependency(
    dashboard_id: int,
    payload: DashboardDependencyCreate,
    current_user: EditorUserDep,
    db: AsyncSession = Depends(get_db),
):
    # Validate that mart_table_name exists in pipeline_steps
    validation = await db.execute(
        text("""
            SELECT ps.dest_table, p.name AS pipeline_name, p.status AS pipeline_status
            FROM app.pipeline_steps ps
            JOIN app.pipelines p ON p.id = ps.pipeline_id
            WHERE ps.dest_table = :mn
        """),
        {"mn": payload.mart_table_name},
    )
    valid_table = validation.fetchone()
    if not valid_table:
        raise HTTPException(
            status_code=400,
            detail=f"Tabel '{payload.mart_table_name}' tidak ditemukan sebagai dest_table di pipeline manapun. Pastikan pipeline yang menghasilkan tabel ini sudah dibuat."
        )

    try:
        await db.execute(
            text("INSERT INTO app.dashboard_dependencies (dashboard_id, mart_table_name) VALUES (:did, :mn)"),
            {"did": dashboard_id, "mn": payload.mart_table_name},
        )
        await db.commit()
    except Exception:
        raise HTTPException(status_code=409, detail="Dependensi sudah ada")
    return {"message": "Dependensi ditambahkan", "valid": True, "pipeline": valid_table.pipeline_name}


@router.delete("/{dashboard_id}/dependencies/{mart_table}")
async def remove_dependency(
    dashboard_id: int,
    mart_table: str,
    current_user: EditorUserDep,
    db: AsyncSession = Depends(get_db),
):
    await db.execute(
        text("DELETE FROM app.dashboard_dependencies WHERE dashboard_id = :did AND mart_table_name = :mn"),
        {"did": dashboard_id, "mn": mart_table},
    )
    await db.commit()
    return {"message": "Dependensi dihapus"}


@router.get("/{dashboard_id}/dependencies/validate")
async def validate_dependencies(dashboard_id: int, db: AsyncSession = Depends(get_db)):
    """Validate all dependencies for a dashboard and return validation status."""
    result = await db.execute(
        text("SELECT * FROM app.dashboard_dependencies WHERE dashboard_id = :did ORDER BY mart_table_name"),
        {"did": dashboard_id},
    )
    deps = result.fetchall()

    if not deps:
        return {"valid": True, "total": 0, "valid_count": 0, "invalid_count": 0, "details": []}

    details = []
    valid_count = 0
    invalid_count = 0

    for dep in deps:
        validation = await db.execute(
            text("""
                SELECT ps.dest_table, p.name AS pipeline_name, p.status AS pipeline_status
                FROM app.pipeline_steps ps
                JOIN app.pipelines p ON p.id = ps.pipeline_id
                WHERE ps.dest_table = :mn
            """),
            {"mn": dep.mart_table_name},
        )
        valid_table = validation.fetchone()

        if valid_table:
            valid_count += 1
            details.append({
                "mart_table_name": dep.mart_table_name,
                "valid": True,
                "pipeline_name": valid_table.pipeline_name,
                "pipeline_status": valid_table.pipeline_status,
            })
        else:
            invalid_count += 1
            details.append({
                "mart_table_name": dep.mart_table_name,
                "valid": False,
                "pipeline_name": None,
                "pipeline_status": None,
                "error": "Tabel tidak ditemukan di pipeline manapun",
            })

    return {
        "valid": invalid_count == 0,
        "total": len(deps),
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "details": details,
    }


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
