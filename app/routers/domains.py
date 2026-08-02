from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.schemas import DomainCreate, DomainResponse

router = APIRouter(prefix="/api/domains", tags=["domains"])


@router.get("", response_model=list[DomainResponse])
async def list_domains(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT d.*,
                (SELECT COUNT(*) FROM app.pipelines WHERE domain_id = d.id) AS pipeline_count,
                (SELECT COUNT(*) FROM app.dashboards WHERE domain_id = d.id) AS dashboard_count
            FROM app.domains d ORDER BY d.id
        """)
    )
    return [
        DomainResponse(
            id=r.id, name=r.name, description=r.description or "",
            icon=r.icon, color=r.color,
            pipeline_count=r.pipeline_count, dashboard_count=r.dashboard_count,
        )
        for r in result.fetchall()
    ]


@router.get("/{domain_id}")
async def get_domain(domain_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT * FROM app.domains WHERE id = :id"), {"id": domain_id})
    r = result.fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Domain tidak ditemukan")
    return {"id": r.id, "name": r.name, "description": r.description, "icon": r.icon, "color": r.color}


@router.post("")
async def create_domain(payload: DomainCreate, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text("INSERT INTO app.domains (name, description, icon, color) VALUES (:n, :d, :i, :c)"),
        {"n": payload.name, "d": payload.description, "i": payload.icon, "c": payload.color},
    )
    await db.commit()
    return {"message": "Domain dibuat"}


@router.put("/{domain_id}")
async def update_domain(domain_id: int, payload: DomainCreate, db: AsyncSession = Depends(get_db)):
    await db.execute(
        text("UPDATE app.domains SET name = :n, description = :d, icon = :i, color = :c WHERE id = :id"),
        {"n": payload.name, "d": payload.description, "i": payload.icon, "c": payload.color, "id": domain_id},
    )
    await db.commit()
    return {"message": "Domain diperbarui"}


@router.delete("/{domain_id}")
async def delete_domain(domain_id: int, db: AsyncSession = Depends(get_db)):
    await db.execute(text("DELETE FROM app.domains WHERE id = :id"), {"id": domain_id})
    await db.commit()
    return {"message": "Domain dihapus"}
