from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_db
from app.core.security import EditorUserDep
from app.models.schemas import ActivityCreate

router = APIRouter(prefix="/api/activity", tags=["activity"])


@router.get("")
async def list_activity(
    limit: int = 10,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("SELECT id, action, details, status, created_at FROM app.activity_log ORDER BY created_at DESC LIMIT :l"),
        {"l": limit},
    )
    rows = result.fetchall()
    return [
        {
            "id": r.id,
            "action": r.action,
            "details": r.details,
            "status": r.status,
            "created_at": str(r.created_at),
        }
        for r in rows
    ]


@router.post("")
async def create_activity(
    payload: ActivityCreate,
    current_user: EditorUserDep,
    db: AsyncSession = Depends(get_db),
):
    await db.execute(
        text("INSERT INTO app.activity_log (action, details, status) VALUES (:a, :d, :s)"),
        {"a": payload.action, "d": payload.details, "s": payload.status},
    )
    await db.commit()
    return {"message": "Activity logged"}
