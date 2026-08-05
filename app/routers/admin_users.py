from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import AdminUserDep, hash_password

router = APIRouter(prefix="/api/admin/users", tags=["admin-users"])

RoleName = Literal["viewer", "editor", "admin"]


class UserCreate(BaseModel):
    email: str = Field(min_length=5, max_length=255)
    full_name: str = Field(min_length=2, max_length=100)
    password: str = Field(min_length=8, max_length=128)
    role: RoleName = "viewer"
    is_active: bool = True


class UserUpdate(BaseModel):
    email: str = Field(min_length=5, max_length=255)
    full_name: str = Field(min_length=2, max_length=100)
    role: RoleName
    is_active: bool
    password: str | None = Field(default=None, min_length=8, max_length=128)


async def _get_user(db: AsyncSession, user_id: int):
    result = await db.execute(
        text("""
            SELECT id, email, full_name, role, avatar_initial, is_active, created_at, updated_at
            FROM app.users WHERE id = :id
        """),
        {"id": user_id},
    )
    return result.mappings().first()


async def _ensure_not_last_admin(
    db: AsyncSession,
    target_id: int,
    next_role: str,
    next_active: bool,
) -> None:
    current = await _get_user(db, target_id)
    if not current:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")
    removing_admin = current["role"] == "admin" and (
        next_role != "admin" or not next_active
    )
    if not removing_admin:
        return
    count_result = await db.execute(
        text("SELECT COUNT(*) FROM app.users WHERE role = 'admin' AND is_active = true")
    )
    if int(count_result.scalar() or 0) <= 1:
        raise HTTPException(status_code=409, detail="Minimal satu admin aktif harus dipertahankan")


@router.get("")
async def list_users(
    current_user: AdminUserDep,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict:
    offset = (page - 1) * limit
    count_result = await db.execute(text("SELECT COUNT(*) FROM app.users"))
    total = int(count_result.scalar() or 0)
    result = await db.execute(
        text("""
            SELECT id, email, full_name, role, avatar_initial, is_active, created_at, updated_at
            FROM app.users ORDER BY created_at DESC, id DESC
            LIMIT :limit OFFSET :offset
        """),
        {"limit": limit, "offset": offset},
    )
    return {
        "items": [dict(row) for row in result.mappings().all()],
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": max(1, -(-total // limit)),
    }


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: UserCreate,
    current_user: AdminUserDep,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    avatar_initial = payload.full_name.strip()[0].upper()
    try:
        result = await db.execute(
            text("""
                INSERT INTO app.users
                    (email, password_hash, full_name, role, avatar_initial, is_active)
                VALUES (:email, :password_hash, :full_name, :role, :avatar_initial, :is_active)
                RETURNING id
            """),
            {
                "email": payload.email.strip().lower(),
                "password_hash": hash_password(payload.password),
                "full_name": payload.full_name.strip(),
                "role": payload.role,
                "avatar_initial": avatar_initial,
                "is_active": payload.is_active,
            },
        )
        user_id = result.scalar_one()
        await db.execute(
            text("INSERT INTO app.activity_log (action, details, status, user_id) VALUES (:action, :details, 'success', :user_id)"),
            {
                "action": "User dibuat",
                "details": f"User {payload.email.strip().lower()} dibuat dengan role {payload.role}",
                "user_id": current_user["id"],
            },
        )
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Email user sudah terdaftar") from exc
    user = await _get_user(db, int(user_id))
    if not user:
        raise HTTPException(status_code=500, detail="User berhasil dibuat tetapi tidak dapat dibaca ulang")
    return dict(user)


@router.patch("/{user_id}")
async def update_user(
    user_id: int,
    payload: UserUpdate,
    current_user: AdminUserDep,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    if user_id == current_user["id"] and not payload.is_active:
        raise HTTPException(status_code=409, detail="Admin tidak dapat menonaktifkan akun sendiri")

    await _ensure_not_last_admin(db, user_id, payload.role, payload.is_active)
    password_hash = hash_password(payload.password) if payload.password else None
    avatar_initial = payload.full_name.strip()[0].upper()
    await db.execute(
        text("""
            UPDATE app.users
            SET email = :email,
                full_name = :full_name,
                role = :role,
                is_active = :is_active,
                avatar_initial = :avatar_initial,
                password_hash = COALESCE(:password_hash, password_hash),
                updated_at = NOW()
            WHERE id = :id
        """),
        {
            "id": user_id,
            "email": payload.email.strip().lower(),
            "full_name": payload.full_name.strip(),
            "role": payload.role,
            "is_active": payload.is_active,
            "avatar_initial": avatar_initial,
            "password_hash": password_hash,
        },
    )
    await db.execute(
        text("INSERT INTO app.activity_log (action, details, status, user_id) VALUES (:action, :details, 'success', :user_id)"),
        {
            "action": "User diperbarui",
            "details": f"User ID {user_id} diubah menjadi role {payload.role}, active={payload.is_active}",
            "user_id": current_user["id"],
        },
    )
    await db.commit()
    user = await _get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")
    return dict(user)


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: int,
    current_user: AdminUserDep,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> None:
    if user_id == current_user["id"]:
        raise HTTPException(status_code=409, detail="Admin tidak dapat menghapus akun sendiri")

    target = await _get_user(db, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    if target["role"] == "admin":
        raise HTTPException(status_code=409, detail="Admin tidak dapat dihapus")

    count_result = await db.execute(
        text("SELECT COUNT(*) FROM app.users WHERE role = 'admin' AND is_active = true AND id != :id"),
        {"id": user_id},
    )
    remaining_admins = int(count_result.scalar() or 0)

    await db.execute(text("DELETE FROM app.users WHERE id = :id"), {"id": user_id})
    await db.execute(
        text("INSERT INTO app.activity_log (action, details, status, user_id) VALUES (:action, :details, 'success', :user_id)"),
        {
            "action": "User dihapus",
            "details": f"User {target['email']} (ID {user_id}) dihapus",
            "user_id": current_user["id"],
        },
    )
    await db.commit()
