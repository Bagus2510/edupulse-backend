from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import (
    create_token,
    decode_token,
    get_current_user,
    verify_password,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: dict


class RefreshRequest(BaseModel):
    refresh_token: str


@router.post("/login", response_model=TokenResponse)
async def login(
    form: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select("*").select_from(text("app.users")).where(text("email = :email").bindparams(email=form.username))
    )
    user = result.mappings().first()
    if not user or not verify_password(form.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Email atau password salah")
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Akun tidak aktif")

    access = create_token({"sub": str(user["id"])}, "access")
    refresh = create_token({"sub": str(user["id"])}, "refresh")

    return TokenResponse(
        access_token=access,
        refresh_token=refresh,
        user={
            "id": user["id"],
            "email": user["email"],
            "full_name": user["full_name"],
            "role": user["role"],
            "avatar_initial": user["avatar_initial"],
        },
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    req: RefreshRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    payload = decode_token(req.refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Token type salah")
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token tidak valid")

    result = await db.execute(
        select("*").select_from(text("app.users")).where(text("id = :id").bindparams(id=int(user_id)))
    )
    user = result.mappings().first()
    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="User tidak ditemukan")

    access = create_token({"sub": str(user["id"])}, "access")
    refresh = create_token({"sub": str(user["id"])}, "refresh")

    return TokenResponse(
        access_token=access,
        refresh_token=refresh,
        user={
            "id": user["id"],
            "email": user["email"],
            "full_name": user["full_name"],
            "role": user["role"],
            "avatar_initial": user["avatar_initial"],
        },
    )


@router.get("/me")
async def me(current_user: Annotated[dict, Depends(get_current_user)]):
    return {
        "id": current_user["id"],
        "email": current_user["email"],
        "full_name": current_user["full_name"],
        "role": current_user["role"],
        "avatar_initial": current_user["avatar_initial"],
    }
