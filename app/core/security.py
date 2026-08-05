from datetime import datetime, timedelta, timezone
from typing import Annotated

import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_token(data: dict, token_type: str = "access") -> str:
    to_encode = data.copy()
    if token_type == "access":
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    else:
        expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": token_type})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid atau sudah kedaluwarsa",
            headers={"WWW-Authenticate": "Bearer"},
        )


ROLE_LEVELS = {
    "viewer": 10,
    "editor": 20,
    "admin": 30,
}


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    payload = decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Token type salah")
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Token tidak valid")
    result = await db.execute(
        select(
            text("id"), text("email"), text("full_name"),
            text("role"), text("avatar_initial"), text("is_active"),
        ).select_from(text("app.users")).where(text("id = :id").bindparams(id=int(user_id)))
    )
    user = result.mappings().first()
    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="User tidak ditemukan")
    return dict(user)


def require_role(minimum_role: str):
    """Create dependency enforcing viewer < editor < admin hierarchy."""
    if minimum_role not in ROLE_LEVELS:
        raise ValueError(f"Unknown role: {minimum_role}")

    async def role_dependency(
        current_user: Annotated[dict, Depends(get_current_user)],
    ) -> dict:
        current_role = current_user.get("role", "viewer")
        if ROLE_LEVELS.get(current_role, 0) < ROLE_LEVELS[minimum_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role {minimum_role} diperlukan untuk aksi ini",
            )
        return current_user

    return role_dependency


ViewerUserDep = Annotated[dict, Depends(require_role("viewer"))]
EditorUserDep = Annotated[dict, Depends(require_role("editor"))]
AdminUserDep = Annotated[dict, Depends(require_role("admin"))]
