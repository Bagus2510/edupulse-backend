from __future__ import annotations

import asyncpg

from app.core.config import settings

_app_pool: asyncpg.Pool | None = None
_superset_pool: asyncpg.Pool | None = None


async def get_app_pool() -> asyncpg.Pool:
    global _app_pool
    if _app_pool is None:
        _app_pool = await asyncpg.create_pool(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            database=settings.DB_NAME,
            min_size=1,
            max_size=10,
            command_timeout=30,
        )
    return _app_pool


async def get_superset_pool() -> asyncpg.Pool:
    global _superset_pool
    if _superset_pool is None:
        _superset_pool = await asyncpg.create_pool(
            host=settings.SUPERSET_DB_HOST,
            port=settings.SUPERSET_DB_PORT,
            user=settings.SUPERSET_DB_USER,
            password=settings.SUPERSET_DB_PASSWORD,
            database=settings.SUPERSET_DB_NAME,
            min_size=1,
            max_size=5,
            command_timeout=30,
        )
    return _superset_pool


async def close_pools() -> None:
    global _app_pool, _superset_pool
    if _app_pool is not None:
        await _app_pool.close()
        _app_pool = None
    if _superset_pool is not None:
        await _superset_pool.close()
        _superset_pool = None
