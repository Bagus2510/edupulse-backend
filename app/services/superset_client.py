import httpx

from app.core.config import settings


async def get_access_token() -> str:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{settings.SUPERSET_URL}/api/v1/security/login",
            json={
                "username": settings.SUPERSET_USERNAME,
                "password": settings.SUPERSET_PASSWORD,
                "provider": "db",
                "refresh": True,
            },
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


async def get_guest_token(dashboard_uuid: str) -> str:
    access_token = await get_access_token()

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{settings.SUPERSET_URL}/api/v1/security/guest_token/",
            headers={"Authorization": f"Bearer {access_token}"},
            json={
                "user": {
                    "username": "guest",
                    "first_name": "Guest",
                    "last_name": "User",
                },
                "resources": [
                    {"type": "dashboard", "id": dashboard_uuid}
                ],
                "rls": [],
            },
        )
        resp.raise_for_status()
        return resp.json()["token"]
