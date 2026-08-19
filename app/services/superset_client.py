import httpx

from app.core.config import settings


async def get_guest_token(dashboard_uuid: str) -> str:
    timeout = httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        login_resp = await client.post(
            f"{settings.SUPERSET_URL}/api/v1/security/login",
            json={
                "username": settings.SUPERSET_USERNAME,
                "password": settings.SUPERSET_PASSWORD,
                "provider": "db",
                "refresh": True,
            },
        )
        login_resp.raise_for_status()
        access_token = login_resp.json()["access_token"]

        csrf_token = client.cookies.get("csrf_token") or client.cookies.get("XSRF-TOKEN", "")
        headers = {"Authorization": f"Bearer {access_token}"}
        if csrf_token:
            headers["X-CSRFToken"] = csrf_token

        guest_resp = await client.post(
            f"{settings.SUPERSET_URL}/api/v1/security/guest_token/",
            headers=headers,
            json={
                "user": {"username": "guest", "first_name": "Guest", "last_name": "User"},
                "resources": [{"type": "dashboard", "id": dashboard_uuid}],
                "rls": [],
            },
        )
        guest_resp.raise_for_status()
        return guest_resp.json()["token"]
