import httpx

from app.core.config import settings


async def get_guest_token(dashboard_uuid: str) -> str:
    async with httpx.AsyncClient() as client:
        # Step 1: Login — cookies (termasuk CSRF) otomatis tersimpan di client
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

        # Step 2: Ambil CSRF token dari cookies
        csrf_token = client.cookies.get("csrf_token")
        if not csrf_token:
            # Fallback: coba dari XSRF-TOKEN cookie
            csrf_token = client.cookies.get("XSRF-TOKEN", "")

        # Step 3: Generate guest token — bawa cookies + CSRF header
        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        if csrf_token:
            headers["X-CSRFToken"] = csrf_token

        guest_resp = await client.post(
            f"{settings.SUPERSET_URL}/api/v1/security/guest_token/",
            headers=headers,
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
        guest_resp.raise_for_status()
        return guest_resp.json()["token"]
