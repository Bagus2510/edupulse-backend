import json

import httpx

from app.core.config import settings


async def _login(client: httpx.AsyncClient) -> str:
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


def _csrf_headers(client: httpx.AsyncClient, access_token: str) -> dict:
    csrf = client.cookies.get("csrf_token") or client.cookies.get("XSRF-TOKEN", "")
    headers = {"Authorization": f"Bearer {access_token}"}
    if csrf:
        headers["X-CSRFToken"] = csrf
    return headers


async def get_dashboard_charts(dashboard_uuid: str) -> list[dict]:
    """Fetch chart IDs from a dashboard."""
    async with httpx.AsyncClient() as client:
        token = await _login(client)
        headers = _csrf_headers(client, token)

        resp = await client.get(
            f"{settings.SUPERSET_URL}/api/v1/dashboard/{dashboard_uuid}",
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()
        result = data.get("result", {})
        position_json = result.get("position_json")

        if not position_json:
            return []

        if isinstance(position_json, str):
            position_json = json.loads(position_json)

        charts = []
        for key, val in position_json.items():
            if key.startswith("CHART-") and isinstance(val, dict):
                meta = val.get("meta", {})
                chart_id = meta.get("chartId") or val.get("id")
                if chart_id:
                    charts.append({
                        "id": chart_id,
                        "name": meta.get("sliceName", f"Chart {chart_id}"),
                    })
        return charts


async def get_chart_data(chart_id: int) -> dict:
    """Fetch chart data (limited rows for AI analysis)."""
    async with httpx.AsyncClient() as client:
        token = await _login(client)
        headers = _csrf_headers(client, token)

        resp = await client.get(
            f"{settings.SUPERSET_URL}/api/v1/chart/{chart_id}",
            headers=headers,
        )
        resp.raise_for_status()
        chart = resp.json().get("result", {})

        query_context = chart.get("query_context")
        if not query_context:
            return {
                "id": chart_id,
                "name": chart.get("slice_name", f"Chart {chart_id}"),
                "viz_type": chart.get("viz_type", "unknown"),
                "data": None,
            }

        if isinstance(query_context, str):
            query_context = json.loads(query_context)

        data_resp = await client.post(
            f"{settings.SUPERSET_URL}/api/v1/chart/data",
            headers={**headers, "Content-Type": "application/json"},
            json=query_context,
        )

        if data_resp.status_code != 200:
            return {
                "id": chart_id,
                "name": chart.get("slice_name", f"Chart {chart_id}"),
                "viz_type": chart.get("viz_type", "unknown"),
                "data": None,
            }

        results = data_resp.json().get("result", [])
        rows = []
        for r in results:
            if "data" in r:
                rows = r["data"][:50]  # limit to 50 rows
                break

        return {
            "id": chart_id,
            "name": chart.get("slice_name", f"Chart {chart_id}"),
            "viz_type": chart.get("viz_type", "unknown"),
            "data": rows,
        }


async def get_dashboard_data(dashboard_uuid: str) -> list[dict]:
    """Fetch all chart data for a dashboard."""
    charts = await get_dashboard_charts(dashboard_uuid)
    results = []
    for chart in charts:
        data = await get_chart_data(chart["id"])
        results.append(data)
    return results
