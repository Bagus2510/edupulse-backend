import json
import logging

import asyncpg

from app.core.config import settings

logger = logging.getLogger(__name__)


async def _get_superset_pool():
    return await asyncpg.create_pool(
        host=settings.SUPERSET_DB_HOST,
        port=settings.SUPERSET_DB_PORT,
        user=settings.SUPERSET_DB_USER,
        password=settings.SUPERSET_DB_PASSWORD,
        database=settings.SUPERSET_DB_NAME,
    )


async def _get_edupulse_pool():
    return await asyncpg.create_pool(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
    )


async def get_dashboard_charts(dashboard_uuid: str) -> list[dict]:
    """Fetch chart IDs and table info from a dashboard by UUID."""
    pool = await _get_superset_pool()
    try:
        async with pool.acquire() as conn:
            # Try embedded_dashboards first
            row = await conn.fetchrow(
                """
                SELECT d.position_json
                FROM embedded_dashboards ed
                JOIN dashboards d ON d.id = ed.dashboard_id
                WHERE ed.uuid = $1
                """,
                dashboard_uuid,
            )

            if not row:
                row = await conn.fetchrow(
                    "SELECT position_json FROM dashboards WHERE uuid = $1",
                    dashboard_uuid,
                )

            if not row or not row["position_json"]:
                logger.warning("Dashboard with UUID %s not found or no position_json", dashboard_uuid)
                return []

            position_json = row["position_json"]
            if isinstance(position_json, str):
                position_json = json.loads(position_json)

            charts = []
            for key, val in position_json.items():
                if key.startswith("CHART-") and isinstance(val, dict):
                    meta = val.get("meta", {})
                    chart_id = meta.get("chartId") or val.get("id")
                    if chart_id:
                        # Look up table info from slices + tables
                        slice_row = await conn.fetchrow(
                            """
                            SELECT s.id, s.slice_name, s.viz_type,
                                   t.table_name, t.schema
                            FROM slices s
                            LEFT JOIN tables t ON t.id = (s.query_context::json->'datasource'->>'id')::int
                            WHERE s.id = $1
                            """,
                            chart_id,
                        )
                        charts.append({
                            "id": chart_id,
                            "name": meta.get("sliceName", f"Chart {chart_id}"),
                            "table_name": slice_row["table_name"] if slice_row else None,
                            "schema": slice_row["schema"] if slice_row else None,
                        })
            return charts
    finally:
        await pool.close()


async def get_chart_data(chart_id: int, table_name: str = None, schema: str = None) -> dict:
    """Fetch chart data by querying edupulse DB directly."""
    chart_name = f"Chart {chart_id}"

    # Get chart name from Superset DB if not provided
    if not table_name:
        spool = await _get_superset_pool()
        try:
            async with spool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT s.slice_name, s.viz_type,
                           t.table_name, t.schema
                    FROM slices s
                    LEFT JOIN tables t ON t.id = (s.query_context::json->'datasource'->>'id')::int
                    WHERE s.id = $1
                    """,
                    chart_id,
                )
                if row:
                    chart_name = row["slice_name"] or chart_name
                    table_name = row["table_name"]
                    schema = row["schema"]
        finally:
            await spool.close()

    if not table_name:
        return {"id": chart_id, "name": chart_name, "viz_type": "unknown", "data": None}

    # Query edupulse DB directly
    epool = await _get_edupulse_pool()
    try:
        async with epool.acquire() as conn:
            query = f'SELECT * FROM "{schema}"."{table_name}" LIMIT 50'
            rows = await conn.fetch(query)
            data = [dict(r) for r in rows]

            # Convert non-serializable types
            for row in data:
                for k, v in row.items():
                    if hasattr(v, 'isoformat'):
                        row[k] = str(v)

            return {
                "id": chart_id,
                "name": chart_name,
                "viz_type": "unknown",
                "data": data,
            }
    except Exception as e:
        logger.error("Failed to query %s.%s: %s", schema, table_name, e)
        return {"id": chart_id, "name": chart_name, "viz_type": "unknown", "data": None}
    finally:
        await epool.close()


async def get_dashboard_data(dashboard_uuid: str) -> list[dict]:
    """Fetch all chart data for a dashboard."""
    charts = await get_dashboard_charts(dashboard_uuid)
    results = []
    for chart in charts:
        data = await get_chart_data(chart["id"], chart.get("table_name"), chart.get("schema"))
        results.append(data)
    return results
