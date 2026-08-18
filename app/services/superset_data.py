import json
import logging
import re

from app.core.pg_pool import get_app_pool, get_superset_pool

logger = logging.getLogger(__name__)


async def _get_superset_pool():
    return await get_superset_pool()


async def _get_edupulse_pool():
    return await get_app_pool()


async def get_dashboard_charts(dashboard_uuid: str) -> list[dict]:
    """Fetch chart IDs and table info from a dashboard by UUID."""
    if not dashboard_uuid:
        logger.warning("get_dashboard_charts called with empty dashboard_uuid")
        return []

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
                        if slice_row:
                            charts.append({
                                "id": chart_id,
                                "name": meta.get("sliceName", slice_row["slice_name"] or f"Chart {chart_id}"),
                                "viz_type": slice_row["viz_type"] or "unknown",
                                "table_name": slice_row["table_name"],
                                "schema": slice_row["schema"],
                            })
                        else:
                            # Fallback: try to get table from params JSON
                            params_row = await conn.fetchrow(
                                """
                                SELECT s.slice_name, s.viz_type, s.params
                                FROM slices s WHERE s.id = $1
                                """,
                                chart_id,
                            )
                            table_name = None
                            schema = None
                            if params_row and params_row["params"]:
                                try:
                                    params = json.loads(params_row["params"]) if isinstance(params_row["params"], str) else params_row["params"]
                                    # Try common Superset param keys
                                    table_name = params.get("table_name") or params.get("table")
                                    schema = params.get("schema") or params.get("database_schema")
                                except (json.JSONDecodeError, TypeError):
                                    pass
                            charts.append({
                                "id": chart_id,
                                "name": meta.get("sliceName", (params_row["slice_name"] if params_row else None) or f"Chart {chart_id}"),
                                "viz_type": (params_row["viz_type"] if params_row else None) or "unknown",
                                "table_name": table_name,
                                "schema": schema,
                            })

            logger.info("Found %d charts for dashboard %s", len(charts), dashboard_uuid)
            return charts
    except Exception as e:
        logger.error("Error fetching charts for dashboard %s: %s", dashboard_uuid, e)
        return []


async def get_chart_data(chart_id: int, table_name: str | None = None, schema: str | None = None) -> dict:
    """Fetch chart data by querying edupulse DB directly."""
    chart_name = f"Chart {chart_id}"

    # Get chart name from Superset DB if not provided
    if not table_name:
        spool = await _get_superset_pool()
        try:
            async with spool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT s.slice_name, s.viz_type, s.params,
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
                    # Fallback: try params JSON
                    if not table_name and row["params"]:
                        try:
                            params = json.loads(row["params"]) if isinstance(row["params"], str) else row["params"]
                            table_name = params.get("table_name") or params.get("table")
                            schema = params.get("schema") or params.get("database_schema")
                        except (json.JSONDecodeError, TypeError):
                            pass
        finally:
            pass

    if not table_name:
        logger.warning("No table_name for chart %d (%s)", chart_id, chart_name)
        return {"id": chart_id, "name": chart_name, "viz_type": "unknown", "data": None}

    # Identifiers cannot use bind parameters; validate metadata-derived names first.
    if not schema or not re.fullmatch(r"[A-Za-z0-9_]+", schema) or not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        logger.warning("Rejected unsafe chart identifier: %s.%s", schema, table_name)
        return {"id": chart_id, "name": chart_name, "viz_type": "unknown", "data": None}

    # Query edupulse DB directly
    epool = await _get_edupulse_pool()
    try:
        async with epool.acquire() as conn:
            query = f'SELECT * FROM "{schema}"."{table_name}" LIMIT 50'
            logger.info("Querying chart data: %s", query)
            rows = await conn.fetch(query)
            data = [dict(r) for r in rows]

            # Convert non-serializable types
            for row in data:
                for k, v in row.items():
                    if hasattr(v, 'isoformat'):
                        row[k] = str(v)

            logger.info("Got %d rows for chart %d (%s)", len(data), chart_id, chart_name)
            return {
                "id": chart_id,
                "name": chart_name,
                "viz_type": "unknown",
                "data": data,
            }
    except Exception as e:
        logger.error("Failed to query %s.%s for chart %d: %s", schema, table_name, chart_id, e)
        return {"id": chart_id, "name": chart_name, "viz_type": "unknown", "data": None}


async def get_dashboard_data(dashboard_uuid: str) -> list[dict]:
    """Fetch all chart data for a dashboard."""
    charts = await get_dashboard_charts(dashboard_uuid)
    results = []
    for chart in charts:
        data = await get_chart_data(
            chart["id"],
            chart.get("table_name") or None,
            chart.get("schema") or None,
        )
        results.append(data)
    return results
