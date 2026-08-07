from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user

router = APIRouter(prefix="/api/lineage", tags=["lineage"])


@router.get("")
async def get_lineage(
    domain_id: int | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Build a lineage graph: raw tables -> pipelines -> mart tables -> dashboards."""
    nodes = []
    edges = []

    # Check which mart tables actually exist (used for filtering)
    existing_mart_tables_result = await db.execute(
        text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'mart'")
    )
    existing_mart_tables = {r.table_name for r in existing_mart_tables_result.fetchall()}

    # 1. Raw tables
    raw_result = await db.execute(
        text(
            "SELECT t.table_name, COALESCE(pg_stats.n_live_tup, 0) AS row_count "
            "FROM information_schema.tables t "
            "LEFT JOIN pg_stat_user_tables pg_stats ON pg_stats.schemaname = t.table_schema AND pg_stats.relname = t.table_name "
            "WHERE t.table_schema = 'raw' ORDER BY t.table_name"
        )
    )
    for r in raw_result.fetchall():
        node_id = f"raw_{r.table_name}"
        nodes.append({
            "id": node_id,
            "type": "raw",
            "label": r.table_name,
            "metadata": {"row_count": r.row_count},
        })

    # 2. Pipelines + steps
    pipeline_filter = ""
    params = {}
    if domain_id:
        pipeline_filter = "WHERE p.domain_id = :did"
        params = {"did": domain_id}

    pipeline_result = await db.execute(
        text(f"SELECT p.* FROM app.pipelines p {pipeline_filter} ORDER BY p.id"),
        params,
    )
    pipelines = pipeline_result.fetchall()

    for p in pipelines:
        p_node_id = f"pipeline_{p.id}"
        nodes.append({
            "id": p_node_id,
            "type": "pipeline",
            "label": p.name,
            "metadata": {"status": p.status, "dag_id": p.dag_id or ""},
        })

        # Steps
        step_result = await db.execute(
            text("SELECT * FROM app.pipeline_steps WHERE pipeline_id = :pid ORDER BY step_order"),
            {"pid": p.id},
        )
        steps = step_result.fetchall()
        prev_step_id = None

        for s in steps:
            s_node_id = f"step_{s.id}"
            nodes.append({
                "id": s_node_id,
                "type": "step",
                "label": s.name,
                "metadata": {"query_type": s.query_type, "step_order": s.step_order},
            })

            # Pipeline -> Step
            edges.append({"from": p_node_id, "to": s_node_id, "label": "contains"})

            # Raw -> Step (source)
            if s.source_table and s.source_table.startswith("raw."):
                source_name = s.source_table.split(".", 1)[1]
                raw_node = f"raw_{source_name}"
                edges.append({"from": raw_node, "to": s_node_id, "label": "source"})

            # Step -> Mart (dest)
            if s.dest_table and s.dest_table.startswith("mart."):
                dest_name = s.dest_table.split(".", 1)[1]
                # Skip if mart table doesn't actually exist
                if dest_name in existing_mart_tables:
                    mart_node = f"mart_{dest_name}"
                    edges.append({"from": s_node_id, "to": mart_node, "label": "produces"})

            # Step chain
            if prev_step_id:
                edges.append({"from": prev_step_id, "to": s_node_id, "label": "next"})
            prev_step_id = s_node_id

    # 3. Mart tables from metadata (only if table actually exists in mart schema)
    mart_result = await db.execute(
        text(
            "SELECT m.*, p.name AS pipeline_name "
            "FROM app.mart_table_metadata m "
            "LEFT JOIN app.pipelines p ON m.producing_pipeline_id = p.id "
            "ORDER BY m.table_name"
        )
    )
    existing_mart_nodes = {n["id"] for n in nodes if n["type"] == "mart"}
    
    for r in mart_result.fetchall():
        # Skip if mart table doesn't actually exist in database
        if r.table_name not in existing_mart_tables:
            continue
            
        node_id = f"mart_{r.table_name}"
        if node_id not in existing_mart_nodes:
            from app.services.mart_metadata import compute_freshness
            freshness = compute_freshness(r.last_built_at)
            nodes.append({
                "id": node_id,
                "type": "mart",
                "label": r.table_name,
                "metadata": {
                    "row_count": r.row_count,
                    "last_built_at": str(r.last_built_at) if r.last_built_at else None,
                    "freshness": freshness,
                    "pipeline_name": r.pipeline_name or "",
                },
            })

    # 4. Dashboards + dependencies
    dashboard_filter = ""
    dash_params = {}
    if domain_id:
        dashboard_filter = "WHERE d.domain_id = :did"
        dash_params = {"did": domain_id}

    dash_result = await db.execute(
        text(f"SELECT d.* FROM app.dashboards d {dashboard_filter} ORDER BY d.id"),
        dash_params,
    )
    for d in dash_result.fetchall():
        d_node_id = f"dashboard_{d.id}"
        nodes.append({
            "id": d_node_id,
            "type": "dashboard",
            "label": d.title,
            "metadata": {"status": d.status, "superset_uuid": d.superset_uuid or ""},
        })

        # Dependencies: mart -> dashboard
        dep_result = await db.execute(
            text("SELECT mart_table_name FROM app.dashboard_dependencies WHERE dashboard_id = :did"),
            {"did": d.id},
        )
        for dep in dep_result.fetchall():
            mart_name = dep.mart_table_name
            if mart_name.startswith("mart."):
                mart_name = mart_name.split(".", 1)[1]
            # Skip if mart table doesn't actually exist
            if mart_name not in existing_mart_tables:
                continue
            mart_node = f"mart_{mart_name}"
            edges.append({"from": mart_node, "to": d_node_id, "label": "consumes"})

    return {"nodes": nodes, "edges": edges}
