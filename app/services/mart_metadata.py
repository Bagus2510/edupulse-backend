import json
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def upsert_mart_metadata(
    db: AsyncSession,
    dest_table: str,
    pipeline_id: int | None = None,
    step_id: int | None = None,
):
    """Register or update a mart table in metadata after pipeline step completes."""
    if not dest_table or not dest_table.startswith("mart."):
        return

    parts = dest_table.split(".", 1)
    schema_name = parts[0] if len(parts) > 1 else "mart"
    table_name = parts[1] if len(parts) > 1 else dest_table

    # Get row count
    try:
        count_result = await db.execute(
            text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
        )
        row_count = count_result.scalar() or 0
    except Exception:
        row_count = 0

    # Get column info
    try:
        col_result = await db.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
            ),
            {"s": schema_name, "t": table_name},
        )
        column_info = [{"name": r.column_name, "type": r.data_type} for r in col_result.fetchall()]
    except Exception:
        column_info = []

    # Upsert
    await db.execute(
        text("""
            INSERT INTO app.mart_table_metadata (table_name, schema_name, producing_pipeline_id, producing_step_id, last_built_at, row_count, column_info, updated_at)
            VALUES (:tn, :sn, :pid, :sid, NOW(), :rc, :ci, NOW())
            ON CONFLICT (table_name) DO UPDATE SET
                producing_pipeline_id = COALESCE(EXCLUDED.producing_pipeline_id, app.mart_table_metadata.producing_pipeline_id),
                producing_step_id = COALESCE(EXCLUDED.producing_step_id, app.mart_table_metadata.producing_step_id),
                last_built_at = NOW(),
                row_count = EXCLUDED.row_count,
                column_info = EXCLUDED.column_info,
                updated_at = NOW()
        """),
        {"tn": table_name, "sn": schema_name, "pid": pipeline_id, "sid": step_id, "rc": row_count, "ci": json.dumps(column_info)},
    )
    await db.commit()


async def register_all_mart_tables(db: AsyncSession, pipeline_id: int):
    """Register all mart dest_tables from a pipeline's steps."""
    result = await db.execute(
        text("SELECT id, dest_table FROM app.pipeline_steps WHERE pipeline_id = :pid AND dest_table LIKE 'mart.%'"),
        {"pid": pipeline_id},
    )
    for row in result.fetchall():
        await upsert_mart_metadata(db, row.dest_table, pipeline_id=pipeline_id, step_id=row.id)


def compute_freshness(last_built_at: datetime | None) -> str:
    """Compute freshness status from last_built_at timestamp."""
    if not last_built_at:
        return "never_built"
    now = datetime.now()
    diff = now - last_built_at
    if diff.total_seconds() < 86400:  # 24 hours
        return "fresh"
    elif diff.total_seconds() < 604800:  # 7 days
        return "stale"
    return "old"
