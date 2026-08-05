"""Metadata synchronization and quality checks for raw and mart assets."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def _quote_identifier(value: str) -> str:
    """Quote a PostgreSQL identifier after validating its safe character set."""
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


async def sync_asset_metadata(
    db: AsyncSession,
    schema_name: str,
    table_name: str,
    asset_type: str,
    owner_id: int | None = None,
) -> int:
    """Upsert asset and column metadata from information_schema."""
    if schema_name not in {"raw", "mart"}:
        raise ValueError("Only raw and mart schemas are supported")

    table_identifier = f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    row_count = await db.scalar(text(f"SELECT COUNT(*) FROM {table_identifier}")) or 0
    column_result = await db.execute(
        text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema_name AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"schema_name": schema_name, "table_name": table_name},
    )
    columns = column_result.fetchall()

    asset_result = await db.execute(
        text("""
            INSERT INTO app.data_assets
                (schema_name, table_name, asset_type, owner_id, row_count, column_count,
                 last_loaded_at, last_built_at, updated_at)
            VALUES (:schema_name, :table_name, CAST(:asset_type AS VARCHAR(20)), :owner_id, :row_count, :column_count,
                    CASE WHEN CAST(:asset_type AS VARCHAR(20)) = 'raw' THEN NOW() ELSE NULL END,
                    CASE WHEN CAST(:asset_type AS VARCHAR(20)) = 'mart' THEN NOW() ELSE NULL END, NOW())
            ON CONFLICT (schema_name, table_name) DO UPDATE SET
                asset_type = EXCLUDED.asset_type,
                owner_id = COALESCE(EXCLUDED.owner_id, app.data_assets.owner_id),
                row_count = EXCLUDED.row_count,
                column_count = EXCLUDED.column_count,
                last_loaded_at = CASE WHEN EXCLUDED.asset_type = 'raw' THEN NOW() ELSE app.data_assets.last_loaded_at END,
                last_built_at = CASE WHEN EXCLUDED.asset_type = 'mart' THEN NOW() ELSE app.data_assets.last_built_at END,
                updated_at = NOW()
            RETURNING id
        """),
        {
            "schema_name": schema_name,
            "table_name": table_name,
            "asset_type": asset_type,
            "owner_id": owner_id,
            "row_count": row_count,
            "column_count": len(columns),
        },
    )
    asset_id = int(asset_result.scalar_one())

    for column in columns:
        await db.execute(
            text("""
                INSERT INTO app.data_columns
                    (asset_id, column_name, data_type, nullable, updated_at)
                VALUES (:asset_id, :column_name, :data_type, :nullable, NOW())
                ON CONFLICT (asset_id, column_name) DO UPDATE SET
                    data_type = EXCLUDED.data_type,
                    nullable = EXCLUDED.nullable,
                    updated_at = NOW()
            """),
            {
                "asset_id": asset_id,
                "column_name": column.column_name,
                "data_type": column.data_type,
                "nullable": column.is_nullable == "YES",
            },
        )

    await db.commit()
    return asset_id


async def backfill_asset_metadata(db: AsyncSession) -> int:
    """Register existing raw and mart tables missing from the metadata plane."""
    result = await db.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables t
        WHERE table_schema IN ('raw', 'mart')
          AND table_type = 'BASE TABLE'
          AND NOT EXISTS (
              SELECT 1 FROM app.data_assets a
              WHERE a.schema_name = t.table_schema AND a.table_name = t.table_name
          )
        ORDER BY table_schema, table_name
    """))
    created = 0
    for row in result.fetchall():
        try:
            await sync_asset_metadata(
                db,
                row.table_schema,
                row.table_name,
                "raw" if row.table_schema == "raw" else "mart",
            )
            created += 1
        except Exception:
            await db.rollback()
            logger.exception("Failed to backfill asset metadata for %s.%s", row.table_schema, row.table_name)
    return created


async def run_quality_checks(
    db: AsyncSession,
    schema_name: str,
    table_name: str,
    asset_id: int | None = None,
) -> dict[str, Any]:
    """Run row, null, and duplicate checks; persist results and asset status."""
    if asset_id is None:
        asset_result = await db.execute(
            text("SELECT id FROM app.data_assets WHERE schema_name = :schema_name AND table_name = :table_name"),
            {"schema_name": schema_name, "table_name": table_name},
        )
        asset_id = asset_result.scalar_one_or_none()
    if asset_id is None:
        raise ValueError("Asset metadata must be synchronized before quality checks")

    table_identifier = f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    row_count = int(await db.scalar(text(f"SELECT COUNT(*) FROM {table_identifier}")) or 0)
    column_result = await db.execute(
        text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema_name AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"schema_name": schema_name, "table_name": table_name},
    )
    columns = column_result.fetchall()

    checks: list[dict[str, Any]] = []
    checks.append({
        "check_name": "row_count_positive",
        "check_type": "row_count",
        "status": "passed" if row_count > 0 else "failed",
        "expected_value": "> 0",
        "actual_value": str(row_count),
        "severity": "critical" if row_count == 0 else "info",
        "message": "Asset memiliki data." if row_count > 0 else "Asset tidak memiliki baris data.",
    })

    null_rates: list[float] = []
    for column in columns:
        name = column.column_name
        quoted_column = _quote_identifier(name)
        null_count = int(await db.scalar(text(
            f"SELECT COUNT(*) FROM {table_identifier} WHERE {quoted_column} IS NULL"
        )) or 0)
        distinct_count = int(await db.scalar(text(
            f"SELECT COUNT(DISTINCT {quoted_column}) FROM {table_identifier}"
        )) or 0)
        null_rate = (null_count / row_count) if row_count else 0.0
        null_rates.append(null_rate)
        await db.execute(
            text("""
                UPDATE app.data_columns
                SET null_count = :null_count, distinct_count = :distinct_count, updated_at = NOW()
                WHERE asset_id = :asset_id AND column_name = :column_name
            """),
            {
                "asset_id": asset_id,
                "column_name": name,
                "null_count": null_count,
                "distinct_count": distinct_count,
            },
        )
        if null_rate > 0.05:
            checks.append({
                "check_name": f"{name}_null_rate",
                "check_type": "null_rate",
                "status": "warning" if null_rate <= 0.20 else "failed",
                "expected_value": "<= 0.05",
                "actual_value": f"{null_rate:.4f}",
                "severity": "high" if null_rate > 0.20 else "medium",
                "message": f"Kolom {name} memiliki null rate {null_rate:.1%}.",
            })

    await db.execute(text("DELETE FROM app.data_quality_results WHERE asset_id = :asset_id"), {"asset_id": asset_id})
    for check in checks:
        await db.execute(
            text("""
                INSERT INTO app.data_quality_results
                    (asset_id, check_name, check_type, status, expected_value, actual_value, severity, message)
                VALUES (:asset_id, :check_name, :check_type, :status, :expected_value, :actual_value, :severity, :message)
            """),
            {"asset_id": asset_id, **check},
        )

    failed = sum(check["status"] == "failed" for check in checks)
    warnings = sum(check["status"] == "warning" for check in checks)
    quality_status = "failed" if failed else "warning" if warnings else "passed"
    quality_score = max(0.0, round(100 - (failed * 35) - (warnings * 10), 2))
    await db.execute(
        text("""
            UPDATE app.data_assets
            SET row_count = :row_count, quality_status = :quality_status,
                quality_score = :quality_score, freshness_status = 'fresh', updated_at = NOW()
            WHERE id = :asset_id
        """),
        {
            "asset_id": asset_id,
            "row_count": row_count,
            "quality_status": quality_status,
            "quality_score": quality_score,
        },
    )
    await db.commit()

    return {
        "asset_id": asset_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "row_count": row_count,
        "quality_status": quality_status,
        "quality_score": quality_score,
        "checks": checks,
    }


async def inspect_asset_quality(db: AsyncSession, schema_name: str, table_name: str) -> dict[str, Any] | None:
    """Return persisted asset, column, and quality metadata."""
    asset_result = await db.execute(
        text("SELECT * FROM app.data_assets WHERE schema_name = :schema_name AND table_name = :table_name"),
        {"schema_name": schema_name, "table_name": table_name},
    )
    asset = asset_result.fetchone()
    if not asset:
        return None

    columns_result = await db.execute(
        text("SELECT * FROM app.data_columns WHERE asset_id = :asset_id ORDER BY id"),
        {"asset_id": asset.id},
    )
    checks_result = await db.execute(
        text("SELECT * FROM app.data_quality_results WHERE asset_id = :asset_id ORDER BY checked_at DESC, id"),
        {"asset_id": asset.id},
    )
    return {
        "asset": dict(asset._mapping),
        "columns": [dict(row._mapping) for row in columns_result.fetchall()],
        "checks": [dict(row._mapping) for row in checks_result.fetchall()],
    }
