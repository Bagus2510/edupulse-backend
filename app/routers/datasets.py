import io
import logging
import re

import pandas as pd
from typing import Annotated

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.schemas import TableInfo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])

MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _sanitize_table_name(filename: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]", "_", filename)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name or "uploaded_data"


@router.get("", response_model=list[TableInfo])
async def list_datasets(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    result = await db.execute(
        text(
            "SELECT t.table_schema, t.table_name, "
            "COALESCE(pg_stats.n_live_tup, 0) AS row_count, "
            "COALESCE(col_info.column_count, 0) AS column_count "
            "FROM information_schema.tables t "
            "LEFT JOIN pg_stat_user_tables pg_stats "
            "ON pg_stats.schemaname = t.table_schema AND pg_stats.relname = t.table_name "
            "LEFT JOIN ("
            "  SELECT table_schema, table_name, COUNT(*) AS column_count "
            "  FROM information_schema.columns "
            "  WHERE table_schema = 'raw' "
            "  GROUP BY table_schema, table_name"
            ") col_info ON col_info.table_schema = t.table_schema AND col_info.table_name = t.table_name "
            "WHERE t.table_schema = 'raw' "
            "ORDER BY t.table_name"
        )
    )
    rows = result.fetchall()
    return [
        TableInfo(
            schema_name=r.table_schema,
            table_name=r.table_name,
            full_name=f"{r.table_schema}.{r.table_name}",
            row_count=r.row_count,
            column_count=r.column_count,
        )
        for r in rows
    ]


@router.get("/mart", response_model=list[TableInfo])
async def list_mart_datasets(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    result = await db.execute(
        text(
            "SELECT t.table_schema, t.table_name, "
            "COALESCE(pg_stats.n_live_tup, 0) AS row_count, "
            "COALESCE(col_info.column_count, 0) AS column_count, "
            "m.last_built_at, m.row_count AS meta_row_count "
            "FROM information_schema.tables t "
            "LEFT JOIN pg_stat_user_tables pg_stats "
            "ON pg_stats.schemaname = t.table_schema AND pg_stats.relname = t.table_name "
            "LEFT JOIN ("
            "  SELECT table_schema, table_name, COUNT(*) AS column_count "
            "  FROM information_schema.columns "
            "  WHERE table_schema = 'mart' "
            "  GROUP BY table_schema, table_name"
            ") col_info ON col_info.table_schema = t.table_schema AND col_info.table_name = t.table_name "
            "LEFT JOIN app.mart_table_metadata m ON m.table_name = t.table_name "
            "WHERE t.table_schema = 'mart' "
            "ORDER BY t.table_name"
        )
    )
    rows = result.fetchall()

    from app.services.mart_metadata import compute_freshness
    items = []
    for r in rows:
        freshness = compute_freshness(r.last_built_at)
        items.append({
            "schema_name": r.table_schema,
            "table_name": r.table_name,
            "full_name": f"{r.table_schema}.{r.table_name}",
            "row_count": r.meta_row_count or r.row_count,
            "column_count": r.column_count,
            "last_built_at": str(r.last_built_at) if r.last_built_at else None,
            "freshness": freshness,
        })
    return items


@router.get("/mart-tables")
async def list_mart_table_metadata(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """List all mart tables with metadata from pipeline tracking."""
    result = await db.execute(
        text("""
            SELECT m.*, p.name AS pipeline_name
            FROM app.mart_table_metadata m
            LEFT JOIN app.pipelines p ON m.producing_pipeline_id = p.id
            ORDER BY m.table_name
        """)
    )
    from app.services.mart_metadata import compute_freshness
    items = []
    for r in result.fetchall():
        freshness = compute_freshness(r.last_built_at)
        items.append({
            "id": r.id,
            "table_name": r.table_name,
            "schema_name": r.schema_name,
            "producing_pipeline_id": r.producing_pipeline_id,
            "producing_step_id": r.producing_step_id,
            "last_built_at": str(r.last_built_at) if r.last_built_at else None,
            "row_count": r.row_count,
            "column_info": r.column_info,
            "freshness": freshness,
            "pipeline_name": r.pipeline_name or "",
        })
    return items


@router.get("/{schema_name}.{table_name}/info")
async def table_info(
    schema_name: Annotated[str, Path(description="Schema name (raw or mart)")],
    table_name: Annotated[str, Path(description="Table name")],
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    count_result = await db.execute(
        text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
    )
    row_count = count_result.scalar()

    col_result = await db.execute(
        text(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t "
            "ORDER BY ordinal_position"
        ),
        {"s": schema_name, "t": table_name},
    )
    columns = [{"name": r.column_name, "type": r.data_type} for r in col_result.fetchall()]

    return {
        "schema": schema_name,
        "table": table_name,
        "row_count": row_count,
        "columns": columns,
    }


@router.post("/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    ext = "." + file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Format tidak didukung: {ext}. Gunakan CSV atau Excel.")

    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File terlalu besar. Maksimal 100 MB.")

    table_name = _sanitize_table_name(file.filename.rsplit(".", 1)[0])

    try:
        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(contents))
        else:
            df = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        logger.error("Failed to parse file %s: %s", file.filename, e)
        raise HTTPException(status_code=400, detail=f"Gagal membaca file: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="File kosong atau tidak memiliki data.")

    df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", c).strip("_").lower() or f"col_{i}" for i, c in enumerate(df.columns)]

    try:
        await db.execute(text(f'DROP TABLE IF EXISTS "raw"."{table_name}"'))

        cols_def = ", ".join(f'"{col}" {dtype}' for col, dtype in zip(df.columns, [_map_dtype(t) for t in df.dtypes]))
        await db.execute(text(f'CREATE TABLE "raw"."{table_name}" ({cols_def})'))

        for _, row in df.iterrows():
            placeholders = ", ".join([f":{c}" for c in df.columns])
            await db.execute(
                text(f'INSERT INTO "raw"."{table_name}" VALUES ({placeholders})'),
                {c: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)) for c, v in zip(df.columns, row)},
            )

        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to insert data: %s", e)
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Gagal menyimpan data: {e}")

    return {
        "message": f"Berhasil upload {len(df)} baris ke raw.{table_name}",
        "table": f"raw.{table_name}",
        "rows": len(df),
        "columns": len(df.columns),
    }


@router.delete("/{schema_name}.{table_name}")
async def delete_dataset(
    schema_name: Annotated[str, Path(description="Schema name (raw or mart)")],
    table_name: Annotated[str, Path(description="Table name")],
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if schema_name not in ("raw", "mart"):
        raise HTTPException(status_code=400, detail="Hanya schema raw/mart yang boleh dihapus")

    await db.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}"'))
    await db.commit()
    return {"message": f"Tabel {schema_name}.{table_name} dihapus"}


def _map_dtype(pandas_dtype) -> str:
    s = str(pandas_dtype)
    if "int" in s:
        return "BIGINT"
    if "float" in s:
        return "DOUBLE PRECISION"
    if "bool" in s:
        return "BOOLEAN"
    if "datetime" in s:
        return "TIMESTAMP"
    return "TEXT"
