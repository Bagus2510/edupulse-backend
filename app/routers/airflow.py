from fastapi import APIRouter, HTTPException

from app.models.schemas import DAGTriggerResponse, DAGStatusResponse
from app.services.airflow_client import trigger_dag, get_dag_status, ensure_dag_unpaused

router = APIRouter(
    prefix="/api/airflow",
    tags=["airflow"],
)


@router.post("/dags/{dag_id}/trigger", response_model=DAGTriggerResponse)
async def trigger(dag_id: str):
    try:
        await ensure_dag_unpaused(dag_id)
        result = await trigger_dag(dag_id)
        return DAGTriggerResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Airflow error: {e}")


@router.get("/dags/{dag_id}/status", response_model=DAGStatusResponse)
async def status(dag_id: str):
    try:
        result = await get_dag_status(dag_id)
        return DAGStatusResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Airflow error: {e}")
