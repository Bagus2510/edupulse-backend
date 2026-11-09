from fastapi import APIRouter, HTTPException

from app.core.security import EditorUserDep
from app.models.schemas import DAGTriggerResponse, DAGStatusResponse
from app.services.airflow_client import trigger_dag, get_dag_status

router = APIRouter(
    prefix="/api/airflow",
    tags=["airflow"],
)


@router.post("/dags/{dag_id}/trigger", response_model=DAGTriggerResponse)
async def trigger(dag_id: str, current_user: EditorUserDep):
    try:
        result = await trigger_dag(dag_id, ensure_unpaused=False)
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
