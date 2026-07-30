import httpx

from app.core.config import settings


def _auth() -> tuple[str, str]:
    return (settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD)


async def ensure_dag_unpaused(dag_id: str) -> None:
    """Unpause DAG so it can be triggered."""
    async with httpx.AsyncClient() as client:
        await client.patch(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}",
            auth=_auth(),
            json={"is_paused": False},
        )


async def trigger_dag(dag_id: str) -> dict:
    await ensure_dag_unpaused(dag_id)
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=_auth(),
            json={"conf": {}},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "dag_id": dag_id,
            "run_id": data.get("run_id") or data.get("dag_run_id"),
            "state": data.get("state"),
        }


async def get_dag_status(dag_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=_auth(),
            params={"order_by": "-execution_date", "limit": 1},
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return {
                "dag_id": dag_id,
                "run_id": None,
                "state": None,
                "execution_date": None,
            }
        latest = runs[0]
        return {
            "dag_id": dag_id,
            "run_id": latest.get("run_id") or latest.get("dag_run_id"),
            "state": latest.get("state"),
            "execution_date": latest.get("execution_date"),
        }
