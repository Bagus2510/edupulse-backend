import asyncio
import logging
import urllib.parse

import httpx

from app.core.config import settings
from app.core.http_client import get_http_client

logger = logging.getLogger(__name__)


async def wait_for_dag(dag_id: str, timeout: float = 60.0, interval: float = 2.0) -> bool:
    """Poll Airflow until DAG registered by scheduler."""
    client = await get_http_client()
    elapsed = 0.0
    while elapsed < timeout:
        try:
            url = f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}"
            resp = await client.get(url, auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD))
            if resp.status_code == 200:
                return True
            if resp.status_code != 404:
                logger.warning("Unexpected Airflow status %s for %s", resp.status_code, dag_id)
        except httpx.RequestError as exc:
            logger.warning("Airflow request error for %s: %s", dag_id, exc)
        await asyncio.sleep(interval)
        elapsed += interval
    logger.warning("DAG %s not found after %.1fs", dag_id, timeout)
    return False


async def ensure_dag_unpaused(dag_id: str) -> None:
    client = await get_http_client()
    try:
        resp = await client.patch(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}",
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            json={"is_paused": False},
        )
        if resp.status_code != 200:
            logger.warning("Failed to unpause DAG %s: %s", dag_id, resp.status_code)
    except httpx.HTTPError:
        logger.exception("Error unpausing DAG %s", dag_id)


async def trigger_dag(dag_id: str, *, conf: dict | None = None, run_id: str | None = None) -> dict:
    found = await wait_for_dag(dag_id)
    if not found:
        raise RuntimeError(f"DAG '{dag_id}' not found in Airflow after timeout")

    await ensure_dag_unpaused(dag_id)
    client = await get_http_client()
    payload: dict = {}
    if conf is not None:
        payload["conf"] = conf
    if run_id is not None:
        payload["dag_run_id"] = run_id

    resp = await client.post(
        f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
        auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        json=payload,
    )
    resp.raise_for_status()
    data = resp.json()
    return {"dag_id": dag_id, "run_id": data.get("dag_run_id") or data.get("run_id"), "state": data.get("state")}


async def get_dag_status(dag_id: str) -> dict:
    client = await get_http_client()
    resp = await client.get(
        f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
        auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        params={"order_by": "-execution_date", "limit": 1},
    )
    resp.raise_for_status()
    runs = resp.json().get("dag_runs", [])
    if not runs:
        return {"dag_id": dag_id, "state": None, "tasks_total": 0, "tasks_completed": 0}

    latest = runs[0]
    dag_state = latest.get("state")
    run_id = latest.get("dag_run_id") or latest.get("run_id")
    tasks_total = 0
    tasks_completed = 0
    if run_id and dag_state == "running":
        try:
            encoded = urllib.parse.quote(run_id, safe="")
            ti_resp = await client.get(
                f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{encoded}/taskInstances",
                auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            )
            if ti_resp.status_code == 200:
                task_instances = ti_resp.json().get("task_instances", [])
                tasks_total = len(task_instances)
                tasks_completed = sum(
                    1 for task in task_instances if task.get("state") in ("success", "failed", "skipped")
                )
                if any(task.get("state") == "failed" for task in task_instances):
                    dag_state = "failed"
        except httpx.HTTPError:
            logger.exception("Failed to fetch task instances for %s", dag_id)

    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": dag_state,
        "tasks_total": tasks_total,
        "tasks_completed": tasks_completed,
        "execution_date": latest.get("execution_date"),
    }


async def cancel_dag_run(dag_id: str, run_id: str) -> dict:
    client = await get_http_client()
    encoded = urllib.parse.quote(run_id, safe="")
    resp = await client.patch(
        f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{encoded}",
        auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        json={"state": "failed"},
    )
    resp.raise_for_status()
    data = resp.json()
    return {"dag_id": dag_id, "run_id": run_id, "state": data.get("state")}
