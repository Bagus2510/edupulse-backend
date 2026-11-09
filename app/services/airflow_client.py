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


async def set_dag_paused(dag_id: str, is_paused: bool) -> None:
    found = await wait_for_dag(dag_id)
    if not found:
        raise RuntimeError(f"DAG '{dag_id}' not found in Airflow after timeout")

    client = await get_http_client()
    resp = await client.patch(
        f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}",
        auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        json={"is_paused": is_paused},
    )
    resp.raise_for_status()


async def trigger_dag(
    dag_id: str,
    *,
    conf: dict | None = None,
    run_id: str | None = None,
    ensure_unpaused: bool = True,
) -> dict:
    found = await wait_for_dag(dag_id)
    if not found:
        raise RuntimeError(f"DAG '{dag_id}' not found in Airflow after timeout")

    if ensure_unpaused:
        await set_dag_paused(dag_id, False)
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


async def get_dag_status(dag_id: str, run_id: str | None = None) -> dict:
    client = await get_http_client()
    if run_id:
        encoded_run_id = urllib.parse.quote(run_id, safe="")
        resp = await client.get(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{encoded_run_id}",
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        )
        resp.raise_for_status()
        dag_run = resp.json()
    else:
        resp = await client.get(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            params={"order_by": "-execution_date", "limit": 1},
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return {"dag_id": dag_id, "run_id": None, "state": None, "tasks_total": 0, "tasks_completed": 0}
        dag_run = runs[0]

    dag_run_id = dag_run.get("dag_run_id") or dag_run.get("run_id") or run_id
    dag_state = dag_run.get("state")
    tasks_total = 0
    tasks_completed = 0
    if dag_run_id and dag_state in ("running", "queued"):
        try:
            encoded = urllib.parse.quote(dag_run_id, safe="")
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
        "run_id": dag_run_id,
        "state": dag_state,
        "tasks_total": tasks_total,
        "tasks_completed": tasks_completed,
        "execution_date": dag_run.get("execution_date"),
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
