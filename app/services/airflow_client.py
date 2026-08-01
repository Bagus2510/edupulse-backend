import httpx
import asyncio
import urllib.parse
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


async def wait_for_dag(dag_id: str, timeout: float = 60.0, interval: float = 2.0) -> bool:
    """Poll Airflow until the DAG is registered by the scheduler."""
    async with httpx.AsyncClient() as client:
        elapsed = 0.0
        while elapsed < timeout:
            try:
                url = f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}"
                logger.info("Checking DAG at %s (elapsed: %.1fs)", url, elapsed)
                resp = await client.get(
                    url,
                    auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
                )
                logger.info("Response %s for %s", resp.status_code, dag_id)
                if resp.status_code == 200:
                    logger.info("DAG %s found by scheduler after %.1fs", dag_id, elapsed)
                    return True
                if resp.status_code != 404:
                    logger.warning("Unexpected status %s for DAG %s: %s", resp.status_code, dag_id, resp.text[:200])
            except httpx.RequestError as e:
                logger.warning("Request error checking DAG %s: %s", dag_id, e)
            await asyncio.sleep(interval)
            elapsed += interval
    logger.warning("DAG %s not found after %.1fs timeout", dag_id, timeout)
    return False


async def ensure_dag_unpaused(dag_id: str) -> None:
    """Unpause a DAG so the scheduler can pick up its task instances."""
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.patch(
                f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}",
                auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
                json={"is_paused": False},
            )
            if resp.status_code == 200:
                logger.info("DAG %s unpaused successfully", dag_id)
            else:
                logger.warning("Failed to unpause DAG %s: %s %s", dag_id, resp.status_code, resp.text)
        except Exception as e:
            logger.error("Error unpausing DAG %s: %s", dag_id, e)


async def trigger_dag(dag_id: str, *, conf: dict | None = None, run_id: str | None = None) -> dict:
    """Wait for DAG to be available, unpause it, then trigger a run."""
    found = await wait_for_dag(dag_id)
    if not found:
        raise RuntimeError(f"DAG '{dag_id}' not found in Airflow after timeout")

    await ensure_dag_unpaused(dag_id)

    async with httpx.AsyncClient() as client:
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
        return {
            "dag_id": dag_id,
            "run_id": data.get("dag_run_id") or data.get("run_id"),
            "state": data.get("state"),
        }


async def get_dag_status(dag_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        # Get latest dag run
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

        # Get task instances to compute tasks_completed
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
                    ti_data = ti_resp.json()
                    task_instances = ti_data.get("task_instances", [])
                    tasks_total = len(task_instances)
                    tasks_completed = sum(
                        1 for ti in task_instances
                        if ti.get("state") in ("success", "failed", "skipped")
                    )
                    # Count queued tasks (not yet started)
                    tasks_queued = sum(
                        1 for ti in task_instances
                        if ti.get("state") == "queued"
                    )
                    # If any task is failed and no retries left, mark as failed
                    any_failed = any(ti.get("state") == "failed" for ti in task_instances)
                    if any_failed:
                        dag_state = "failed"
            except Exception:
                pass

        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": dag_state,
            "tasks_total": tasks_total,
            "tasks_completed": tasks_completed,
            "execution_date": latest.get("execution_date"),
        }


async def cancel_dag_run(dag_id: str, run_id: str) -> dict:
    """Set a DAG run state to 'failed' to interrupt a running pipeline."""
    async with httpx.AsyncClient() as client:
        encoded = urllib.parse.quote(run_id, safe="")
        resp = await client.patch(
            f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{encoded}",
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            json={"state": "failed"},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": data.get("state"),
        }
