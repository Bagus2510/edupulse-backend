import json
import random
import string
from pathlib import Path

from app.core.config import settings


def _get_dags_dir() -> Path:
    dags_dir = Path(__file__).resolve().parent.parent.parent.parent / "Apache_Airflow" / "dags"
    dags_dir.mkdir(parents=True, exist_ok=True)
    return dags_dir


def _indent_user_code(code: str, indent: int = 4) -> str:
    """Indent user code properly inside a function body."""
    lines = code.strip().splitlines()
    if not lines:
        return ""
    padded = " " * indent
    return "\n".join(padded + line if line.strip() else "" for line in lines)


def generate_dag_id(pipeline_name: str) -> str:
    """Generate a unique DAG ID from pipeline name: {sanitized_name}_{3-char-hex}."""
    safe_name = "".join(c if c.isalnum() else "_" for c in pipeline_name).strip("_").lower()
    suffix = "".join(random.choices(string.hexdigits[:16], k=3))
    return f"{safe_name}_{suffix}"


def generate_dag_content(dag_id: str, pipeline_name: str, steps: list[dict], schedule_interval: str = "", max_active_runs: int = 1, on_failure_callback: str = "") -> str:
    """Generate DAG file content. Returns the Python source code."""
    safe_name = "".join(c if c.isalnum() else "_" for c in pipeline_name)

    step_functions = []
    task_lines = []
    task_ids = []
    var_names = []

    for i, step in enumerate(steps, 1):
        func_name = f"step_{i}"
        var_names.append(func_name)
        raw_task_id = step.get("name", f"Step {i}")
        task_id = "".join(c if c.isalnum() or c == "_" else "_" for c in raw_task_id).strip("_") or f"step_{i}"
        task_ids.append(task_id)
        query_type = step.get("query_type", "sql")
        query = step["query"]
        source_table = step.get("source_table", "")
        dest_table = step.get("dest_table", "")
        execution_timeout = step.get("execution_timeout", 300)
        retries = step.get("retries", 1)
        retry_delay = step.get("retry_delay", 5)

        if query_type == "sql":
            safe_query = json.dumps(query)
            source_comment = f"    # Source: {source_table}\n" if source_table else ""
            dest_comment = f"    # Dest: {dest_table}\n" if dest_table else ""
            is_select = query.strip().upper().startswith("SELECT")
            if is_select:
                body = (
                    f"def {func_name}():\n"
                    f"{source_comment}{dest_comment}"
                    f'    hook = PostgresHook(postgres_conn_id="edupulse")\n'
                    f"    conn = hook.get_conn()\n"
                    f"    cursor = conn.cursor()\n"
                    f"    cursor.execute({safe_query})\n"
                    f"    rows = cursor.fetchall()\n"
                    f'    print(f"Fetched {{len(rows)}} rows")\n'
                    f"    for row in rows[:10]:\n"
                    f"        print(row)\n"
                    f"    cursor.close()\n"
                    f"    conn.close()"
                )
            else:
                body = (
                    f"def {func_name}():\n"
                    f"{source_comment}{dest_comment}"
                    f'    hook = PostgresHook(postgres_conn_id="edupulse")\n'
                    f"    conn = hook.get_conn()\n"
                    f"    cursor = conn.cursor()\n"
                    f"    cursor.execute({safe_query})\n"
                    f"    conn.commit()\n"
                    f"    cursor.close()\n"
                    f"    conn.close()"
                )
        else:
            user_code = _indent_user_code(query)
            if source_table or dest_table:
                body = (
                    f"def {func_name}():\n"
                    f'    hook = PostgresHook(postgres_conn_id="edupulse")\n'
                    f"    engine = hook.get_sqlalchemy_engine()\n"
                )
                if source_table:
                    body += f'    df = pd.read_sql("SELECT * FROM {source_table}", engine)\n'
                body += (
                    f"    # --- user transformation ---\n"
                    f"{user_code}\n"
                    f"    # --- end transformation ---\n"
                )
                if dest_table:
                    body += (
                        f'    if not df.empty:\n'
                        f'        df.to_sql("{dest_table}", engine, if_exists="replace", index=False)\n'
                        f'        print(f"Saved {{len(df)}} rows to {dest_table}")\n'
                    )
            else:
                body = (
                    f"def {func_name}():\n"
                    f"    # --- user transformation ---\n"
                    f"{user_code}\n"
                    f"    # --- end transformation ---\n"
                )

        step_functions.append(body)
        task_lines.append(
            f'    {func_name} = PythonOperator(\n'
            f'        task_id="{task_id}",\n'
            f'        python_callable={func_name},\n'
            f'        execution_timeout=timedelta(seconds={execution_timeout}),\n'
            f'        retries={retries},\n'
            f'        retry_delay=timedelta(minutes={retry_delay}),\n'
            f'    )'
        )

    task_chain = " >> ".join(var_names)
    functions_block = "\n\n\n".join(step_functions)
    tasks_block = "\n".join(task_lines)

    # Schedule: None = manual trigger, otherwise use the provided schedule
    if not schedule_interval or schedule_interval == "manual":
        schedule_str = "None"
    else:
        schedule_str = repr(schedule_interval)

    # On-failure callback
    failure_callback_code = ""
    failure_callback_ref = "None"
    if on_failure_callback and on_failure_callback.strip():
        failure_callback_code = (
            f"\n\n\ndef _on_failure(context):\n"
            f'    print(f"Pipeline {safe_name} gagal: {{context[\'exception\']}}")\n'
        )
        failure_callback_ref = "_on_failure"

    dag_content = (
        f"# Generated by EduPulse Pipeline Builder\n"
        f"# Pipeline: {pipeline_name} (ID: {dag_id})\n"
        f"from datetime import datetime, timedelta\n"
        f"from airflow import DAG\n"
        f"from airflow.operators.python import PythonOperator\n"
        f'from airflow.providers.postgres.hooks.postgres import PostgresHook\n'
        f"import pandas as pd\n\n\n"
        f"{functions_block}\n"
        f"{failure_callback_code}\n\n\n"
        f"default_args = {{\n"
        f'    "owner": "edupulse",\n'
        f"    \"depends_on_past\": False,\n"
        f"    \"retries\": 1,\n"
        f"    \"retry_delay\": timedelta(minutes=5),\n"
        f'    "on_failure_callback": {failure_callback_ref},\n'
        f"}}\n\n\n"
        f'with DAG(\n'
        f'    dag_id="{dag_id}",\n'
        f"    default_args=default_args,\n"
        f'    description="Auto-generated: {safe_name}",\n'
        f"    schedule={schedule_str},\n"
        f"    start_date=datetime(2026, 1, 1),\n"
        f"    catchup=False,\n"
        f"    max_active_runs={max_active_runs},\n"
        f'    tags=["edupulse", "auto-generated"],\n'
        f") as dag:\n"
        f"{tasks_block}\n\n"
        f"    {task_chain}\n"
    )

    return dag_content


def save_dag_file(dag_id: str, content: str) -> Path:
    """Write DAG content to file. Returns the file path."""
    dags_dir = _get_dags_dir()
    dag_file = dags_dir / f"{dag_id}.py"
    dag_file.write_text(content, encoding="utf-8")
    return dag_file


def delete_dag_file(dag_id: str) -> bool:
    """Delete a DAG file by dag_id."""
    dag_file = _get_dags_dir() / f"{dag_id}.py"
    if dag_file.exists():
        dag_file.unlink()
        return True
    return False
