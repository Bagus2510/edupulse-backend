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


def _build_step_function(
    func_name: str,
    query_type: str,
    query: str,
    source_table: str,
    dest_table: str,
) -> str:
    """Build a single step function with instrumentation (timing + rowcount + XCom push)."""
    source_comment = f"    # Source: {source_table}\n" if source_table else ""
    dest_comment = f"    # Dest: {dest_table}\n" if dest_table else ""

    if query_type == "sql":
        safe_query = json.dumps(query)
        is_select = query.strip().upper().startswith("SELECT")
        if is_select:
            body = (
                f"def {func_name}(ti):\n"
                f"    import time\n"
                f"{source_comment}{dest_comment}"
                f"    _start = time.time()\n"
                f'    hook = PostgresHook(postgres_conn_id="edupulse")\n'
                f"    conn = hook.get_conn()\n"
                f"    cursor = conn.cursor()\n"
                f"    cursor.execute({safe_query})\n"
                f"    rows = cursor.fetchall()\n"
                f"    rowcount = len(rows)\n"
                f'    print(f"Fetched {{rowcount}} rows")\n'
                f"    for row in rows[:10]:\n"
                f"        print(row)\n"
                f"    cursor.close()\n"
                f"    conn.close()\n"
                f"    _elapsed = round((time.time() - _start) * 1000)\n"
                f'    ti.xcom_push(key="rows_read", value=rowcount)\n'
                f'    ti.xcom_push(key="rows_written", value=0)\n'
                f'    ti.xcom_push(key="duration_ms", value=_elapsed)\n'
            )
        else:
            replace_dest = ""
            if dest_table and query.strip().upper().startswith("CREATE TABLE"):
                replace_dest = f'    cursor.execute("DROP TABLE IF EXISTS {dest_table}")\n'
            body = (
                f"def {func_name}(ti):\n"
                f"    import time\n"
                f"{source_comment}{dest_comment}"
                f"    _start = time.time()\n"
                f'    hook = PostgresHook(postgres_conn_id="edupulse")\n'
                f"    conn = hook.get_conn()\n"
                f"    cursor = conn.cursor()\n"
                f"{replace_dest}"
                f"    cursor.execute({safe_query})\n"
                f"    rowcount = cursor.rowcount\n"
                f"    conn.commit()\n"
                f"    cursor.close()\n"
                f"    conn.close()\n"
                f"    _elapsed = round((time.time() - _start) * 1000)\n"
                f'    ti.xcom_push(key="rows_read", value=0)\n'
                f'    ti.xcom_push(key="rows_written", value=rowcount if rowcount >= 0 else 0)\n'
                f'    ti.xcom_push(key="duration_ms", value=_elapsed)\n'
            )
    else:
        user_code = _indent_user_code(query)
        if source_table or dest_table:
            body = (
                f"def {func_name}(ti):\n"
                f"    import time\n"
                f'    hook = PostgresHook(postgres_conn_id="edupulse")\n'
                f"    engine = hook.get_sqlalchemy_engine()\n"
                f"    _start = time.time()\n"
            )
            if source_table:
                body += f'    df = pd.read_sql("SELECT * FROM {source_table}", engine)\n'
            body += (
                f"    _rows_in = len(df) if 'df' in dir() else 0\n"
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
            body += (
                f"    _elapsed = round((time.time() - _start) * 1000)\n"
                f'    ti.xcom_push(key="rows_read", value=_rows_in)\n'
                f'    ti.xcom_push(key="rows_written", value=len(df) if "df" in dir() else 0)\n'
                f'    ti.xcom_push(key="duration_ms", value=_elapsed)\n'
            )
        else:
            body = (
                f"def {func_name}(ti):\n"
                f"    import time\n"
                f"    _start = time.time()\n"
                f"    # --- user transformation ---\n"
                f"{user_code}\n"
                f"    # --- end transformation ---\n"
                f"    _elapsed = round((time.time() - _start) * 1000)\n"
                f'    ti.xcom_push(key="rows_read", value=0)\n'
                f'    ti.xcom_push(key="rows_written", value=0)\n'
                f'    ti.xcom_push(key="duration_ms", value=_elapsed)\n'
            )
    return body


def generate_dag_content(dag_id: str, pipeline_name: str, steps: list[dict], schedule_interval: str = "", max_active_runs: int = 1, on_failure_callback: str = "") -> str:
    """Generate DAG file content with full instrumentation. Returns the Python source code."""
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

        body = _build_step_function(func_name, query_type, query, source_table, dest_table)
        step_functions.append(body)
        task_lines.append(
            f'    {func_name} = PythonOperator(\n'
            f'        task_id="{task_id}",\n'
            f'        python_callable={func_name},\n'
            f'        execution_timeout=timedelta(seconds={execution_timeout}),\n'
            f'        retries={retries},\n'
            f'        retry_delay=timedelta(minutes={retry_delay}),\n'
            f'        provide_context=True,\n'
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

    # Post-pipeline callback: write instrumentation to DB
    post_callback_code = (
        f"\n\n\ndef _post_pipeline(context):\n"
        f'    \"\"\"Write instrumentation data to app.pipeline_runs after all tasks complete.\"\"\"\n'
        f"    import json\n"
        f"    try:\n"
        f'        from airflow.providers.postgres.hooks.postgres import PostgresHook\n'
        f"        hook = PostgresHook(postgres_conn_id='edupulse')\n"
        f"        conn = hook.get_conn()\n"
        f"        cursor = conn.cursor()\n"
        f"        dag_id = context['dag'].dag_id\n"
        f"        run_id = context['run'].run_id\n"
        f"        total_rows_read = 0\n"
        f"        total_rows_written = 0\n"
        f"        total_duration_ms = 0\n"
        f"        task_states = context['dag_run'].get_task_instance_states() if hasattr(context['dag_run'], 'get_task_instance_states') else {{}}\n"
        f"        # Pull XCom from each task\n"
        f"        for task in context['dag'].tasks:\n"
        f"            try:\n"
        f"                ti = context['ti'].dag_run.get_task_instance(task.task_id, session=None)\n"
        f"                if ti:\n"
        f"                    rows_read = ti.xcom_pull(key='rows_read', task_ids=task.task_id)\n"
        f"                    rows_written = ti.xcom_pull(key='rows_written', task_ids=task.task_id)\n"
        f"                    dur = ti.xcom_pull(key='duration_ms', task_ids=task.task_id)\n"
        f"                    total_rows_read += rows_read or 0\n"
        f"                    total_rows_written += rows_written or 0\n"
        f"                    total_duration_ms += dur or 0\n"
        f"            except Exception:\n"
        f"                pass\n"
        f"        cursor.execute(\n"
        f'            "UPDATE app.pipeline_runs SET rows_read = %s, rows_written = %s, duration_ms = %s, quality_status = \'passed\' WHERE id = (SELECT id FROM app.pipeline_runs WHERE dag_id = %s AND status = \'running\' ORDER BY id DESC LIMIT 1)",\n'
        f"            (total_rows_read, total_rows_written, total_duration_ms, dag_id)\n"
        f"        )\n"
        f"        conn.commit()\n"
        f"        cursor.close()\n"
        f"        conn.close()\n"
        f"    except Exception as e:\n"
        f'        print(f"Post-pipeline callback error: {{e}}")\n'
    )

    # On-failure callback
    failure_callback_code = (
        f"\n\n\ndef _on_failure(context):\n"
        f'    \"\"\"Write error message to app.pipeline_runs on failure.\"\"\"\n'
        f"    import traceback\n"
        f"    try:\n"
        f'        from airflow.providers.postgres.hooks.postgres import PostgresHook\n'
        f"        hook = PostgresHook(postgres_conn_id='edupulse')\n"
        f"        conn = hook.get_conn()\n"
        f"        cursor = conn.cursor()\n"
        f"        dag_id = context['dag'].dag_id\n"
        f"        error_msg = str(context.get('exception', ''))\n"
        f"        tb = traceback.format_exception(type(context.get('exception', Exception())), context.get('exception', Exception()), context.get('exception', None).__traceback__ if hasattr(context.get('exception', Exception()), '__traceback__') else None)\n"
        f"        full_error = error_msg + '\\n' + ''.join(tb) if tb else error_msg\n"
        f"        cursor.execute(\n"
        f'            "UPDATE app.pipeline_runs SET status = \'failed\', error_message = %s WHERE id = (SELECT id FROM app.pipeline_runs WHERE dag_id = %s AND status = \'running\' ORDER BY id DESC LIMIT 1)",\n'
        f"            (full_error[:2000], dag_id)\n"
        f"        )\n"
        f"        conn.commit()\n"
        f"        cursor.close()\n"
        f"        conn.close()\n"
        f"    except Exception as e:\n"
        f'        print(f"Failure callback error: {{e}}")\n'
    )

    # Wrap tasks with post-pipeline callback using on_success_callback on last task
    post_callback_ref = "_post_pipeline"

    dag_content = (
        f"# Generated by Datapulse Pipeline Builder\n"
        f"# Pipeline: {pipeline_name} (ID: {dag_id})\n"
        f"from datetime import datetime, timedelta\n"
        f"from airflow import DAG\n"
        f"from airflow.operators.python import PythonOperator\n"
        f'from airflow.providers.postgres.hooks.postgres import PostgresHook\n'
        f"import pandas as pd\n\n\n"
        f"{functions_block}\n"
        f"{post_callback_code}\n"
        f"{failure_callback_code}\n\n\n"
        f"default_args = {{\n"
        f'    "owner": "edupulse",\n'
        f"    \"depends_on_past\": False,\n"
        f"    \"retries\": 1,\n"
        f"    \"retry_delay\": timedelta(minutes=5),\n"
        f'    "on_failure_callback": _on_failure,\n'
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
        f"    {task_chain}\n\n"
        f"    # Attach post-pipeline callback to last task\n"
        f"    {var_names[-1]}.on_success_callback = {post_callback_ref}\n"
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
