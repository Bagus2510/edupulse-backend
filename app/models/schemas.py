from pydantic import BaseModel


# Superset
class GuestTokenRequest(BaseModel):
    dashboard_uuid: str


class GuestTokenResponse(BaseModel):
    guest_token: str
    dashboard_uuid: str


# Airflow
class DAGTriggerResponse(BaseModel):
    dag_id: str
    run_id: str
    state: str


class DAGStatusResponse(BaseModel):
    dag_id: str
    run_id: str | None = None
    state: str | None = None
    execution_date: str | None = None


# AI Analytics
class AnalyzeRequest(BaseModel):
    dashboard_title: str
    dashboard_description: str
    dashboard_uuid: str = ""


class AnalyzeResponse(BaseModel):
    dashboard_type: str
    summary: str
    key_findings: list[str]
    trend: str
    business_recommendation: str
    potential_issue: str | None
    confidence: float


# AI Chat
class ChatRequest(BaseModel):
    message: str
    dashboard_uuid: str
    session_id: str
    dashboard_title: str = ""


class ChatResponse(BaseModel):
    response: str
    session_id: str


class ChatClearRequest(BaseModel):
    session_id: str


# Dashboards
class DashboardCreate(BaseModel):
    title: str
    description: str = ""
    superset_uuid: str = ""
    status: str = "pending"


class DashboardResponse(BaseModel):
    id: int
    title: str
    description: str | None
    superset_uuid: str | None
    status: str
    is_default: bool
    created_at: str
    updated_at: str


# Activity
class ActivityResponse(BaseModel):
    id: int
    action: str
    details: str | None
    status: str
    created_at: str


class ActivityCreate(BaseModel):
    action: str
    details: str = ""
    status: str = "success"


# Pipeline Runs
class PipelineRunResponse(BaseModel):
    id: int
    dag_id: str
    run_id: str | None
    status: str
    duration: str | None
    tasks_total: int | None
    tasks_completed: int | None
    created_at: str


class PipelineTriggerRequest(BaseModel):
    dag_id: str


# Pipeline Builder
class PipelineStepCreate(BaseModel):
    name: str
    source_table: str = ""
    query: str
    query_type: str = "sql"
    dest_table: str = ""


class PipelineStepResponse(BaseModel):
    id: int
    pipeline_id: int
    step_order: int
    name: str
    source_table: str | None
    query: str
    query_type: str
    dest_table: str | None


class PipelineCreate(BaseModel):
    name: str
    description: str = ""
    steps: list[PipelineStepCreate] = []


class PipelineResponse(BaseModel):
    id: int
    name: str
    description: str | None
    status: str
    last_run_at: str | None
    steps: list[PipelineStepResponse] = []
    created_at: str
    updated_at: str


class PipelineListResponse(BaseModel):
    id: int
    name: str
    description: str | None
    status: str
    last_run_at: str | None
    step_count: int
    created_at: str


class TableInfo(BaseModel):
    schema_name: str
    table_name: str
    full_name: str
    row_count: int = 0
    column_count: int = 0


class ColumnInfo(BaseModel):
    column_name: str
    data_type: str
