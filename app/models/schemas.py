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
    domain_id: int | None = None


class DashboardResponse(BaseModel):
    id: int
    title: str
    description: str | None
    superset_uuid: str | None
    status: str
    is_default: bool
    domain_id: int | None = None
    dependency_count: int = 0
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
    execution_timeout: int = 300
    retries: int = 1
    retry_delay: int = 5


class PipelineStepResponse(BaseModel):
    id: int
    pipeline_id: int
    step_order: int
    name: str
    source_table: str | None
    query: str
    query_type: str
    dest_table: str | None
    execution_timeout: int | None = 300
    retries: int | None = 1
    retry_delay: int | None = 5


class PipelineCreate(BaseModel):
    name: str
    description: str = ""
    max_active_runs: int = 1
    on_failure_callback: str = ""
    schedule_interval: str = ""
    domain_id: int | None = None
    steps: list[PipelineStepCreate] = []


class PipelineResponse(BaseModel):
    id: int
    dag_id: str = ""
    name: str
    description: str | None
    status: str
    last_run_at: str | None
    max_active_runs: int | None = 1
    on_failure_callback: str | None = ""
    schedule_interval: str | None = ""
    schedule_description: str = ""
    is_active: bool = True
    domain_id: int | None = None
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
    schedule_interval: str | None = ""
    schedule_description: str = ""
    is_active: bool = True
    domain_id: int | None = None
    created_at: str


class TableInfo(BaseModel):
    schema_name: str
    table_name: str
    full_name: str
    row_count: int = 0
    column_count: int = 0
    quality_status: str = "unknown"
    quality_score: float | None = None
    freshness_status: str = "unknown"
    last_loaded_at: str | None = None
    last_built_at: str | None = None


class ColumnInfo(BaseModel):
    column_name: str
    data_type: str


# Phase 1: Dashboard Dependencies
class DashboardDependencyCreate(BaseModel):
    mart_table_name: str


class DashboardDependencyResponse(BaseModel):
    id: int
    dashboard_id: int
    mart_table_name: str


# Phase 1: Mart Table Metadata
class MartTableMetadataResponse(BaseModel):
    id: int
    table_name: str
    schema_name: str
    producing_pipeline_id: int | None = None
    producing_step_id: int | None = None
    last_built_at: str | None = None
    row_count: int = 0
    column_info: list[dict] = []
    freshness: str = "never_built"


# Phase 3: Domains
class DomainCreate(BaseModel):
    name: str
    description: str = ""
    icon: str | None = None
    color: str = "pulse"


class DomainResponse(BaseModel):
    id: int
    name: str
    description: str = ""
    icon: str | None = None
    color: str = "pulse"
    pipeline_count: int = 0
    dashboard_count: int = 0
