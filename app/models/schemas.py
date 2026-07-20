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


class AnalyzeResponse(BaseModel):
    dashboard_type: str
    summary: str
    key_findings: list[str]
    trend: str
    business_recommendation: str
    potential_issue: str | None
    confidence: float


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
