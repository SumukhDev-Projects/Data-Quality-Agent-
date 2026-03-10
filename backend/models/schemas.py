"""
models/schemas.py
-----------------
Pydantic models for API request/response validation.
These define the JSON shape that crosses the API boundary.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class ConnectionCreate(BaseModel):
    name: str
    description: Optional[str] = None
    conn_string: str = Field(..., description="PostgreSQL connection string: postgresql://user:pass@host:port/db")


class ConnectionResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    db_type: str
    created_at: datetime
    last_run_at: Optional[datetime]

    class Config:
        from_attributes = True


class RunRequest(BaseModel):
    connection_id: str
    tables: Optional[list[str]] = Field(
        default=None,
        description="Specific tables to analyze. Leave empty to analyze all tables."
    )


class AnomalyFindingResponse(BaseModel):
    id: str
    table_name: Optional[str]
    column_name: Optional[str]
    severity: str
    issue_type: str
    title: str
    description: Optional[str]
    suggestion: Optional[str]
    metric_value: Optional[float]
    threshold: Optional[float]
    created_at: datetime

    class Config:
        from_attributes = True


class AgentRunResponse(BaseModel):
    id: str
    connection_id: Optional[str]
    status: str
    tables_analyzed: int
    columns_profiled: int
    issues_found: int
    ai_summary: Optional[str]
    error_message: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    findings: list[AnomalyFindingResponse] = []

    class Config:
        from_attributes = True


class ColumnProfileResponse(BaseModel):
    id: str
    table_name: str
    column_name: str
    data_type: Optional[str]
    row_count: Optional[int]
    null_count: Optional[int]
    null_rate: Optional[float]
    unique_count: Optional[int]
    unique_rate: Optional[float]
    min_value: Optional[float]
    max_value: Optional[float]
    mean_value: Optional[float]
    std_value: Optional[float]
    p25: Optional[float]
    p50: Optional[float]
    p75: Optional[float]
    p99: Optional[float]
    top_values: Optional[dict]
    value_counts: Optional[dict]

    class Config:
        from_attributes = True


class DbtExportRequest(BaseModel):
    run_id: str
    model_prefix: str = ""


class DashboardStats(BaseModel):
    total_connections: int
    total_runs: int
    total_findings: int
    critical_findings: int
    last_run_at: Optional[datetime]
