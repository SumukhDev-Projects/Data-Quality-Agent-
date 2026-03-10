"""
main.py
-------
FastAPI application entry point. All routes defined here.

ROUTE GROUPS:
  /connections  — manage saved DB connections
  /runs         — trigger and retrieve analysis runs
  /profiles     — browse column profiles from a run
  /export       — download dbt schema.yml
  /stats        — dashboard summary stats

EXECUTION FLOW for a new analysis run:
  1. POST /runs → creates AgentRun record, status=pending
  2. Background task starts:
     a. DatabaseProfiler connects to target DB, profiles every column
     b. AnomalyDetector runs statistical checks on profiles
     c. DataQualityAgent sends profiles+flags to Claude for enrichment
     d. Results saved to DB, status=done
  3. GET /runs/{id} → returns completed run with all findings
"""

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from sqlalchemy.orm import Session
from sqlalchemy import func
import uuid
from datetime import datetime

from database.db import (
    get_db, create_tables,
    Connection, AgentRun, ColumnProfile, AnomalyFinding
)
from models.schemas import (
    ConnectionCreate, ConnectionResponse,
    RunRequest, AgentRunResponse,
    ColumnProfileResponse, AnomalyFindingResponse,
    DbtExportRequest, DashboardStats
)
from services.profiler import DatabaseProfiler
from services.anomaly_detector import AnomalyDetector
from services.dbt_generator import DbtTestGenerator
from agents.dq_agent import DataQualityAgent

app = FastAPI(
    title="AI Data Quality Agent",
    description="Autonomous data quality analysis powered by Claude",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

anomaly_detector = AnomalyDetector()
dbt_generator = DbtTestGenerator()
dq_agent = DataQualityAgent()


@app.on_event("startup")
async def startup():
    create_tables()


@app.get("/health")
def health():
    return {"status": "ok", "service": "data-quality-agent"}


# ── Connections ───────────────────────────────────────────────────────────────

@app.post("/connections", response_model=ConnectionResponse)
def create_connection(data: ConnectionCreate, db: Session = Depends(get_db)):
    """
    Save a database connection.
    The connection string is tested before saving — if it can't connect, reject it.
    """
    # Test the connection
    try:
        profiler = DatabaseProfiler(data.conn_string)
        profiler.close()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot connect to database: {str(e)}")

    conn = Connection(
        id=str(uuid.uuid4()),
        name=data.name,
        description=data.description,
        conn_string=data.conn_string,
    )
    db.add(conn)
    db.commit()
    db.refresh(conn)
    return conn


@app.get("/connections", response_model=list[ConnectionResponse])
def list_connections(db: Session = Depends(get_db)):
    return db.query(Connection).order_by(Connection.created_at.desc()).all()


@app.delete("/connections/{conn_id}")
def delete_connection(conn_id: str, db: Session = Depends(get_db)):
    conn = db.query(Connection).filter(Connection.id == conn_id).first()
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    db.delete(conn)
    db.commit()
    return {"deleted": conn_id}


# ── Runs ──────────────────────────────────────────────────────────────────────

@app.post("/runs", response_model=AgentRunResponse)
def start_run(
    request: RunRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Triggers a new analysis run. Returns immediately with run_id.
    The actual analysis happens in a background task.
    Poll GET /runs/{id} to check status.
    """
    conn = db.query(Connection).filter(Connection.id == request.connection_id).first()
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    run = AgentRun(
        id=str(uuid.uuid4()),
        connection_id=request.connection_id,
        status="running"
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # Fire off analysis in background so the HTTP response returns immediately
    background_tasks.add_task(
        _run_analysis,
        run_id=run.id,
        conn_string=conn.conn_string,
        tables=request.tables
    )

    return run


@app.get("/runs", response_model=list[AgentRunResponse])
def list_runs(db: Session = Depends(get_db)):
    runs = db.query(AgentRun).order_by(AgentRun.started_at.desc()).limit(50).all()
    return runs


@app.get("/runs/{run_id}", response_model=AgentRunResponse)
def get_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(AgentRun).filter(AgentRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.get("/runs/{run_id}/profiles", response_model=list[ColumnProfileResponse])
def get_run_profiles(run_id: str, table: str = None, db: Session = Depends(get_db)):
    """Returns column profiles for a completed run, optionally filtered by table."""
    query = db.query(ColumnProfile).filter(ColumnProfile.run_id == run_id)
    if table:
        query = query.filter(ColumnProfile.table_name == table)
    return query.order_by(ColumnProfile.table_name, ColumnProfile.column_name).all()


@app.get("/runs/{run_id}/findings", response_model=list[AnomalyFindingResponse])
def get_run_findings(run_id: str, severity: str = None, db: Session = Depends(get_db)):
    """Returns anomaly findings for a run, optionally filtered by severity."""
    query = db.query(AnomalyFinding).filter(AnomalyFinding.run_id == run_id)
    if severity:
        query = query.filter(AnomalyFinding.severity == severity)
    return query.order_by(AnomalyFinding.severity, AnomalyFinding.table_name).all()


# ── Export ────────────────────────────────────────────────────────────────────

@app.post("/export/dbt")
def export_dbt_schema(request: DbtExportRequest, db: Session = Depends(get_db)):
    """
    Generates a dbt schema.yml from a completed run's column profiles.
    Returns YAML as a downloadable file.
    """
    run = db.query(AgentRun).filter(AgentRun.id == request.run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status != "done":
        raise HTTPException(status_code=400, detail="Run not complete yet")

    # Rebuild a profile object from DB records
    profiles = db.query(ColumnProfile).filter(ColumnProfile.run_id == request.run_id).all()

    # Group by table
    from services.profiler import DatabaseProfile, TableProfile, ColumnProfile as CP
    from dataclasses import dataclass

    table_map: dict = {}
    for p in profiles:
        if p.table_name not in table_map:
            table_map[p.table_name] = []
        # Reconstruct ColumnProfile dataclass from ORM object
        cp = CP(
            table_name=p.table_name,
            column_name=p.column_name,
            data_type=p.data_type or "",
            row_count=p.row_count or 0,
            null_count=p.null_count or 0,
            null_rate=p.null_rate or 0.0,
            unique_count=p.unique_count or 0,
            unique_rate=p.unique_rate or 0.0,
            min_value=p.min_value,
            max_value=p.max_value,
            mean_value=p.mean_value,
            std_value=p.std_value,
            p25=p.p25, p50=p.p50, p75=p.p75, p99=p.p99,
            top_values=p.top_values,
            value_counts=p.value_counts
        )
        table_map[p.table_name].append(cp)

    db_profile = DatabaseProfile()
    for tname, cols in table_map.items():
        tp = TableProfile(
            table_name=tname,
            row_count=cols[0].row_count if cols else 0,
            column_count=len(cols),
            columns=cols
        )
        db_profile.tables.append(tp)

    yaml_content = dbt_generator.generate(db_profile, model_prefix=request.model_prefix)

    return Response(
        content=yaml_content.encode("utf-8"),
        media_type="text/yaml",
        headers={"Content-Disposition": "attachment; filename=schema.yml"}
    )


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/stats", response_model=DashboardStats)
def get_stats(db: Session = Depends(get_db)):
    last_run = db.query(AgentRun).filter(
        AgentRun.status == "done"
    ).order_by(AgentRun.completed_at.desc()).first()

    return DashboardStats(
        total_connections=db.query(func.count(Connection.id)).scalar(),
        total_runs=db.query(func.count(AgentRun.id)).filter(AgentRun.status == "done").scalar(),
        total_findings=db.query(func.count(AnomalyFinding.id)).scalar(),
        critical_findings=db.query(func.count(AnomalyFinding.id)).filter(
            AnomalyFinding.severity == "critical"
        ).scalar(),
        last_run_at=last_run.completed_at if last_run else None
    )


# ── Background Analysis Task ──────────────────────────────────────────────────

def _run_analysis(run_id: str, conn_string: str, tables: list[str] = None):
    """
    The full analysis pipeline. Runs in a background thread.

    Steps:
    1. Profile the target database (SQL stats queries)
    2. Run statistical anomaly detection
    3. Call AI agent for intelligent enrichment
    4. Save all results to metadata DB
    """
    # Get a fresh DB session for this background thread
    from database.db import SessionLocal
    db = SessionLocal()
    start_time = datetime.utcnow()

    try:
        run = db.query(AgentRun).filter(AgentRun.id == run_id).first()

        # ── Step 1: Profile ────────────────────────────────────────────────
        profiler = DatabaseProfiler(conn_string)
        db_profile = profiler.profile(tables)
        profiler.close()

        # Save column profiles to DB
        for table in db_profile.tables:
            for col in table.columns:
                db_profile_record = ColumnProfile(
                    id=str(uuid.uuid4()),
                    run_id=run_id,
                    table_name=col.table_name,
                    column_name=col.column_name,
                    data_type=col.data_type,
                    row_count=col.row_count,
                    null_count=col.null_count,
                    null_rate=col.null_rate,
                    unique_count=col.unique_count,
                    unique_rate=col.unique_rate,
                    min_value=col.min_value,
                    max_value=col.max_value,
                    mean_value=col.mean_value,
                    std_value=col.std_value,
                    p25=col.p25, p50=col.p50, p75=col.p75, p99=col.p99,
                    top_values=col.top_values,
                    value_counts=col.value_counts
                )
                db.add(db_profile_record)
        db.commit()

        # ── Step 2: Statistical Detection ─────────────────────────────────
        flags = anomaly_detector.detect(db_profile)

        # ── Step 3: AI Enrichment ──────────────────────────────────────────
        ai_result = dq_agent.analyze(db_profile, flags)

        # ── Step 4: Save Findings ──────────────────────────────────────────
        for finding in ai_result["findings"]:
            db_finding = AnomalyFinding(
                id=str(uuid.uuid4()),
                run_id=run_id,
                table_name=finding.get("table_name"),
                column_name=finding.get("column_name"),
                severity=finding.get("severity", "warning"),
                issue_type=finding.get("issue_type", "unknown"),
                title=finding.get("title", ""),
                description=finding.get("description", ""),
                suggestion=finding.get("suggestion", ""),
                metric_value=finding.get("metric_value"),
                threshold=finding.get("threshold")
            )
            db.add(db_finding)

        # Update run record
        end_time = datetime.utcnow()
        run.status = "done"
        run.tables_analyzed = db_profile.total_tables
        run.columns_profiled = db_profile.total_columns
        run.issues_found = len(ai_result["findings"])
        run.ai_summary = ai_result["summary"]
        run.completed_at = end_time
        run.duration_seconds = (end_time - start_time).total_seconds()

        # Update connection's last_run_at
        conn = db.query(Connection).filter(Connection.id == run.connection_id).first()
        if conn:
            conn.last_run_at = end_time

        db.commit()

    except Exception as e:
        run = db.query(AgentRun).filter(AgentRun.id == run_id).first()
        if run:
            run.status = "error"
            run.error_message = str(e)
            run.completed_at = datetime.utcnow()
            db.commit()
        raise
    finally:
        db.close()
