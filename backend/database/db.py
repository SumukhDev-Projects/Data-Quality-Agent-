"""
database/db.py
--------------
WHY THIS FILE EXISTS:
  The agent needs its own metadata store — separate from the databases it analyzes.
  This Postgres DB (dqa_metadata) stores:
    - Target DB connections (name + encrypted connection string)
    - Run history (every time you ran the agent against a DB)
    - Profile snapshots (the statistical profile of every column, per run)
    - Anomaly reports (what the AI found)

  The DATABASE IT ANALYZES is different — you connect to it dynamically per run.
  This separation is critical: we never modify the target DB, only read it.

TABLES:
  connections       → saved DB connection strings
  agent_runs        → one row per analysis run (with status + summary)
  column_profiles   → statistical snapshot of every column in the analyzed DB
  anomaly_findings  → AI-detected issues per run
"""

from sqlalchemy import (
    create_engine, Column, String, Integer, Float,
    DateTime, Text, JSON, ForeignKey, Boolean, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dqa:dqa_secret@db:5432/dqa_metadata")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Connection(Base):
    """Saved database connections. Connection strings stored as-is (add encryption in prod)."""
    __tablename__ = "connections"

    id           = Column(String, primary_key=True)
    name         = Column(String, nullable=False)          # friendly name e.g. "Production DB"
    description  = Column(Text)
    conn_string  = Column(Text, nullable=False)            # postgresql://user:pass@host:port/db
    db_type      = Column(String, default="postgresql")    # for future MySQL/BigQuery support
    created_at   = Column(DateTime, default=datetime.utcnow)
    last_run_at  = Column(DateTime)

    runs = relationship("AgentRun", back_populates="connection", cascade="all, delete")


class AgentRun(Base):
    """
    One row per analysis run.
    A run = connect to target DB → profile all tables → AI analysis → store findings.
    """
    __tablename__ = "agent_runs"

    id              = Column(String, primary_key=True)
    connection_id   = Column(String, ForeignKey("connections.id"))
    status          = Column(String, default="pending")    # pending | running | done | error
    tables_analyzed = Column(Integer, default=0)
    columns_profiled = Column(Integer, default=0)
    issues_found    = Column(Integer, default=0)
    ai_summary      = Column(Text)                         # Claude's plain-English summary
    error_message   = Column(Text)
    started_at      = Column(DateTime, default=datetime.utcnow)
    completed_at    = Column(DateTime)
    duration_seconds = Column(Float)

    connection = relationship("Connection", back_populates="runs")
    profiles   = relationship("ColumnProfile",  back_populates="run", cascade="all, delete")
    findings   = relationship("AnomalyFinding", back_populates="run", cascade="all, delete")


class ColumnProfile(Base):
    """
    Statistical snapshot of one column in the target DB.
    Captured once per run — lets you compare profiles across runs to detect drift.

    This is what the AI reads to form its opinion about data quality.
    """
    __tablename__ = "column_profiles"

    id           = Column(String, primary_key=True)
    run_id       = Column(String, ForeignKey("agent_runs.id"))
    table_name   = Column(String, nullable=False)
    column_name  = Column(String, nullable=False)
    data_type    = Column(String)                           # postgres type: text, int4, etc.

    # Volume
    row_count    = Column(Integer)
    null_count   = Column(Integer)
    null_rate    = Column(Float)                            # null_count / row_count
    unique_count = Column(Integer)
    unique_rate  = Column(Float)                            # unique_count / row_count

    # Numeric stats (only populated for numeric columns)
    min_value    = Column(Float)
    max_value    = Column(Float)
    mean_value   = Column(Float)
    std_value    = Column(Float)
    p25          = Column(Float)                            # 25th percentile
    p50          = Column(Float)                            # median
    p75          = Column(Float)                            # 75th percentile
    p99          = Column(Float)                            # 99th percentile (outlier detection)

    # Categorical stats (only for low-cardinality columns)
    top_values   = Column(JSON)                             # {"value": count} dict, top 10
    value_counts = Column(JSON)                             # full distribution if cardinality < 50

    created_at   = Column(DateTime, default=datetime.utcnow)

    run = relationship("AgentRun", back_populates="profiles")


class AnomalyFinding(Base):
    """
    One AI-detected data quality issue.
    Every finding is tied to a run, table, and optionally a column.
    """
    __tablename__ = "anomaly_findings"

    id           = Column(String, primary_key=True)
    run_id       = Column(String, ForeignKey("agent_runs.id"))
    table_name   = Column(String)
    column_name  = Column(String)                          # null if table-level finding

    # Severity: critical > warning > info
    severity     = Column(String, default="warning")       # critical | warning | info
    issue_type   = Column(String)                          # null_rate | outlier | inconsistent_values | negative_values | etc.
    title        = Column(String)                          # short 1-line description
    description  = Column(Text)                            # Claude's plain-English explanation
    metric_value = Column(Float)                           # the actual number (e.g. null_rate=0.08)
    threshold    = Column(Float)                           # what it should be (e.g. 0.05)
    suggestion   = Column(Text)                            # Claude's recommended fix

    created_at   = Column(DateTime, default=datetime.utcnow)

    run = relationship("AgentRun", back_populates="findings")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    Base.metadata.create_all(bind=engine)
