"""
services/profiler.py
---------------------
WHY THIS FILE EXISTS:
  Before the AI can detect anomalies, we need raw statistics about every column.
  This service connects to the TARGET database (the one being analyzed),
  runs SQL queries against it, and returns a structured profile.

  This is the DATA ENGINEERING layer — pure SQL + pandas, no AI.
  Keeping it separate from the AI agent means:
    1. We can test profiling independently
    2. We can profile any Postgres DB (not just the sample one)
    3. If we add BigQuery support later, only this file changes

WHAT IT PROFILES PER COLUMN:
  - Row count, null count, null rate
  - Unique count, unique rate (detects primary key violations if unique_rate < 1.0 on PK cols)
  - For numerics: min, max, mean, std, p25, p50, p75, p99 (percentiles catch outliers)
  - For categoricals: top value distribution (catches inconsistent casing like COMPLETED/completed)

KEY TECHNIQUE — Why percentiles over just min/max?
  min/max are skewed by extreme outliers.
  p99 tells you "what's the 99th percentile value?" — if p99 is $800 but max is $999,999,
  that $999,999 is clearly an outlier. The AI uses this to flag it intelligently.
"""

from sqlalchemy import create_engine, text, inspect
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Columns with fewer unique values than this are treated as categorical
CATEGORICAL_THRESHOLD = 50
# Max rows to sample for expensive stats on large tables
SAMPLE_THRESHOLD = 100_000


@dataclass
class ColumnProfile:
    table_name: str
    column_name: str
    data_type: str
    row_count: int
    null_count: int
    null_rate: float
    unique_count: int
    unique_rate: float
    # Numeric stats
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None
    p25: Optional[float] = None
    p50: Optional[float] = None
    p75: Optional[float] = None
    p99: Optional[float] = None
    # Categorical stats
    top_values: Optional[dict] = None
    value_counts: Optional[dict] = None


@dataclass
class TableProfile:
    table_name: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile] = field(default_factory=list)


@dataclass
class DatabaseProfile:
    tables: list[TableProfile] = field(default_factory=list)

    @property
    def total_tables(self) -> int:
        return len(self.tables)

    @property
    def total_columns(self) -> int:
        return sum(t.column_count for t in self.tables)

    def to_dict(self) -> dict:
        """Serializes to a dict Claude can read."""
        return {
            "tables": [
                {
                    "table": t.table_name,
                    "row_count": t.row_count,
                    "columns": [
                        {k: v for k, v in vars(c).items() if v is not None}
                        for c in t.columns
                    ]
                }
                for t in self.tables
            ]
        }


# Numeric postgres types
NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric", "decimal",
    "real", "double precision", "float", "int4", "int8", "int2",
    "float4", "float8", "money"
}


class DatabaseProfiler:
    """
    Connects to any PostgreSQL database and profiles all tables.

    Usage:
        profiler = DatabaseProfiler("postgresql://user:pass@host:5432/dbname")
        profile = profiler.profile()
    """

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string, pool_pre_ping=True)

    def profile(self, tables: list[str] = None) -> DatabaseProfile:
        """
        Profiles all tables (or specific ones if provided).
        Returns a DatabaseProfile with stats for every column.
        """
        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names(schema="public")

        # Filter to requested tables if provided
        target_tables = [t for t in all_tables if tables is None or t in tables]

        db_profile = DatabaseProfile()

        for table in target_tables:
            try:
                table_profile = self._profile_table(table, inspector)
                db_profile.tables.append(table_profile)
                logger.info(f"Profiled {table}: {table_profile.row_count} rows, {table_profile.column_count} cols")
            except Exception as e:
                logger.warning(f"Failed to profile table {table}: {e}")
                continue

        return db_profile

    def _profile_table(self, table_name: str, inspector) -> TableProfile:
        """Profiles one table — row count + all column stats."""
        # Get row count
        with self.engine.connect() as conn:
            row_count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{table_name}"')
            ).scalar() or 0

        # Get column definitions
        columns = inspector.get_columns(table_name, schema="public")

        table_profile = TableProfile(
            table_name=table_name,
            row_count=row_count,
            column_count=len(columns)
        )

        for col in columns:
            col_name = col["name"]
            col_type = str(col["type"]).lower().split("(")[0].strip()  # normalize e.g. VARCHAR(100) → varchar

            try:
                col_profile = self._profile_column(table_name, col_name, col_type, row_count)
                table_profile.columns.append(col_profile)
            except Exception as e:
                logger.warning(f"Failed to profile {table_name}.{col_name}: {e}")

        return table_profile

    def _profile_column(self, table: str, column: str, dtype: str, row_count: int) -> ColumnProfile:
        """
        Profiles one column. Runs different SQL depending on data type.

        For NUMERIC columns: runs percentile + stats queries
        For TEXT/CATEGORICAL: runs value distribution queries
        """
        with self.engine.connect() as conn:
            # Basic stats — null count + unique count (all types)
            basic = conn.execute(text(f"""
                SELECT
                    COUNT(*) FILTER (WHERE "{column}" IS NULL) AS null_count,
                    COUNT(DISTINCT "{column}") AS unique_count
                FROM "{table}"
            """)).fetchone()

            null_count = basic[0] or 0
            unique_count = basic[1] or 0
            null_rate = null_count / row_count if row_count > 0 else 0.0
            unique_rate = unique_count / row_count if row_count > 0 else 0.0

            profile = ColumnProfile(
                table_name=table,
                column_name=column,
                data_type=dtype,
                row_count=row_count,
                null_count=null_count,
                null_rate=round(null_rate, 4),
                unique_count=unique_count,
                unique_rate=round(unique_rate, 4)
            )

            # Numeric stats
            is_numeric = any(t in dtype for t in NUMERIC_TYPES)
            if is_numeric:
                try:
                    stats = conn.execute(text(f"""
                        SELECT
                            MIN("{column}"::numeric),
                            MAX("{column}"::numeric),
                            AVG("{column}"::numeric),
                            STDDEV("{column}"::numeric),
                            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column}"::numeric),
                            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "{column}"::numeric),
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column}"::numeric),
                            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "{column}"::numeric)
                        FROM "{table}"
                        WHERE "{column}" IS NOT NULL
                    """)).fetchone()

                    if stats and stats[0] is not None:
                        profile.min_value  = float(stats[0])
                        profile.max_value  = float(stats[1])
                        profile.mean_value = round(float(stats[2]), 4)
                        profile.std_value  = round(float(stats[3]), 4) if stats[3] else None
                        profile.p25        = float(stats[4])
                        profile.p50        = float(stats[5])
                        profile.p75        = float(stats[6])
                        profile.p99        = float(stats[7])
                except Exception as e:
                    logger.debug(f"Numeric stats failed for {table}.{column}: {e}")

            # Categorical distribution — only for low-cardinality columns
            if unique_count <= CATEGORICAL_THRESHOLD and unique_count > 0:
                try:
                    rows = conn.execute(text(f"""
                        SELECT "{column}"::text AS val, COUNT(*) AS cnt
                        FROM "{table}"
                        WHERE "{column}" IS NOT NULL
                        GROUP BY "{column}"::text
                        ORDER BY cnt DESC
                        LIMIT 20
                    """)).fetchall()

                    profile.top_values = {str(r[0]): int(r[1]) for r in rows[:10]}
                    if unique_count <= CATEGORICAL_THRESHOLD:
                        profile.value_counts = {str(r[0]): int(r[1]) for r in rows}
                except Exception as e:
                    logger.debug(f"Categorical stats failed for {table}.{column}: {e}")

        return profile

    def close(self):
        self.engine.dispose()
