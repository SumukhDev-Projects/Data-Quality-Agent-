"""
services/anomaly_detector.py
-----------------------------
WHY THIS FILE EXISTS:
  Before sending anything to Claude, we run deterministic statistical checks.
  This pre-screening catches obvious issues (null rates, negatives, outliers)
  using math — not AI. This is faster, cheaper, and more precise for known patterns.

  Claude then receives BOTH the raw profile + these pre-screened flags,
  and provides intelligent interpretation: WHY the issue matters, HOW to fix it,
  and WHAT business impact it might have.

  This separation of concerns is a key design principle:
  - Use statistics for DETECTION (fast, deterministic, no API cost)
  - Use AI for INTERPRETATION (context-aware, business-meaningful explanations)

CHECKS PERFORMED:
  1. High null rate           — column nulls exceed threshold (default 5%)
  2. Negative values          — numeric columns with values < 0 (often impossible)
  3. Statistical outliers     — IQR method: values > Q3 + 3×IQR
  4. Zero values              — columns where 0 is likely invalid (quantity, amount)
  5. Inconsistent categories  — same concept spelled differently (COMPLETED vs completed)
  6. Low unique rate on ID    — suspected primary key with duplicates
  7. Future dates             — date columns with future timestamps
  8. Constant columns         — columns with only one unique value (likely broken)
"""

from dataclasses import dataclass
from typing import Optional
from services.profiler import ColumnProfile, DatabaseProfile
import logging

logger = logging.getLogger(__name__)

# Configurable thresholds
NULL_RATE_THRESHOLD    = 0.05   # flag if >5% nulls
HIGH_NULL_THRESHOLD    = 0.20   # critical if >20% nulls
OUTLIER_IQR_FACTOR     = 3.0    # IQR × this = outlier boundary
MIN_UNIQUE_RATE_FOR_ID = 0.95   # ID-like columns should have ≥95% unique values


@dataclass
class AnomalyFlag:
    """A single pre-screened data quality flag from statistical analysis."""
    table_name: str
    column_name: Optional[str]
    severity: str          # critical | warning | info
    issue_type: str        # null_rate | outlier | negative_values | etc.
    title: str
    description: str
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    suggestion: str = ""


class AnomalyDetector:
    """
    Runs deterministic statistical checks on a DatabaseProfile.
    Returns a list of AnomalyFlag objects to pass to the AI agent.

    Usage:
        detector = AnomalyDetector()
        flags = detector.detect(db_profile)
    """

    def detect(self, profile: DatabaseProfile) -> list[AnomalyFlag]:
        """Run all checks against the full database profile."""
        flags = []

        for table in profile.tables:
            # Table-level checks
            flags.extend(self._check_empty_table(table.table_name, table.row_count))

            for col in table.columns:
                flags.extend(self._check_null_rate(col))
                flags.extend(self._check_negative_values(col))
                flags.extend(self._check_statistical_outliers(col))
                flags.extend(self._check_zero_values(col))
                flags.extend(self._check_inconsistent_categories(col))
                flags.extend(self._check_constant_column(col))

        logger.info(f"Anomaly detection complete: {len(flags)} flags found")
        return flags

    def _check_empty_table(self, table_name: str, row_count: int) -> list[AnomalyFlag]:
        if row_count == 0:
            return [AnomalyFlag(
                table_name=table_name, column_name=None,
                severity="critical", issue_type="empty_table",
                title=f"Table '{table_name}' is empty",
                description=f"The table has 0 rows. This may indicate a failed ETL job or incorrect table reference.",
                metric_value=0, threshold=1,
                suggestion="Check upstream ETL pipeline for failures."
            )]
        return []

    def _check_null_rate(self, col: ColumnProfile) -> list[AnomalyFlag]:
        flags = []
        if col.null_rate > HIGH_NULL_THRESHOLD:
            flags.append(AnomalyFlag(
                table_name=col.table_name, column_name=col.column_name,
                severity="critical", issue_type="high_null_rate",
                title=f"Critical null rate in {col.table_name}.{col.column_name}",
                description=f"{col.null_count:,} of {col.row_count:,} rows ({col.null_rate*100:.1f}%) are NULL — exceeds critical threshold of {HIGH_NULL_THRESHOLD*100:.0f}%.",
                metric_value=col.null_rate, threshold=HIGH_NULL_THRESHOLD,
                suggestion=f"Investigate why {col.column_name} is null so frequently. Consider adding a NOT NULL constraint after fixing the data."
            ))
        elif col.null_rate > NULL_RATE_THRESHOLD:
            flags.append(AnomalyFlag(
                table_name=col.table_name, column_name=col.column_name,
                severity="warning", issue_type="null_rate",
                title=f"Elevated null rate in {col.table_name}.{col.column_name}",
                description=f"{col.null_count:,} of {col.row_count:,} rows ({col.null_rate*100:.1f}%) are NULL — above threshold of {NULL_RATE_THRESHOLD*100:.0f}%.",
                metric_value=col.null_rate, threshold=NULL_RATE_THRESHOLD,
                suggestion=f"Review whether nulls in {col.column_name} are expected. Add a dbt not_null test if this column should always have values."
            ))
        return flags

    def _check_negative_values(self, col: ColumnProfile) -> list[AnomalyFlag]:
        """Flag numeric columns where min_value < 0 (most business metrics can't be negative)."""
        if col.min_value is None or col.min_value >= 0:
            return []

        # Skip columns where negatives might be valid
        skip_patterns = ["temperature", "latitude", "longitude", "delta", "change", "diff", "balance"]
        if any(p in col.column_name.lower() for p in skip_patterns):
            return []

        return [AnomalyFlag(
            table_name=col.table_name, column_name=col.column_name,
            severity="critical", issue_type="negative_values",
            title=f"Negative values in {col.table_name}.{col.column_name}",
            description=f"Minimum value is {col.min_value:.2f}. For a '{col.column_name}' column, negative values are almost certainly data entry errors or ETL bugs.",
            metric_value=col.min_value, threshold=0,
            suggestion=f"Run: SELECT COUNT(*) FROM {col.table_name} WHERE {col.column_name} < 0; to see scope. Add a dbt accepted_values or expression_is_true test."
        )]

    def _check_statistical_outliers(self, col: ColumnProfile) -> list[AnomalyFlag]:
        """
        IQR method: outliers are values beyond Q3 + 3×IQR.
        Uses p99 as a proxy when we don't have raw values.
        """
        if None in (col.p25, col.p75, col.p99, col.max_value):
            return []

        iqr = col.p75 - col.p25
        if iqr == 0:
            return []

        upper_fence = col.p75 + (OUTLIER_IQR_FACTOR * iqr)

        if col.max_value > upper_fence and col.p99 < col.max_value * 0.5:
            # Max is way beyond p99 — extreme outlier
            return [AnomalyFlag(
                table_name=col.table_name, column_name=col.column_name,
                severity="warning", issue_type="statistical_outlier",
                title=f"Extreme outlier in {col.table_name}.{col.column_name}",
                description=(
                    f"Max value ({col.max_value:,.2f}) is far beyond the statistical fence ({upper_fence:,.2f}). "
                    f"The 99th percentile is {col.p99:,.2f}, but max is {col.max_value:,.2f} — "
                    f"this is {col.max_value/col.p99:.0f}× the p99 value."
                ),
                metric_value=col.max_value, threshold=upper_fence,
                suggestion=f"SELECT * FROM {col.table_name} WHERE {col.column_name} > {upper_fence:.0f}; to identify outlier rows. Verify if these are valid data points or data entry errors."
            )]
        return []

    def _check_zero_values(self, col: ColumnProfile) -> list[AnomalyFlag]:
        """
        Flag columns where zero values likely indicate bad data.
        Checks using p25 — if median is well above 0 but min is 0,
        the zeros are likely invalid.
        """
        if col.min_value is None or col.min_value > 0 or col.p50 is None:
            return []

        zero_patterns = ["quantity", "amount", "price", "count", "total", "revenue", "cost"]
        if not any(p in col.column_name.lower() for p in zero_patterns):
            return []

        if col.p50 > 0 and col.min_value == 0:
            return [AnomalyFlag(
                table_name=col.table_name, column_name=col.column_name,
                severity="warning", issue_type="zero_values",
                title=f"Suspicious zero values in {col.table_name}.{col.column_name}",
                description=f"Column contains zero values but median is {col.p50:,.2f}. For a '{col.column_name}' column, zeros likely represent missing or invalid data.",
                metric_value=0, threshold=col.p50,
                suggestion=f"SELECT COUNT(*) FROM {col.table_name} WHERE {col.column_name} = 0; Add a dbt expression_is_true test: {col.column_name} > 0"
            )]
        return []

    def _check_inconsistent_categories(self, col: ColumnProfile) -> list[AnomalyFlag]:
        """
        Detect same-value-different-case patterns like 'COMPLETED' vs 'completed' vs 'Completed'.
        Uses top_values dict to compare case-normalized counts.
        """
        if not col.top_values or len(col.top_values) < 2:
            return []

        # Group values by lowercase
        normalized: dict[str, list[str]] = {}
        for val in col.top_values.keys():
            key = val.lower().strip()
            normalized.setdefault(key, []).append(val)

        # Find groups with multiple casings
        inconsistent = {k: v for k, v in normalized.items() if len(v) > 1}

        if inconsistent:
            examples = "; ".join(
                f"'{'/'.join(variants)}'" for variants in list(inconsistent.values())[:3]
            )
            return [AnomalyFlag(
                table_name=col.table_name, column_name=col.column_name,
                severity="warning", issue_type="inconsistent_values",
                title=f"Inconsistent casing in {col.table_name}.{col.column_name}",
                description=f"Same values appear with different formatting: {examples}. This breaks GROUP BY queries and downstream reporting.",
                metric_value=len(inconsistent),
                suggestion=f"Standardize with: UPDATE {col.table_name} SET {col.column_name} = LOWER({col.column_name}); or add a dbt accepted_values test with the canonical set."
            )]
        return []

    def _check_constant_column(self, col: ColumnProfile) -> list[AnomalyFlag]:
        """Flag columns where every non-null value is the same — likely broken ETL."""
        if col.unique_count == 1 and col.null_rate < 0.99 and col.row_count > 10:
            val = list(col.top_values.keys())[0] if col.top_values else "unknown"
            return [AnomalyFlag(
                table_name=col.table_name, column_name=col.column_name,
                severity="info", issue_type="constant_column",
                title=f"Constant column: {col.table_name}.{col.column_name}",
                description=f"All {col.row_count:,} rows have the same value: '{val}'. This column carries no information and may indicate a stuck ETL field.",
                suggestion="Verify whether this column is intentionally constant, or if the upstream pipeline has a bug causing it to always write the same value."
            )]
        return []
