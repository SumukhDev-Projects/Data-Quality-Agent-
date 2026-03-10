"""
agents/dq_agent.py
-------------------
WHY THIS FILE EXISTS:
  This is the AI brain of the system. It takes:
    1. A DatabaseProfile (statistical snapshots of every column)
    2. A list of AnomalyFlags (pre-screened statistical issues)
  
  And uses Claude to:
    1. Interpret what the issues mean in business terms
    2. Prioritize which issues matter most
    3. Enrich each flag with a business-context explanation
    4. Write a plain-English executive summary
    5. Generate SQL fix scripts for common issues

  WHY NOT JUST USE THE STATISTICAL DETECTOR?
    The detector flags "null_rate=0.08" — that's a number.
    Claude knows that for an `email` column in a `customers` table,
    8% nulls likely means users skipped optional signup fields — very different
    from 8% nulls in an `order_amount` column, which probably means broken ETL.
    
    Context-aware interpretation is where AI adds value that statistics can't.

ARCHITECTURE: Two-phase AI analysis
  Phase 1 — Per-table deep analysis:
    For each table, Claude reads the profile + flags and identifies issues
    specific to that table's business context.
  
  Phase 2 — Cross-table synthesis:
    Claude looks across all tables to find systemic patterns
    (e.g., a bad ETL job affecting multiple tables at the same time).
    Then writes the executive summary.

TOOL-CALLING:
  We use Claude's tool-calling to get structured JSON output per finding,
  guaranteeing the response can be parsed into AnomalyFinding DB records.
"""

import anthropic
import json
import os
from typing import Any
from services.profiler import DatabaseProfile, TableProfile
from services.anomaly_detector import AnomalyFlag
import logging

logger = logging.getLogger(__name__)

MODEL = "claude-3-5-sonnet-20241022"


class DataQualityAgent:
    """
    AI agent that interprets statistical profiles and anomaly flags
    to produce business-meaningful data quality reports.
    """

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    def analyze(
        self,
        profile: DatabaseProfile,
        flags: list[AnomalyFlag]
    ) -> dict:
        """
        Main analysis pipeline.

        Returns:
            {
                "summary": "Executive summary paragraph...",
                "findings": [
                    {
                        "table_name": ...,
                        "column_name": ...,
                        "severity": "critical"|"warning"|"info",
                        "issue_type": ...,
                        "title": ...,
                        "description": ...,
                        "suggestion": ...,
                        "metric_value": ...,
                        "threshold": ...
                    },
                    ...
                ]
            }
        """
        # Convert pre-screened flags to AI-enrichable findings
        enriched_findings = self._enrich_findings(profile, flags)

        # Generate executive summary
        summary = self._generate_summary(profile, enriched_findings)

        return {
            "summary": summary,
            "findings": enriched_findings
        }

    def _enrich_findings(
        self,
        profile: DatabaseProfile,
        flags: list[AnomalyFlag]
    ) -> list[dict]:
        """
        Uses Claude to enrich each statistical flag with business context.
        Groups flags by table for efficiency (one API call per table).
        """
        if not flags:
            return []

        # Group flags by table
        by_table: dict[str, list[AnomalyFlag]] = {}
        for flag in flags:
            by_table.setdefault(flag.table_name, []).append(flag)

        all_findings = []

        for table_name, table_flags in by_table.items():
            # Find the table profile for context
            table_profile = next(
                (t for t in profile.tables if t.table_name == table_name), None
            )
            table_findings = self._enrich_table_findings(table_name, table_flags, table_profile)
            all_findings.extend(table_findings)

        return all_findings

    def _enrich_table_findings(
        self,
        table_name: str,
        flags: list[AnomalyFlag],
        table_profile
    ) -> list[dict]:
        """
        Enriches all flags for one table with AI interpretation.
        Uses tool-calling to get structured JSON output.
        """
        # Build table context string
        context = self._build_table_context(table_name, flags, table_profile)

        tool = {
            "name": "report_findings",
            "description": "Report enriched data quality findings for this table",
            "input_schema": {
                "type": "object",
                "properties": {
                    "findings": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "table_name":   {"type": "string"},
                                "column_name":  {"type": ["string", "null"]},
                                "severity":     {"type": "string", "enum": ["critical", "warning", "info"]},
                                "issue_type":   {"type": "string"},
                                "title":        {"type": "string"},
                                "description":  {"type": "string", "description": "Plain English explanation of why this matters and what impact it has"},
                                "suggestion":   {"type": "string", "description": "Specific, actionable fix with example SQL or dbt test"},
                                "metric_value": {"type": ["number", "null"]},
                                "threshold":    {"type": ["number", "null"]}
                            },
                            "required": ["table_name", "severity", "issue_type", "title", "description", "suggestion"]
                        }
                    }
                },
                "required": ["findings"]
            }
        }

        try:
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=2000,
                system="""You are a senior data engineer reviewing data quality issues.
For each flagged issue, provide:
1. A clear business-context explanation (why does this matter, not just what the number is)
2. A specific, actionable suggestion including example SQL or dbt test syntax
3. Accurate severity assessment — be conservative, don't over-escalate

Write as if explaining to a data analyst who needs to fix the issue today.""",
                tools=[tool],
                tool_choice={"type": "tool", "name": "report_findings"},
                messages=[{"role": "user", "content": context}]
            )

            for block in response.content:
                if block.type == "tool_use" and block.name == "report_findings":
                    return block.input.get("findings", [])

        except Exception as e:
            logger.error(f"AI enrichment failed for {table_name}: {e}")
            # Fall back to returning the raw flags as findings
            return [self._flag_to_dict(f) for f in flags]

        return [self._flag_to_dict(f) for f in flags]

    def _build_table_context(
        self,
        table_name: str,
        flags: list[AnomalyFlag],
        table_profile
    ) -> str:
        """Builds the context string for AI enrichment of one table's issues."""
        context_parts = [f"## Table: {table_name}"]

        if table_profile:
            context_parts.append(
                f"Rows: {table_profile.row_count:,} | Columns: {table_profile.column_count}"
            )
            # Add column profile summaries
            col_summaries = []
            for col in table_profile.columns[:10]:  # limit to avoid token overrun
                summary = (
                    f"  - {col.column_name} ({col.data_type}): "
                    f"null_rate={col.null_rate*100:.1f}%, "
                    f"unique={col.unique_count}"
                )
                if col.min_value is not None:
                    summary += f", range=[{col.min_value}, {col.max_value}]"
                if col.top_values:
                    top = list(col.top_values.items())[:4]
                    summary += f", top_values={dict(top)}"
                col_summaries.append(summary)
            context_parts.append("Column profiles:\n" + "\n".join(col_summaries))

        context_parts.append("\n## Flagged Issues:")
        for flag in flags:
            context_parts.append(
                f"\n### {flag.severity.upper()}: {flag.title}\n"
                f"Issue type: {flag.issue_type}\n"
                f"Column: {flag.column_name or 'TABLE-LEVEL'}\n"
                f"Details: {flag.description}\n"
                f"Metric: {flag.metric_value} (threshold: {flag.threshold})"
            )

        context_parts.append(
            "\nEnrich each finding with business-context explanation and actionable suggestion."
        )
        return "\n".join(context_parts)

    def _generate_summary(
        self,
        profile: DatabaseProfile,
        findings: list[dict]
    ) -> str:
        """
        Generates a plain-English executive summary of the entire analysis.
        This is what shows up at the top of the report.
        """
        critical = [f for f in findings if f.get("severity") == "critical"]
        warnings = [f for f in findings if f.get("severity") == "warning"]

        stats = f"""
Database Overview:
- {profile.total_tables} tables analyzed
- {profile.total_columns} columns profiled
- {len(findings)} total issues found ({len(critical)} critical, {len(warnings)} warnings)

Critical Issues:
{chr(10).join(f"- {f['title']}" for f in critical[:5]) or "None"}

Top Warnings:
{chr(10).join(f"- {f['title']}" for f in warnings[:5]) or "None"}
"""

        try:
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=500,
                system="""You are a data quality lead writing an executive summary.
Write 2-3 concise paragraphs. Be specific about the most important issues.
Focus on business impact and priority. Don't list every finding — summarize the pattern.
Write in plain English, no markdown headers.""",
                messages=[{
                    "role": "user",
                    "content": f"Write an executive summary for this data quality report:\n{stats}"
                }]
            )
            return response.content[0].text.strip()

        except Exception as e:
            logger.error(f"Summary generation failed: {e}")
            return (
                f"Data quality analysis complete. Found {len(findings)} issues across "
                f"{profile.total_tables} tables: {len(critical)} critical, {len(warnings)} warnings. "
                "Review individual findings for details."
            )

    def _flag_to_dict(self, flag: AnomalyFlag) -> dict:
        """Converts a raw AnomalyFlag to a dict (fallback when AI enrichment fails)."""
        return {
            "table_name":   flag.table_name,
            "column_name":  flag.column_name,
            "severity":     flag.severity,
            "issue_type":   flag.issue_type,
            "title":        flag.title,
            "description":  flag.description,
            "suggestion":   flag.suggestion,
            "metric_value": flag.metric_value,
            "threshold":    flag.threshold
        }
