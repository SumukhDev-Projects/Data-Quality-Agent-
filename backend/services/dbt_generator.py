"""
services/dbt_generator.py
--------------------------
WHY THIS FILE EXISTS:
  One of the most valuable outputs of this agent is a dbt schema.yml file
  with auto-generated tests based on what was found in the data.

  Instead of a data engineer manually writing tests like:
    - not_null for columns that should never be null
    - accepted_values for category columns
    - relationships for foreign keys

  ...the agent generates them automatically from the profile.
  This directly saves hours of work.

WHAT IT GENERATES:
  - not_null tests for columns with null_rate = 0 (never null → should always be filled)
  - unique tests for columns with unique_rate = 1.0 (perfect uniqueness → enforce it)
  - accepted_values tests for low-cardinality categorical columns
  - expression_is_true tests for numeric constraints (price > 0, quantity > 0)
  - relationships tests for detected foreign key columns (column named *_id)

FORMAT:
  Standard dbt schema.yml format — drop directly into your dbt project's models/ folder.
"""

import yaml
from services.profiler import DatabaseProfile, ColumnProfile

# Columns that should be > 0 based on name
POSITIVE_VALUE_COLUMNS = {"price", "amount", "quantity", "cost", "revenue", "total", "count"}

# Patterns suggesting a foreign key
FK_PATTERNS = {"_id", "_fk"}


class DbtTestGenerator:
    """
    Generates a dbt schema.yml from a DatabaseProfile.

    Usage:
        gen = DbtTestGenerator()
        yaml_str = gen.generate(db_profile, model_prefix="stg_")
    """

    def generate(self, profile: DatabaseProfile, model_prefix: str = "") -> str:
        """
        Returns a YAML string ready to save as schema.yml.
        model_prefix: optional prefix to add to model names (e.g. "stg_" for staging)
        """
        models = []

        for table in profile.tables:
            model = {
                "name": f"{model_prefix}{table.table_name}",
                "description": f"Auto-profiled by DQA — {table.row_count:,} rows, {table.column_count} columns.",
                "columns": []
            }

            for col in table.columns:
                col_def = self._build_column_tests(col)
                if col_def:
                    model["columns"].append(col_def)

            models.append(model)

        schema = {
            "version": 2,
            "models": models
        }

        # Use block style for readability — easier to diff in git
        return yaml.dump(
            schema,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            indent=2
        )

    def _build_column_tests(self, col: ColumnProfile) -> dict | None:
        """
        Builds the test list for one column.
        Returns None if no tests apply.
        """
        tests = []
        col_lower = col.column_name.lower()

        # not_null — if null_rate is exactly 0, this column is always populated
        if col.null_rate == 0.0 and col.row_count > 0:
            tests.append("not_null")

        # unique — if every value is distinct, enforce uniqueness
        if col.unique_rate == 1.0 and col.row_count > 1 and col.null_rate == 0.0:
            tests.append("unique")

        # accepted_values — for low-cardinality categoricals
        if (col.value_counts and 2 <= len(col.value_counts) <= 20
                and col.unique_rate < 0.1):
            # Use lowercase-normalized canonical values
            canonical = sorted(set(v.lower().strip() for v in col.value_counts.keys()))
            tests.append({
                "accepted_values": {
                    "values": canonical
                }
            })

        # expression_is_true — for numeric columns that should be > 0
        if col.min_value is not None:
            should_be_positive = any(p in col_lower for p in POSITIVE_VALUE_COLUMNS)
            if should_be_positive and col.min_value >= 0:
                # Only add if data currently passes (don't generate failing tests)
                tests.append({
                    "dbt_utils.expression_is_true": {
                        "expression": f"{col.column_name} >= 0"
                    }
                })

        # relationships — FK columns (name ends with _id)
        # Note: generates a template you fill in — we don't know the target table
        if any(col_lower.endswith(p) for p in FK_PATTERNS) and col_lower != "id":
            # Guess the referenced table from column name
            ref_table = col_lower.replace("_id", "").replace("_fk", "") + "s"
            tests.append({
                "relationships": {
                    "to": f"ref('{ref_table}')",
                    "field": "id",
                    "_comment": "REVIEW: verify table and field names"
                }
            })

        if not tests:
            return None

        return {
            "name": col.column_name,
            "description": f"{col.data_type} | null_rate={col.null_rate*100:.1f}% | unique={col.unique_count:,}",
            "tests": tests
        }
