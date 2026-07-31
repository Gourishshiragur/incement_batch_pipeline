"""
Enterprise Schema History Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Tracks schema evolution over time.
"""

from __future__ import annotations

import json
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


class SchemaHistory:
    """
    Enterprise reusable schema history manager.
    """

    def __init__(
        self,
        history_path: str,
    ):

        self.history_path = history_path

    ####################################################################
    # Schema Utilities
    ####################################################################

    @staticmethod
    def schema_dict(
        df: DataFrame,
    ) -> Dict[str, str]:
        """
        Convert Spark schema into dictionary.
        """

        return {
            field.name: field.dataType.simpleString()
            for field in df.schema.fields
        }

    ####################################################################
    # History Writer
    ####################################################################

    def save_schema(
        self,
        df: DataFrame,
        pipeline_name: str,
        stage: str,
    ) -> None:
        """
        Save current schema snapshot.
        """

        schema_json = json.dumps(
            self.schema_dict(df),
            sort_keys=True,
        )

        history_df = (
            df.sparkSession.createDataFrame(
                [
                    (
                        pipeline_name,
                        stage,
                        schema_json,
                    )
                ],
                [
                    "pipeline_name",
                    "stage",
                    "schema_json",
                ],
            )
            .withColumn(
                "recorded_at",
                current_timestamp(),
            )
        )

        (
            history_df.write
            .format("delta")
            .mode("append")
            .save(self.history_path)
        )

    ####################################################################
    # Schema Comparison
    ####################################################################

    @staticmethod
    def compare(
        previous_schema: Dict[str, str],
        current_schema: Dict[str, str],
    ) -> List[str]:
        """
        Compare two schema dictionaries.
        """

        changes = []

        columns = sorted(
            set(previous_schema.keys())
            | set(current_schema.keys())
        )

        for column in columns:

            old_type = previous_schema.get(column)

            new_type = current_schema.get(column)

            if old_type != new_type:

                changes.append(
                    f"{column}: {old_type} -> {new_type}"
                )

        return changes