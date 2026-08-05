"""
Enterprise Schema History Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Tracks schema evolution over time.
"""

from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
import json
from typing import Dict, List
import hashlib


from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col


class SchemaHistory:
    """
    Enterprise reusable schema history manager.
    """

    def __init__(
        self,
        schema_history_path: str,
        schema_changes_path: str,
    ):
        self.schema_history_path = schema_history_path
        self.schema_changes_path = schema_changes_path

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

        return {field.name: field.dataType.simpleString() for field in df.schema.fields}

    @staticmethod
    def schema_hash(schema_json: str) -> str:
        """
        Return SHA256 hash of a schema JSON.
        """

        return hashlib.sha256(schema_json.encode("utf-8")).hexdigest()

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

        schema_hash = self.schema_hash(schema_json)

        latest_hash = self.load_latest_hash(
            df.sparkSession,
            pipeline_name,
            stage,
        )

        if latest_hash == schema_hash:
            return

        version = self.get_next_version(
            df.sparkSession,
            pipeline_name,
            stage,
        )

        history_df = df.sparkSession.createDataFrame(
            [
                (
                    pipeline_name,
                    stage,
                    version,
                    schema_hash,
                    schema_json,
                )
            ],
            [
                "pipeline_name",
                "stage",
                "version",
                "schema_hash",
                "schema_json",
            ],
        ).withColumn(
            "recorded_at",
            current_timestamp(),
        )

        (history_df.write.format("delta").mode("append").save(self.schema_history_path))

    def load_latest_schema(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
    ) -> Dict[str, str] | None:
        """
        Return latest saved schema for a pipeline stage.
        """

        try:

            history_df = (
                spark.read.format("delta")
                .load(self.schema_history_path)
                .filter(col("pipeline_name") == pipeline_name)
                .filter(col("stage") == stage)
                .orderBy(col("version").desc())
                .limit(1)
            )
        except AnalysisException:
            return None

        row = history_df.first()

        if row is None:
            return None

        return json.loads(row["schema_json"])

    def load_latest_hash(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
    ) -> str | None:
        """
        Return the latest schema hash for a pipeline stage.
        """

        try:
            history_df = (
                spark.read.format("delta")
                .load(self.schema_history_path)
                .filter(col("pipeline_name") == pipeline_name)
                .filter(col("stage") == stage)
                .orderBy(col("version").desc())
                .limit(1)
            )
        except AnalysisException:
            return None

        row = history_df.first()

        if row is None:
            return None

        return row["schema_hash"]

    def get_next_version(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
    ) -> int:
        """
        Return the next schema version for a pipeline stage.
        """

        try:
            history_df = (
                spark.read.format("delta")
                .load(self.schema_history_path)
                .filter(col("pipeline_name") == pipeline_name)
                .filter(col("stage") == stage)
            )
        except AnalysisException:
            return 1

        latest_version = history_df.agg({"version": "max"}).first()[0]

        if latest_version is None:
            return 1

        return latest_version + 1

    ####################################################################
    # Schema Comparison
    ####################################################################

    @staticmethod
    def compare(
        previous_schema: Dict[str, str],
        current_schema: Dict[str, str],
    ) -> List[dict]:
        """
        Compare two schema dictionaries.
        """

        changes = []

        columns = sorted(set(previous_schema.keys()) | set(current_schema.keys()))

        for column in columns:

            old_type = previous_schema.get(column)

            new_type = current_schema.get(column)

            if old_type is None:

                changes.append(
                    {
                        "change_type": "COLUMN_ADDED",
                        "column": column,
                        "old_type": None,
                        "new_type": new_type,
                    }
                )

            elif new_type is None:

                changes.append(
                    {
                        "change_type": "COLUMN_REMOVED",
                        "column": column,
                        "old_type": old_type,
                        "new_type": None,
                    }
                )

            elif old_type != new_type:

                changes.append(
                    {
                        "change_type": "TYPE_CHANGED",
                        "column": column,
                        "old_type": old_type,
                        "new_type": new_type,
                    }
                )

        return changes

    def record_changes(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
        changes: List[dict],
        action: str,
    ) -> None:
        """
        Persist schema evolution events.
        """

        if not changes:
            return

        rows = []

        for change in changes:

            rows.append(
                (
                    pipeline_name,
                    stage,
                    change["change_type"],
                    change["column"],
                    change["old_type"],
                    change["new_type"],
                    action,
                )
            )

        history_df = spark.createDataFrame(
            rows,
            [
                "pipeline_name",
                "stage",
                "change_type",
                "column_name",
                "old_type",
                "new_type",
                "action",
            ],
        ).withColumn(
            "recorded_at",
            current_timestamp(),
        )

        (history_df.write.format("delta").mode("append").save(self.schema_changes_path))

    def has_schema_changed(
        self,
        previous_schema: Dict[str, str] | None,
        current_schema: Dict[str, str],
    ) -> bool:
        """
        Return True if the schema has changed.
        """

        if previous_schema is None:
            return True

        return (
            len(
                self.compare(
                    previous_schema,
                    current_schema,
                )
            )
            > 0
        )

    def schema_report(
        self,
        changes: List[dict],
    ) -> str:
        """
        Return a human-readable schema evolution report.
        """

        if not changes:
            return "No schema changes detected."

        report = ["Schema Evolution Report"]

        for change in changes:

            report.append(
                (
                    f"[{change['change_type']}] "
                    f"{change['column']} "
                    f"({change['old_type']} -> {change['new_type']})"
                )
            )

        return "\n".join(report)
