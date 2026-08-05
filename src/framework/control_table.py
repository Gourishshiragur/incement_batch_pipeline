"""
Enterprise Control Table Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Tracks pipeline execution metadata.
"""

from __future__ import annotations
from .control_schema import CONTROL_SCHEMA
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)
from .constants import (
    STATUS_STARTED,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_SKIPPED,
)


class ControlTable:
    """
    Enterprise reusable control table manager.
    """

    def __init__(
        self,
        spark: SparkSession,
        control_path: str,
    ):

        self.spark = spark

        self.control_path = control_path

    def start_run(
        self,
        pipeline_name: str,
        pipeline_type: str,
        run_id: str,
        execution_mode: str = "SNAPSHOT",
        trigger_type: str = "MANUAL",
        batch_id: str | None = None,
        snapshot_date: str | None = None,
        source_file: str | None = None,
        watermark: str | None = None,
    ) -> None:
        """
        Register the start of a pipeline execution.
        """
        self.register_run(
            pipeline_name=pipeline_name,
            pipeline_type=pipeline_type,
            run_id=run_id,
            execution_mode=execution_mode,
            trigger_type=trigger_type,
            batch_id=batch_id,
            snapshot_date=snapshot_date,
            source_file=source_file,
            watermark=watermark,
            status=STATUS_STARTED,
        )

    def finish_run(
        self,
        pipeline_name: str,
        run_id: str,
        rows_read: int | None = None,
        rows_written: int | None = None,
        duration_seconds: int | None = None,
    ) -> None:
        """

        Mark pipeline execution as completed successfully.

        Future implementation will update the existing control
        record using Delta MERGE instead of append.
        """
        self.update_status(
            pipeline_name=pipeline_name,
            run_id=run_id,
            status=STATUS_SUCCESS,
            rows_read=rows_read,
            rows_written=rows_written,
            duration_seconds=duration_seconds,
        )

    def fail_run(
        self,
        pipeline_name: str,
        run_id: str,
        error_message: str | None = None,
    ) -> None:
        """
        Mark pipeline execution as failed.
        """

        self.update_status(
            pipeline_name=pipeline_name,
            run_id=run_id,
            status=STATUS_FAILED,
            error_message=error_message,
        )

    def skip_run(
        self,
        pipeline_name: str,
        run_id: str,
        rows_read: int | None = 0,
        rows_written: int | None = 0,
        duration_seconds: int | None = None,
    ) -> None:
        """
        Mark pipeline execution as skipped.
        """

        self.update_status(
            pipeline_name=pipeline_name,
            run_id=run_id,
            status=STATUS_SKIPPED,
            rows_read=rows_read,
            rows_written=rows_written,
            duration_seconds=duration_seconds,
        )

    ####################################################################
    # Pipeline Registration
    ####################################################################

    def register_run(
        self,
        pipeline_name: str,
        pipeline_type: str,
        run_id: str,
        execution_mode: str = "SNAPSHOT",
        trigger_type: str = "MANUAL",
        batch_id: Optional[str] = None,
        snapshot_date: Optional[str] = None,
        source_file: Optional[str] = None,
        watermark: Optional[str] = None,
        status: str = STATUS_STARTED,
    ) -> None:
        """
        Register a pipeline execution.

        """

        df = (
            self.spark.createDataFrame(
                [
                    Row(
                        pipeline_name=pipeline_name,
                        pipeline_type=pipeline_type,
                        run_id=run_id,
                        execution_mode=execution_mode,
                        trigger_type=trigger_type,
                        batch_id=batch_id,
                        snapshot_date=snapshot_date,
                        source_file=source_file,
                        watermark=watermark,
                        status=status,
                        rows_read=None,
                        rows_written=None,
                        duration_seconds=None,
                        error_message=None,
                        start_time=None,
                        end_time=None,
                        updated_at=None,
                    )
                ],
                schema=CONTROL_SCHEMA,
            )
            .withColumn(
                "start_time",
                current_timestamp(),
            )
            .withColumn(
                "end_time",
                lit(None).cast("timestamp"),
            )
            .withColumn(
                "updated_at",
                current_timestamp(),
            )
        )

        (df.write.format("delta").mode("append").save(self.control_path))

    ####################################################################
    # Update Status
    ####################################################################

    def update_status(
        self,
        pipeline_name: str,
        run_id: str,
        status: str,
        rows_read: int | None = None,
        rows_written: int | None = None,
        duration_seconds: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Placeholder for updating run status.

        Future implementation can use
        Delta MERGE instead of append.
        """
        schema = StructType(
            [
                StructField("pipeline_name", StringType(), False),
                StructField("run_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("rows_read", LongType(), True),
                StructField("rows_written", LongType(), True),
                StructField("duration_seconds", LongType(), True),
                StructField("error_message", StringType(), True),
            ]
        )
        df = (
            self.spark.createDataFrame(
                [
                    Row(
                        pipeline_name=pipeline_name,
                        run_id=run_id,
                        status=status,
                        rows_read=rows_read,
                        rows_written=rows_written,
                        duration_seconds=duration_seconds,
                        error_message=error_message,
                    )
                ],
                schema=schema,
            )
            .withColumn(
                "end_time",
                current_timestamp(),
            )
            .withColumn(
                "updated_at",
                current_timestamp(),
            )
        )

        if not DeltaTable.isDeltaTable(self.spark, self.control_path):
            (df.write.format("delta").mode("append").save(self.control_path))
            return

        control_table = DeltaTable.forPath(
            self.spark,
            self.control_path,
        )

        (
            control_table.alias("t")
            .merge(
                df.alias("s"),
                "t.pipeline_name = s.pipeline_name " "AND t.run_id = s.run_id",
            )
            .whenMatchedUpdate(
                set={
                    "status": "s.status",
                    "rows_read": "s.rows_read",
                    "rows_written": "s.rows_written",
                    "error_message": "s.error_message",
                    "end_time": "s.end_time",
                    "updated_at": "s.updated_at",
                }
            )
            .execute()
        )

    ####################################################################
    # Watermark
    ####################################################################

    def update_watermark(
        self,
        pipeline_name: str,
        watermark: str,
    ) -> None:
        """
        Save latest processed watermark.
        """

        df = self.spark.createDataFrame(
            [
                (
                    pipeline_name,
                    watermark,
                )
            ],
            [
                "pipeline_name",
                "watermark",
            ],
        ).withColumn("updated_at", current_timestamp())

        if not DeltaTable.isDeltaTable(self.spark, self.control_path):
            (df.write.format("delta").mode("append").save(self.control_path))
            return

        control_table = DeltaTable.forPath(
            self.spark,
            self.control_path,
        )

        (
            control_table.alias("t")
            .merge(
                df.alias("s"),
                "t.pipeline_name = s.pipeline_name",
            )
            .whenMatchedUpdate(
                set={
                    "watermark": "s.watermark",
                    "updated_at": "s.updated_at",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    ####################################################################
    # Future APIs
    ####################################################################

    def latest_watermark(
        self,
        pipeline_name: str,
    ) -> str | None:
        """
        Return latest processed watermark.
        """

        if not DeltaTable.isDeltaTable(self.spark, self.control_path):
            return None

        df = (
            self.spark.read.format("delta")
            .load(self.control_path)
            .filter(f"pipeline_name = '{pipeline_name}'")
            .select("watermark")
            .orderBy("updated_at", ascending=False)
            .limit(1)
        )

        rows = df.collect()

        if not rows:
            return None

        return rows[0]["watermark"]

    def last_stage_status(
        self,
        pipeline_name: str,
        source_file: str,
    ) -> str | None:
        """
        Return latest execution status for a source file.
        """

        if not DeltaTable.isDeltaTable(
            self.spark,
            self.control_path,
        ):
            return None

        rows = (
            self.spark.read.format("delta")
            .load(self.control_path)
            .filter(f"pipeline_name = '{pipeline_name}'")
            .filter(f"source_file = '{source_file}'")
            .orderBy("updated_at", ascending=False)
            .select("status")
            .limit(1)
            .collect()
        )

        if not rows:
            return None

        return rows[0]["status"]

    def already_processed(
        self,
        pipeline_name: str,
        source_file: str,
    ) -> bool:
        """
        Return True if the source file has already been
        processed successfully.
        """

        if not DeltaTable.isDeltaTable(
            self.spark,
            self.control_path,
        ):
            return False

        df = (
            self.spark.read.format("delta")
            .load(self.control_path)
            .filter(f"pipeline_name = '{pipeline_name}'")
            .filter(f"source_file = '{source_file}'")
            .filter("status = 'SUCCESS'")
        )

        return df.limit(1).count() > 0
