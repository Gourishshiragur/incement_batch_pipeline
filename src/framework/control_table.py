"""
Enterprise Control Table Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Tracks pipeline execution metadata.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from .constants import STATUS_STARTED

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

    ####################################################################
    # Pipeline Registration
    ####################################################################

    def register_run(
        self,
        pipeline_name: str,
        pipeline_type: str,
        run_id: str,
        batch_id: Optional[str] = None,
        snapshot_date: Optional[str] = None,
        watermark: Optional[str] = None,
        status: str = STATUS_STARTED,
    ) -> None:
        """
        Register a pipeline execution.
        """

        df = self.spark.createDataFrame(
            [
                (
                    pipeline_name,
                    pipeline_type,
                    run_id,
                    batch_id,
                    snapshot_date,
                    watermark,
                    status,
                )
            ],
            [
                "pipeline_name",
                "pipeline_type",
                "run_id",
                "batch_id",
                "snapshot_date",
                "watermark",
                "status",
            ],
        ).withColumn(
            "updated_at",
            current_timestamp(),
        )

        (
            df.write
            .format("delta")
            .mode("append")
            .save(self.control_path)
        )

    ####################################################################
    # Update Status
    ####################################################################

    def update_status(
        self,
        pipeline_name: str,
        run_id: str,
        status: str,
    ) -> None:
        """
        Placeholder for updating run status.

        Future implementation can use
        Delta MERGE instead of append.
        """

        df = self.spark.createDataFrame(
            [
                (
                    pipeline_name,
                    run_id,
                    status,
                )
            ],
            [
                "pipeline_name",
                "run_id",
                "status",
            ],
        ).withColumn(
            "updated_at",
            current_timestamp(),
        )

        (
            df.write
            .format("delta")
            .mode("append")
            .save(self.control_path)
        )

    ####################################################################
    # Watermark
    ####################################################################

    def save_watermark(
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
        ).withColumn(
            "updated_at",
            current_timestamp(),
        )

        (
            df.write
            .format("delta")
            .mode("append")
            .save(self.control_path)
        )

    ####################################################################
    # Future APIs
    ####################################################################

    def latest_watermark(
        self,
        pipeline_name: str,
    ):
        """
        Placeholder.

        Future implementation:
        Return latest watermark
        from Delta table.
        """

        return None