"""
Enterprise Quarantine Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Stores invalid records for later analysis.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


class QuarantineManager:
    """
    Enterprise reusable quarantine manager.
    """

    def __init__(
        self,
        quarantine_path: str,
    ):

        self.quarantine_path = quarantine_path

    ####################################################################
    # Public API
    ####################################################################

    def quarantine(
        self,
        df: DataFrame,
        reason: str,
        stage: str,
        pipeline_name: Optional[str] = None,
    ) -> int:
        """
        Write invalid records into quarantine storage.

        Returns:
            Number of quarantined records.
        """

        quarantine_df = (

            df

            .withColumn(
                "quarantine_reason",
                lit(reason),
            )

            .withColumn(
                "pipeline_name",
                lit(pipeline_name),
            )

            .withColumn(
                "stage",
                lit(stage),
            )

            .withColumn(
                "quarantined_at",
                current_timestamp(),
            )

        )

        (
            quarantine_df.write
            .format("delta")
            .mode("append")
            .save(self.quarantine_path)
        )

        return quarantine_df.count()

    ####################################################################
    # Utilities
    ####################################################################

    def exists(self) -> bool:
        """
        Placeholder for future implementation.

        Can later verify whether the
        quarantine location exists.
        """

        return True