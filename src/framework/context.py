"""
Enterprise Framework Context

Initializes and exposes all framework services.

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from .audit import AuditFramework
from .control_table import ControlTable
from .error_logger import ErrorLogger
from .logger import PipelineLogger
from .metrics import PipelineMetrics
from .quarantine import QuarantineManager
from .schema_history import SchemaHistory
from .schema_validator import SchemaValidator


class FrameworkContext:
    """
    Central entry point for the enterprise framework.

    Creates and exposes all reusable framework components.
    """

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
        pipeline_type: str,
        control_path: str,
        quarantine_path: str,
        schema_history_path: str,
        job_name: str | None = None,
        trigger_type: str | None = None,
    ):

        ###########################################################
        # Logger
        ###########################################################

        self.logger = PipelineLogger(
            pipeline_name=pipeline_name
        )

        ###########################################################
        # Metrics
        ###########################################################

        self.metrics = PipelineMetrics()

        ###########################################################
        # Audit
        ###########################################################

        self.audit = AuditFramework(
            pipeline_name=pipeline_name,
            pipeline_type=pipeline_type,
            job_name=job_name,
            trigger_type=trigger_type,
        )
        
        ###########################################################
        # Error Logger
        ###########################################################

        self.error_logger = ErrorLogger(
            logger=self.logger
        )

        ###########################################################
        # Schema
        ###########################################################

        self.validator = SchemaValidator()

        self.schema_history = SchemaHistory(
            history_path=schema_history_path,
        )

        ###########################################################
        # Quarantine
        ###########################################################

        self.quarantine = QuarantineManager(
            quarantine_path=quarantine_path,
        )

        ###########################################################
        # Control Table
        ###########################################################

        self.control = ControlTable(
            spark=spark,
            control_path=control_path,
        )