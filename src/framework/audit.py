"""
Enterprise Audit Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Supports:
- Local Spark
- Databricks
"""

from __future__ import annotations
from datetime import datetime
from typing import Any, Optional
from .constants import (
    DEFAULT_FILES_PROCESSED,
    DEFAULT_RETRY_COUNT,
    STATUS_SUCCESS,
    STATUS_FAILED,
)

from .utils import (
    cluster_name,
    detect_environment,
    elapsed_seconds,
    execution_engine,
    generate_execution_id,
    generate_run_id,
    hostname,
    user_name,
    utc_now,
    workspace_name,
)

from .constants import (
    DEFAULT_FILES_PROCESSED,
    DEFAULT_RETRY_COUNT,
    STATUS_SUCCESS,
    STATUS_FAILED,
    FRAMEWORK_VERSION,
)

class AuditFramework:
    """
    Enterprise reusable audit framework.

    One object represents one pipeline execution.
    """

    def __init__(
        self,
        pipeline_name: str,
        pipeline_type: str,
        job_name: Optional[str] = None,
        trigger_type: Optional[str] = None,
    ):
        
        self.pipeline_name = pipeline_name

        self.pipeline_type = pipeline_type

        self.job_name = job_name

        self.trigger_type = trigger_type

        self.run_id = generate_run_id()

        self.execution_id = generate_execution_id()

        self.environment = detect_environment()
        
        self.execution_engine = execution_engine()

        self.workspace_name = workspace_name()

        self.cluster_name = cluster_name()

        self.hostname = hostname()

        self.user_name = user_name()
        
        self.started_at: Optional[datetime] = None
        
        self.completed_at: Optional[datetime] = None
        
        self.retry_count = DEFAULT_RETRY_COUNT

    ####################################################################
    # Public API
    ####################################################################

    def start_run(self) -> str:
        """
        Start pipeline execution.
        """

        self.started_at = utc_now()

        return self.run_id

    def get_run_id(self) -> str:

        return self.run_id

    def get_execution_id(self) -> str:
        return self.execution_id

    ####################################################################
    # Internal Helpers
    ####################################################################

    def _build_record(
        self,
        stage: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        files_processed: int = DEFAULT_FILES_PROCESSED,
        source_name: Optional[str] = None,
        source_path: Optional[str] = None,
        target_name: Optional[str] = None,
        target_path: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        duration_seconds: float = 0.0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> dict:
        """
        Build a single audit record.
        """
        return {

            # Run Information
            "run_id": self.run_id,
            "execution_id": self.execution_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_type": self.pipeline_type,
            "stage": stage,
            "status": status,

            # Pipeline Information
            "job_name": self.job_name,
            "trigger_type": self.trigger_type,
            "retry_count": self.retry_count,

            # Data Statistics
            "rows_read": int(rows_read),
            "rows_written": int(rows_written),
            "rows_rejected": int(rows_rejected),
            "files_processed": int(files_processed),

            # Source / Target
            "source_name": source_name,
            "source_path": source_path,
            "target_name": target_name,
            "target_path": target_path,

            # Execution Context
            "metadata": metadata or {},

            # Environment
            "environment": self.environment,
            "execution_engine": self.execution_engine,
            "workspace_name": self.workspace_name,
            "cluster_name": self.cluster_name,
            "hostname": self.hostname,
            "user_name": self.user_name,

            # Performance
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": float(duration_seconds),

            # Errors
            "error_type": error_type,
            "error_message": error_message,

            # Metadata
            "created_at": utc_now(),
            "framework_version": FRAMEWORK_VERSION,
        }

    ####################################################################
    # Audit API
    ####################################################################

    def log_stage(
        self,
        stage: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        files_processed: int = DEFAULT_FILES_PROCESSED,
        source_name: Optional[str] = None,
        source_path: Optional[str] = None,
        target_name: Optional[str] = None,
        target_path: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        duration_seconds: float = 0.0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> dict:
        """
        Write one audit record.
        """
        record = self._build_record(
            stage=stage,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            files_processed=files_processed,
            source_name=source_name,
            source_path=source_path,
            target_name=target_name,
            target_path=target_path,
            metadata=metadata,
            duration_seconds=duration_seconds,
            error_type=error_type,
            error_message=error_message,
            
        )
        
        return record

    def finish_run(
        self,
        stage: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        files_processed: int = DEFAULT_FILES_PROCESSED,
        source_name: Optional[str] = None,
        source_path: Optional[str] = None,
        target_name: Optional[str] = None,
        target_path: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        timer_start: Optional[float] = None,
    ) -> dict:
        """
        Write SUCCESS audit record.
        """
        duration = (
            elapsed_seconds(timer_start)
            if timer_start is not None
            else 0.0
        )
        
        self.completed_at = utc_now()
        return self.log_stage(
            stage=stage,
            status=STATUS_SUCCESS,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            files_processed=files_processed,
            source_name=source_name,
            source_path=source_path,
            target_name=target_name,
            target_path=target_path,
            metadata=metadata,
            duration_seconds=duration,
        )

    def fail_run(
        self,
        stage: str,
        exception: Exception,
        timer_start: Optional[float] = None,
    ) -> dict:
        """
        Write FAILED audit record.
        """

        duration = (
            elapsed_seconds(timer_start)
            if timer_start is not None
            else 0.0
        )

        self.completed_at = utc_now()

        return self.log_stage(
            stage=stage,
            status=STATUS_FAILED,
            duration_seconds=duration,
            error_type=type(exception).__name__,
            error_message=str(exception),
        )