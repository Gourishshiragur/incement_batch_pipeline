"""
Enterprise Error Logger

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from __future__ import annotations

import traceback
from typing import Optional

from .logger import PipelineLogger


class ErrorLogger:
    """
    Enterprise reusable error logger.
    """

    def __init__(
        self,
        logger: PipelineLogger,
    ):

        self.logger = logger

    ####################################################################
    # Public API
    ####################################################################

    def log_exception(
        self,
        stage: str,
        exception: Exception,
        pipeline_name: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> dict:
        """
        Log an exception and return structured metadata.
        """

        error_record = {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "stage": stage,
            "error_type": type(exception).__name__,
            "error_message": str(exception),
            "stack_trace": traceback.format_exc(),
        }

        self.logger.exception(
            f"[{stage}] Pipeline failed",
            exception,
        )

        return error_record