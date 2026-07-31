"""
Enterprise Logging Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Supports:
- Local execution
- Databricks
"""

from __future__ import annotations

import logging
import sys
from typing import Optional


class PipelineLogger:
    """
    Enterprise reusable logger.

    Creates a single logger instance per pipeline.
    """

    def __init__(
        self,
        pipeline_name: str,
        level: int = logging.INFO,
    ):

        self.pipeline_name = pipeline_name

        self.logger = logging.getLogger(pipeline_name)

        self.logger.setLevel(level)

        # Prevent duplicate log messages
        self.logger.propagate = False

        if not self.logger.handlers:

            handler = logging.StreamHandler(sys.stdout)

            formatter = logging.Formatter(
                fmt="[%(asctime)s] %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            handler.setFormatter(formatter)

            self.logger.addHandler(handler)

    ####################################################################
    # Logging API
    ####################################################################

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def critical(self, message: str) -> None:
        self.logger.critical(message)
        
    ####################################################################
    # Pipeline Lifecycle
    ####################################################################

    def pipeline_started(self) -> None:
        self.info(f"Pipeline '{self.pipeline_name}' started.")

    def pipeline_completed(self) -> None:
        self.info(f"Pipeline '{self.pipeline_name}' completed successfully.")

    def pipeline_failed(
        self,
            error: str | None = None,
    ) -> None:
        if error:
            self.error(
                f"Pipeline '{self.pipeline_name}' failed: {error}"
        )
        else:
            self.error(
                f"Pipeline '{self.pipeline_name}' failed."
        )

    ####################################################################
    # Stage Lifecycle
    ####################################################################

    def stage_started(self, stage: str) -> None:
        self.info(f"{stage} started.")

    def stage_completed(self, stage: str) -> None:
        self.info(f"{stage} completed.")

    def stage_failed(self, stage: str, error: str) -> None:
        self.error(f"{stage} failed: {error}")

    def exception(
        self,
        message: str,
        exception: Optional[Exception] = None,
    ) -> None:
        """
        Log an exception with traceback.
        """

        if exception is None:
            self.logger.exception(message)
        else:
            self.logger.exception(
                f"{message}: {exception}"
            )
            
def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger instance.
    """
    return PipelineLogger(name).logger

__all__ = [
    "PipelineLogger",
    "get_logger",
]