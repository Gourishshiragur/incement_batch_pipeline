"""
Enterprise Framework Utilities

Reusable across:
- Audit Framework
- Logger
- Metrics
- Quarantine
- Schema Validation
"""

from __future__ import annotations

import getpass
import socket
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .constants import (
    DEFAULT_EXECUTION_ENGINE,
    ENV_DATABRICKS,
    ENV_LOCAL,
)


# ============================================================
# ID Helpers
# ============================================================

def generate_run_id() -> str:
    """Generate a unique run identifier."""
    return str(uuid.uuid4())


def generate_execution_id() -> str:
    """Generate a unique execution identifier."""
    return str(uuid.uuid4())


# ============================================================
# Time Helpers
# ============================================================

def utc_now() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


def start_timer() -> float:
    """Start a high-precision timer."""
    return time.perf_counter()


def elapsed_seconds(start_time: float) -> float:
    """Calculate elapsed time in seconds."""
    return round(time.perf_counter() - start_time, 3)


# ============================================================
# Environment Helpers
# ============================================================

def detect_environment(
    spark: SparkSession | None = None,
) -> str:
    """
    Detect whether the pipeline is running
    locally or in Databricks.
    """
    if spark is None:
        return ENV_LOCAL

    try:
        spark.conf.get("spark.databricks.workspaceUrl")
        return ENV_DATABRICKS
    except Exception:
        return ENV_LOCAL


def execution_engine() -> str:
    """Return execution engine."""
    return DEFAULT_EXECUTION_ENGINE


# ============================================================
# Databricks Helpers
# ============================================================

def workspace_name(
    spark: SparkSession | None = None,
) -> str | None:
    """Return Databricks workspace URL if available."""
    
    if spark is None:
        return None

    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        return None


def cluster_name(
    spark: SparkSession | None = None,
) -> str | None:
    """Return Databricks cluster name if available."""
    
    if spark is None:
        return None
    
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
    except Exception:
        return None


# ============================================================
# System Helpers
# ============================================================

def hostname() -> str:
    """Return machine hostname."""
    return socket.gethostname()


def user_name() -> str:
    """Return current operating system user."""
    return getpass.getuser()