"""
Enterprise Framework Constants

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

# ============================================================
# Framework
# ============================================================

FRAMEWORK_VERSION = "1.0.0"

DEFAULT_EXECUTION_ENGINE = "Spark"

# ============================================================
# Environments
# ============================================================

ENV_LOCAL = "local"

ENV_DATABRICKS = "databricks"

# ============================================================
# Pipeline Status
# ============================================================
STATUS_SKIPPED = "SKIPPED"

STATUS_STARTED = "STARTED"

STATUS_RUNNING = "RUNNING"

STATUS_SUCCESS = "SUCCESS"

STATUS_FAILED = "FAILED"

STATUS_WARNING = "WARNING"

# ============================================================
# Pipeline Types
# ============================================================

PIPELINE_INCREMENTAL = "incremental"

PIPELINE_MICROBATCH = "micro_batch"

PIPELINE_STREAMING = "streaming"

# ============================================================
# Trigger Types
# ============================================================

TRIGGER_MANUAL = "MANUAL"

TRIGGER_SCHEDULE = "SCHEDULE"

TRIGGER_EVENT = "EVENT"

TRIGGER_API = "API"

# ============================================================
# Common Stages
# ============================================================

STAGE_BRONZE = "BRONZE"

STAGE_SILVER = "SILVER"

STAGE_GOLD = "GOLD"

STAGE_VALIDATION = "VALIDATION"

STAGE_RECONCILIATION = "RECONCILIATION"

STAGE_QUARANTINE = "QUARANTINE"
STAGE_AUDIT = "AUDIT"

STAGE_CONTROL = "CONTROL"

# ============================================================
# Default Values
# ============================================================

DEFAULT_RETRY_COUNT = 0

DEFAULT_FILES_PROCESSED = 0

DEFAULT_ROWS = 0
DEFAULT_DURATION = 0.0
