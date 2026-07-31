import json
import os
from pathlib import Path


def is_databricks():
    """Return True when running inside Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _metadata_path():
    """Return metadata file path based on environment."""
    return (
        "/Volumes/workspace/default/incremental_batch/config/pipeline_metadata.json"
        if is_databricks()
        else Path(__file__).resolve().parent.parent / "config" / "pipeline_metadata.json"
    )


# Load metadata only once
with open(_metadata_path(), "r") as f:
    METADATA = json.load(f)


def get_paths():
    env = "databricks" if is_databricks() else "local"
    return METADATA["paths"][env]


def get_pipeline_name():
    return METADATA["pipeline_name"]


def get_load_type():
    return METADATA["load_type"]


def get_file_format():
    return METADATA["file_format"]


def get_target_format():
    return METADATA["target_format"]