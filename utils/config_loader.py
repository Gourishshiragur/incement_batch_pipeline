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

def _config_path():
    """Return pipeline configuration file path based on environment."""
    return (
        "/Volumes/workspace/default/incremental_batch/config/pipeline_config.json"
        if is_databricks()
        else Path(__file__).resolve().parent.parent / "config" / "pipeline_config.json"
    )


with open(_config_path(), "r") as f:
    CONFIG = json.load(f)
 
    
def get_environment():
    """Return current execution environment."""
    return "databricks" if is_databricks() else "local"


def get_paths():
    """Return storage paths for the active environment."""
    return METADATA["paths"][get_environment()]


def get_config():
    """Return pipeline configuration."""
    return CONFIG


def get_metadata():
    """Return pipeline metadata."""
    return METADATA


def get_pipeline_name():
    """Return pipeline name."""
    return METADATA["pipeline_name"]


def get_load_type():
    """Return configured load type."""
    return METADATA["load_type"]


def get_file_format():
    """Return source file format."""
    return METADATA["file_format"]


def get_target_format():
    """Return target storage format."""
    return METADATA["target_format"]