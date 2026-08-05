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
        else Path(__file__).resolve().parent.parent
        / "config"
        / "pipeline_metadata.json"
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
    """
    Return configured execution environment.
    """

    env = CONFIG.get("environment", "auto")

    if env == "auto":
        return "databricks" if is_databricks() else "local"

    return env


def get_paths():
    """
    Return storage metadata for the active environment.
    """
    return METADATA["paths"][get_environment()]


def get_base_path():
    """
    Return base storage path.
    """

    paths = get_paths()

    if get_environment() == "local":
        return paths["base_path"]

    return f"/Volumes/{paths['catalog']}/{paths['schema']}/{paths['volume']}"


def get_storage_path(folder_name):
    """
    Return fully qualified storage path for a folder.
    """

    paths = get_paths()

    base = get_base_path()

    folder = paths["folders"].get(folder_name)

    if folder is None:
        raise KeyError(f"Unknown storage folder: {folder_name}")

    return f"{base}/{folder}"


def get_table_name(table_name):
    """
    Return configured table name.
    """

    return get_paths()["tables"][table_name]


def get_config():
    """Return pipeline configuration."""
    return CONFIG


def get_metadata():
    """Return pipeline metadata."""
    return METADATA


def get_pipeline_name():
    """Return pipeline name."""
    return METADATA["pipeline"]["name"]


def get_load_type():
    """Return configured load type."""
    return METADATA["pipeline"]["type"]


def get_file_format():
    """Return source file format."""
    return METADATA["source"]["format"]


def get_target_format():
    """Return target storage format."""
    return METADATA["target"]["format"]
