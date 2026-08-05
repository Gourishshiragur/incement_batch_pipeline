import json
import os
from pathlib import Path


def is_databricks():
    """Return True when running inside Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# Repository root (works for local development and Databricks Repos)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _metadata_path() -> Path:
    """Return the metadata configuration file path."""
    return PROJECT_ROOT / "config" / "pipeline_metadata.json"


def _config_path() -> Path:
    """Return the pipeline configuration file path."""
    return PROJECT_ROOT / "config" / "pipeline_config.json"


# ---------------------------------------------------------------------
# Load Metadata
# ---------------------------------------------------------------------

metadata_file = _metadata_path()

if not metadata_file.exists():
    raise FileNotFoundError(f"Metadata configuration file not found: {metadata_file}")

with metadata_file.open("r", encoding="utf-8") as f:
    METADATA = json.load(f)


# ---------------------------------------------------------------------
# Load Configuration
# ---------------------------------------------------------------------

config_file = _config_path()

if not config_file.exists():
    raise FileNotFoundError(f"Pipeline configuration file not found: {config_file}")

with config_file.open("r", encoding="utf-8") as f:
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

    return (
        f"/Volumes/" f"{paths['catalog']}/" f"{paths['schema']}/" f"{paths['volume']}"
    )


def get_storage_path(folder_name):
    """
    Return fully qualified storage path for a configured folder.
    """
    folder = get_paths()["folders"].get(folder_name)

    if folder is None:
        raise KeyError(f"Unknown storage folder: {folder_name}")

    return f"{get_base_path()}/{folder}"


def get_table_name(table_name):
    """
    Return configured table name.
    """
    tables = get_paths()["tables"]

    if table_name not in tables:
        raise KeyError(f"Unknown table: {table_name}")

    return tables[table_name]


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
