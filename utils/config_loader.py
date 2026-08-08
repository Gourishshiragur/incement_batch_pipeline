import json
import os
from pathlib import Path

# ---------------------------------------------------------------------
# Environment Detection
# ---------------------------------------------------------------------


def is_databricks() -> bool:
    """Return True when running inside Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# ---------------------------------------------------------------------
# Project Root
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG_DIR = PROJECT_ROOT / "config"

METADATA_FILE = CONFIG_DIR / "pipeline_metadata.json"
CONFIG_FILE = CONFIG_DIR / "pipeline_config.json"


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------

if not METADATA_FILE.exists():
    raise FileNotFoundError(f"Metadata configuration file not found:\n{METADATA_FILE}")

if not CONFIG_FILE.exists():
    raise FileNotFoundError(f"Pipeline configuration file not found:\n{CONFIG_FILE}")


# ---------------------------------------------------------------------
# Load Configuration
# ---------------------------------------------------------------------

with METADATA_FILE.open("r", encoding="utf-8") as f:
    METADATA = json.load(f)

with CONFIG_FILE.open("r", encoding="utf-8") as f:
    CONFIG = json.load(f)


# ---------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------


def get_environment() -> str:
    """Return active execution environment."""

    env = CONFIG.get("environment", "auto")

    if env == "auto":
        return "databricks" if is_databricks() else "local"

    return env


# ---------------------------------------------------------------------
# Metadata Access
# ---------------------------------------------------------------------


def get_paths():
    """
    Return resolved paths for the active environment while preserving
    the original metadata structure.
    """

    env = get_environment()
    cfg = METADATA["paths"][env]

    paths = dict(cfg)  # preserve base_path/folders/tables

    if env == "local":
        base = cfg["base_path"]
    else:
        base = f"/Volumes/" f"{cfg['catalog']}/" f"{cfg['schema']}/" f"{cfg['volume']}"

    # Resolve storage folders
    for name, folder in cfg["folders"].items():
        paths[name] = f"{base}/{folder}"

    # Resolve tables
    for name, table in cfg["tables"].items():
        paths[f"{name}_table"] = table

    # Bronze/Silver/Gold/Control targets
    if env == "databricks":
        paths["bronze"] = cfg["tables"]["bronze"]
        paths["silver"] = cfg["tables"]["silver"]
        paths["gold"] = cfg["tables"]["gold"]
        paths["control"] = cfg["tables"]["control"]
        paths["reconciliation"] = cfg["tables"]["reconciliation"]
        paths["audit_table"] = cfg["tables"]["audit"]
    else:
        paths["bronze"] = f"{base}/{cfg['folders']['bronze']}"
        paths["silver"] = f"{base}/{cfg['folders']['silver']}"
        paths["gold"] = f"{base}/{cfg['folders']['gold']}"
        paths["control"] = f"{base}/{cfg['folders']['control']}"
        paths["reconciliation"] = f"{base}/{cfg['folders']['reconciliation']}"

    return paths


def get_base_path():
    """Return base storage path."""

    paths = get_paths()

    if get_environment() == "local":
        return paths["base_path"]

    return (
        f"/Volumes/" f"{paths['catalog']}/" f"{paths['schema']}/" f"{paths['volume']}"
    )


def get_storage_path(folder_name: str) -> str:
    """Return full storage path."""

    folders = get_paths()["folders"]

    if folder_name not in folders:
        raise KeyError(f"Unknown storage folder: {folder_name}")

    return f"{get_base_path()}/{folders[folder_name]}"


def get_table_name(table_name: str) -> str:
    """Return configured table."""

    tables = get_paths()["tables"]

    if table_name not in tables:
        raise KeyError(f"Unknown table: {table_name}")

    return tables[table_name]


# ---------------------------------------------------------------------
# Configuration Access
# ---------------------------------------------------------------------


def get_config():
    return CONFIG


def get_metadata():
    return METADATA


def get_pipeline_name():
    return METADATA["pipeline"]["name"]


def get_load_type():
    return METADATA["pipeline"]["type"]


def get_file_format():
    return METADATA["source"]["format"]


def get_target_format():
    return METADATA["target"]["format"]
