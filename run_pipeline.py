"""
Enterprise Pipeline Orchestrator

Flow

--generate
Generate Snapshots
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
Reconciliation
        ↓
Validation

Without --generate

Bronze
 ↓
Silver
 ↓
Gold
 ↓
Reconciliation
 ↓
Validation
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
import shutil
from utils.config_loader import (
    get_config,
    get_paths,
    get_environment,
)

ROOT = Path(__file__).resolve().parent


def snapshots_exist(raw_dir: Path) -> bool:
    """
    Check whether the required snapshot files already exist.
    """
    expected = [raw_dir / f"snapshot_day{i}.csv" for i in range(5)]
    return all(path.exists() for path in expected)


def clean_snapshots(raw_dir: Path):
    """
    Remove previously generated snapshots.
    """
    if raw_dir.exists():
        shutil.rmtree(raw_dir)

    raw_dir.mkdir(parents=True, exist_ok=True)

def run_script(
    script: str,
    *args: str,
) -> float:

    script_path = ROOT / script

    print("\n" + "=" * 80)
    print(f"Running : {script_path.name}")
    print("=" * 80)

    start = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=ROOT,
        text=True,
)

    elapsed = round(time.time() - start, 2)

    if result.returncode != 0:
        raise RuntimeError(
            f"{script_path.name} failed "
            f"(Exit Code {result.returncode})"
        )

    return elapsed


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate fresh snapshots before pipeline"
    )

    parser.add_argument(
        "--from",
        dest="start_from",
        choices=[
            "bronze",
            "silver",
            "gold",
            "validation"
        ],
        default="bronze"
    )

    parser.add_argument(
        "--only",
        choices=[
            "generate",
            "bronze",
            "silver",
            "gold",
            "validation"
        ]
    )

    args = parser.parse_args()

    config = get_config()
    paths = get_paths()
    environment = get_environment()
    
    is_databricks = environment == "databricks"

    REPORTS_DIR = ROOT / config["reports_directory"]
    
    RAW_DIR = ROOT / paths["raw"]

    stages = config["stages"]

    execution = []

    if args.only:

        execution.append((args.only, stages[args.only]))

    else:

        order = [
            "bronze",
            "silver",
            "gold",
            "validation"
        ]

        generate_required = False

        if args.generate:
            if is_databricks:
                print("\n--generate is not supported in Databricks.")
                print("Please populate the landing path before running the pipeline.\n")
            else:
                print("\n--generate specified. Regenerating snapshots...\n")
                clean_snapshots(RAW_DIR)
                generate_required = True
               
        elif is_databricks:
            print("\nRunning in Databricks.")
            print("Skipping sample snapshot generation.")
            print("Expecting input data in the configured landing path.\n")

        elif not snapshots_exist(RAW_DIR):
            print("\nNo snapshots found.")
            print("Generating snapshots automatically...\n")
            generate_required = True

        else:
            print("\nExisting snapshots detected.")
            print("Skipping snapshot generation.\n")

        if generate_required:
            execution.append(("generate", stages["generate"]))

        start_index = order.index(args.start_from)

        for stage in order[start_index:]:
            execution.append((stage, stages[stage]))

    REPORTS_DIR.mkdir(exist_ok=True)

    pipeline_summary = {
        "status": "PASS",
        "execution_mode": "incremental",
        "generate_snapshots": args.generate,
        "stages": {}
    }

    total_start = time.time()

    try:

        for stage_name, script in execution:

            runtime = run_script(script)

            pipeline_summary["stages"][stage_name] = {
                "status": "PASS",
                "runtime_seconds": runtime
            }

        pipeline_summary["total_runtime_seconds"] = round(
            time.time() - total_start,
            2
        )

    except Exception as ex:

        pipeline_summary["status"] = "FAILED"

        pipeline_summary["error"] = str(ex)

    with open(
        REPORTS_DIR / "pipeline_execution_report.json",
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            pipeline_summary,
            f,
            indent=4
        )

    print("\nPipeline Summary\n")
    print(json.dumps(pipeline_summary, indent=4))


if __name__ == "__main__":
    main()