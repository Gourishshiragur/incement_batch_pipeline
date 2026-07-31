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

ROOT = Path(__file__).resolve().parent

CONFIG_FILE = ROOT / "config" / "pipeline_config.json"

REPORTS_DIR = ROOT / "reports"


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

import shutil

RAW_DIR = ROOT / "data" / "raw"


def snapshots_exist() -> bool:
    """
    Check whether the required snapshot files already exist.
    """
    expected = [RAW_DIR / f"snapshot_day{i}.csv" for i in range(5)]
    return all(path.exists() for path in expected)


def clean_snapshots():
    """
    Remove previously generated snapshots.
    """
    if RAW_DIR.exists():
        shutil.rmtree(RAW_DIR)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

def run_script(script):

    script_path = ROOT / script

    print("\n" + "=" * 80)
    print(f"Running : {script_path.name}")
    print("=" * 80)

    start = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=ROOT
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
            "reconciliation",
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
            "reconciliation",
            "validation"
        ]
    )

    args = parser.parse_args()

    config = load_config()

    stages = config["stages"]

    execution = []

    if args.only:

        if args.only == "generate":
            execution.append(("generate", stages["generate"]))
        else:
            execution.append((args.only, stages[args.only]))

    else:

        order = [
            "bronze",
            "silver",
            "gold",
            "reconciliation",
            "validation"
        ]

    if args.generate:
            print("\n--generate specified. Regenerating snapshots...\n")

            clean_snapshots()
            execution.append(("generate", stages["generate"]))

    elif not snapshots_exist():
        print("\nNo snapshots found.")
        print("Generating snapshots automatically...\n")

        execution.append(("generate", stages["generate"]))

    else:
        print("\nExisting snapshots detected.")
        print("Skipping snapshot generation.\n")

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