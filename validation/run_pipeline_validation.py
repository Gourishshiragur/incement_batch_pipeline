"""
Local validation harness for the Incremental Batch Lakehouse pipeline.

Imports pipeline_core.py directly -- the SAME functions covered by
tests/test_pipeline_logic.py -- so the numbers below come from the tested
code path, not a parallel copy of it.
"""

import sys
import os
import json
import time
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pipeline_core import (
    silver_data_quality_gate,
    silver_change_detection,
    merge_upsert,
)

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = PROJECT_ROOT / "validation"
N_DAYS = 5


def bronze_ingest(day_idx):
    df = pd.read_csv(DATA_DIR / f"snapshot_day{day_idx}.csv")
    df["_ingestion_ts"] = pd.Timestamp.now(tz="UTC").isoformat()
    df["_source_file"] = f"snapshot_day{day_idx}.csv"
    return df


def gold_aggregate(silver_current_state):
    return (
        silver_current_state.groupby(["customer_id", "machine_id"])
        .agg(
            avg_fuel_level=("fuel_level", "mean"),
            avg_payload_t=("payload_weight_t", "mean"),
            fault_events=("fault_code", lambda s: (s != "NONE").sum()),
            total_readings=("reading_id", "count"),
        )
        .reset_index()
    )


def main():
    results = []
    prior_state = None
    timings = []

    for d in range(N_DAYS):
        t0 = time.time()
        bronze_df = bronze_ingest(d)
        total_rows = len(bronze_df)

        clean_df, dq_dropped = silver_data_quality_gate(bronze_df)
        classified = silver_change_detection(clean_df, prior_state)

        counts = classified["_change_type"].value_counts().to_dict()
        new_ct = counts.get("NEW", 0)
        changed_ct = counts.get("CHANGED", 0)
        unchanged_ct = counts.get("UNCHANGED", 0)
        incremental_volume = new_ct + changed_ct

        if prior_state is None:
            merged_state = classified.drop(columns=["_change_type"])
        else:
            to_upsert = classified[classified["_change_type"].isin(["NEW", "CHANGED"])]
            merged_state = merge_upsert(prior_state, to_upsert)

        gold_df = gold_aggregate(merged_state)
        elapsed = time.time() - t0

        reduction_pct = (
            round((1 - incremental_volume / total_rows) * 100, 2)
            if prior_state is not None
            else None
        )

        day_result = {
            "day": d,
            "total_rows_in_snapshot_file": int(total_rows),
            "dq_dropped_rows": int(dq_dropped),
            "new_rows": int(new_ct),
            "changed_rows": int(changed_ct),
            "unchanged_rows_skipped": int(unchanged_ct),
            "incremental_volume_processed": int(incremental_volume),
            "reprocessing_reduction_pct_vs_full_reload": reduction_pct,
            "merged_state_row_count": int(len(merged_state)),
            "gold_rows": int(len(gold_df)),
            "elapsed_seconds": round(elapsed, 2),
        }
        results.append(day_result)
        print(json.dumps(day_result, indent=2))

        prior_state = merged_state
        timings.append(elapsed)

    comparable = [
        r for r in results if r["reprocessing_reduction_pct_vs_full_reload"] is not None
    ]
    summary = {
        "days_measured": len(comparable),
        "avg_reprocessing_reduction_pct": round(
            sum(r["reprocessing_reduction_pct_vs_full_reload"] for r in comparable)
            / len(comparable),
            2,
        ),
        "min_reprocessing_reduction_pct": min(
            r["reprocessing_reduction_pct_vs_full_reload"] for r in comparable
        ),
        "max_reprocessing_reduction_pct": max(
            r["reprocessing_reduction_pct_vs_full_reload"] for r in comparable
        ),
        "peak_daily_record_volume": max(
            r["total_rows_in_snapshot_file"] for r in results
        ),
        "avg_pipeline_runtime_seconds": round(sum(timings) / len(timings), 2),
    }

    with open(OUT_DIR / "measured_results.json", "w") as f:
        json.dump({"daily_results": results, "summary": summary}, f, indent=2)

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
