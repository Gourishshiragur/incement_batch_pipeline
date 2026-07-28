# Benchmarks — Incremental Batch Lakehouse Pipeline

All numbers below were produced by actually running the pipeline logic against
generated data, not estimated. See `validation/run_pipeline_validation.py` for
the harness and `validation/measured_results.json` for raw output.

## Test data
- 25 customers, 970 machines, ~349,200 new telemetry readings generated per day
- 5 daily snapshot extracts simulating realistic re-pulled windows (85% of
  prior rows carried forward, ~4% of those corrected/re-transmitted, 15% aged
  out of the window, plus each day's new readings)
- Peak single-run snapshot size: **1,295,055 rows**

## Measured results (Day 1 → Day 4, Day 0 has no prior state to compare against)

| Day | Full snapshot rows (full-reload equivalent) | Rows actually processed (new + changed) | Unchanged rows skipped | Reprocessing reduction |
|---|---|---|---|---|
| 1 | 646,020 | 361,127 | 284,893 | 44.10% |
| 2 | 898,317 | 371,099 | 527,218 | 58.69% |
| 3 | 1,112,770 | 379,776 | 732,994 | 65.87% |
| 4 | 1,295,055 | 386,965 | 908,090 | 70.12% |

**Average reprocessing reduction: 59.7%**
**Average pipeline runtime per run: ~3 seconds** (pandas validation harness on generated data; run the Databricks notebooks for production-representative timing)

## How to reproduce
1. `python3 data/generate_snapshots.py` — regenerates the 5 daily snapshot CSVs
2. `python3 validation/run_pipeline_validation.py` — runs Bronze→Silver→Gold
   logic locally and writes `validation/measured_results.json`
3. For the authoritative Spark/Delta run: import `notebooks/01_bronze_ingest.py`,
   `02_silver_merge_upsert.py`, `03_gold_kpi_aggregation.py` into Databricks
   Community Edition, upload the CSVs to DBFS, and run each notebook once per
   `snapshot_day` widget value (0 through 4) in sequence.

## Note on the resume figure
The resume currently states "cutting simulated reprocessing volume by ~40-50%".
The measured average here (59.7%) is higher than that claim — the resume
figure is conservative relative to what this run actually demonstrates.
