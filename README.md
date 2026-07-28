# Incremental Batch Lakehouse Pipeline — Mining Telemetry

Bronze → Silver → Gold medallion pipeline for **daily machine-telemetry ingestion** from
mining/heavy-equipment fleets — the kind of data a logistics or mining operations team uses
to monitor fuel consumption, payload cycles, and fault patterns across hundreds of machines
and dozens of customer accounts.

Built on **Azure Databricks + Delta Lake + ADF**, architected as an incremental batch job:
instead of reloading the full daily snapshot every run, it detects what actually changed,
processes only that, and upserts it into the state table. This is the pattern that made the
difference between a pipeline that ran in hours and one that ran in minutes at HCL — replicated
here as a standalone, testable, reproducible project.

---

## Business context — why this pipeline exists

A heavy-equipment fleet telemetry system emits daily snapshot extracts: the full current state
of every machine's sensor readings for every customer account. These extracts **re-transmit a
large fraction of unchanged prior rows** — because the source system doesn't track deltas, it
just dumps the whole window every day. By day 4 of a typical rolling window, 70% of the snapshot
is unchanged data from prior days.

**Without this pipeline:** a naive full-reload approach processes every row in every snapshot,
including the 70% that didn't change. At 30-50 GB/day across 10-50 customer accounts, that
adds up to hours of unnecessary compute.

**With this pipeline:** Silver-layer change detection classifies each row as NEW, CHANGED, or
UNCHANGED. Only NEW and CHANGED rows are upserted into the target state table. UNCHANGED rows
are skipped entirely. The Gold layer reads from the already-merged state, so downstream
analytics always sees a complete, current view — not just the delta.

---

## Measured results (reproducible — run the validation harness yourself)

Data: 25 customers, 970 machines, ~349,200 new telemetry readings generated per day.
5 daily snapshot CSVs simulating realistic re-pull windows with corrections and new readings.

| Day | Full snapshot rows | Rows actually processed | Rows skipped | Reprocessing reduction |
|---|---|---|---|---|
| 1 | 646,020 | 361,127 | 284,893 | **44.1%** |
| 2 | 898,317 | 371,099 | 527,218 | **58.7%** |
| 3 | 1,112,770 | 379,776 | 732,994 | **65.9%** |
| 4 | 1,295,055 | 386,965 | 908,090 | **70.1%** |

**Average reprocessing reduction: 59.7% across days 1-4**
**Peak snapshot size handled: 1,295,055 rows**

The reduction grows day-over-day because the state table accumulates — an unchanged reading
present since day 1 is correctly skipped on days 2, 3, and 4. This is the expected behaviour
for a mature incremental pipeline, not an artifact of the data.

To reproduce:
```bash
python3 data/generate_snapshots.py          # regenerate the 5 daily CSVs
python3 validation/run_pipeline_validation.py  # runs the full pipeline and writes measured_results.json
```

---

## Pipeline architecture

```
Daily snapshot extract (CSV, per customer account)
        │
        ▼
[Bronze layer — notebook 01]
  Raw append to Delta Bronze table
  Schema enforcement, ingestion metadata (_ingestion_ts, _source_file)
  Audit log entry written
        │
        ▼
[Silver layer — notebook 02]
  Data quality gate:
    - Drop rows with null business keys (customer_id, machine_id, reading_id)
    - Drop sensor readings outside valid ranges (fuel 0-100%, payload 0-60t)
    - Deduplicate on reading_id (keep last)
  Change detection vs. prior Silver state:
    - NEW  → insert
    - CHANGED (fuel_level / payload_weight_t / fault_code differ) → upsert
    - UNCHANGED → skip entirely
  Delta MERGE upsert on reading_id
        │
        ▼
[Gold layer — notebook 03]
  KPI aggregation per customer per day:
    - avg fuel level, avg payload, fault rate, active machine count
  Written as a Delta Gold table partitioned by customer_id + snapshot_day
  Consumed by BI/analytics teams for fleet health dashboards
```

---

## Running on Databricks Community Edition (free)

1. Upload `data/snapshot_day0.csv` through `data/snapshot_day4.csv` to DBFS at
   `/FileStore/mining_telemetry/raw/`
2. Import `notebooks/01_bronze_ingest.py`, `02_silver_merge_upsert.py`,
   `03_gold_kpi_aggregation.py` into a Databricks workspace
3. Run each notebook once per day index (widget `snapshot_day` = 0, 1, 2, 3, 4) in sequence
4. At day 4, inspect the Gold table — it should contain one KPI row per customer per day

Databricks Community Edition is free at [community.cloud.databricks.com](https://community.cloud.databricks.com).
No credit card required. The notebooks use standard PySpark + Delta Lake, which are pre-installed
in every Databricks runtime.

---

## Local testing (no Spark needed)

`pipeline_core.py` extracts the Silver change-detection and merge-upsert logic into plain Python
(pandas) so it can be tested without a Spark cluster:

```bash
pip install pandas numpy
python3 tests/run_tests_no_pytest.py   # runs all unit tests without pytest
python3 validation/run_pipeline_validation.py  # end-to-end run, produces measured_results.json
```

Tests cover: change classification (NEW/CHANGED/UNCHANGED), data quality gate drops,
merge-upsert idempotency, and Gold KPI output shape.

---

## Design decisions worth discussing in an interview

**Why comparison-based change detection rather than CDC?**
The source system emits full snapshot extracts, not a CDC stream — it has no change log to tap
into. Comparison-based detection on `reading_id` + tracked fields is the right tool when you
can't modify the source system.

**Why Delta MERGE rather than overwrite?**
Full overwrite would lose the history of corrections — if a prior day's reading was wrong and
re-transmitted with a corrected value, overwrite destroys the original. MERGE upserts the
correction while preserving audit history.

**Why does the reduction grow day-over-day?**
Day 1 still has to process all of day 0's readings as the baseline state gets established.
By day 4, the majority of the snapshot is stable readings that have been in the state table
since earlier days — and the change detector correctly skips them. This is not an optimistic
simulation; it's a natural consequence of how rolling-window snapshot systems work.

---

## Stack

Python · PySpark · Delta Lake · Azure Databricks · Azure Data Factory (ADF) · ADLS Gen2

---

*The measured numbers in this README come directly from `validation/measured_results.json`,
produced by running `validation/run_pipeline_validation.py` on the generated data in this repo.
They are reproducible by anyone who clones this repo and runs the two commands above.*
