# Getting the data into Databricks

**Be upfront about this if anyone asks: this is synthetic/generated data, not a
real customer dataset.** You don't have access to real mining telemetry data
outside HCL, so a generated dataset with realistic structure and volume is the
honest, standard way to build an independent portfolio project. Say that
directly if asked — "I generated realistic synthetic telemetry data to build
and load-test this independently" is a completely normal and credible answer.
Don't imply it's real production data; that's the one thing that would turn
this from "reasonable portfolio project" into a lie.

## Option A — Databricks Community Edition UI (simplest, no CLI needed)
1. Log into your Databricks Community Edition workspace
2. Left sidebar → **Data** → **Add data** → **Upload files to DBFS**
3. Upload the 5 CSVs from `data/snapshot_day0.csv` … `snapshot_day4.csv`
4. Note the target path shown (typically `/FileStore/tables/...`) — update the
   `RAW_PATH` variable in `notebooks/01_bronze_ingest.py` to match if it
   differs from `/FileStore/mining_telemetry/raw/`
5. Also upload `data/machine_roster.csv` if you want to join in machine
   metadata later

## Option B — Databricks CLI (if you install it locally, where you have internet)
```bash
pip install databricks-cli
databricks configure --token   # paste your workspace URL + personal access token
databricks fs mkdirs dbfs:/FileStore/mining_telemetry/raw
databricks fs cp data/snapshot_day0.csv dbfs:/FileStore/mining_telemetry/raw/
databricks fs cp data/snapshot_day1.csv dbfs:/FileStore/mining_telemetry/raw/
databricks fs cp data/snapshot_day2.csv dbfs:/FileStore/mining_telemetry/raw/
databricks fs cp data/snapshot_day3.csv dbfs:/FileStore/mining_telemetry/raw/
databricks fs cp data/snapshot_day4.csv dbfs:/FileStore/mining_telemetry/raw/
```

## Running the notebooks after upload
Run in this exact order, once per day, changing the `snapshot_day` widget
value each time (0 → 1 → 2 → 3 → 4):
1. `01_bronze_ingest.py`
2. `02_silver_merge_upsert.py`
3. `03_gold_kpi_aggregation.py` (only needs to run after the last day, or after each day if you want daily KPI snapshots too)

After all 5 days, query `reconciliation_log` (written by notebook 02) to see
Databricks' own measured reduction numbers — compare them to
`validation/measured_results.json`. They should be close; small differences
are expected because Spark's execution and pandas' aren't identical, and
that's a normal, honest thing to be able to explain if asked.

## What "production" would look like instead of DBFS
In a real deployment, `RAW_PATH` / `BRONZE_PATH` / etc. would point to
mounted ADLS Gen2 paths (`/mnt/adls/raw/...`) via a service principal, not
DBFS FileStore. DBFS is fine and normal for a personal Community Edition
project — just don't describe it as ADLS-backed if asked directly, since it
isn't.
