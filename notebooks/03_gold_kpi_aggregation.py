# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: KPI Aggregation
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Reads the Silver current-state table and aggregates to business-ready
# MAGIC KPIs per customer + machine:
# MAGIC   - avg_fuel_level    — fleet fuel health
# MAGIC   - avg_payload_t     — utilization proxy
# MAGIC   - fault_events      — maintenance signal
# MAGIC   - total_readings    — data completeness indicator
# MAGIC
# MAGIC Applies OPTIMIZE + ZORDER BY (customer_id, machine_id) on the Gold table
# MAGIC so BI tools can do fast point-lookup queries per customer or machine.
# MAGIC
# MAGIC **Business output:**
# MAGIC Gold is what the analytics team queries. A fleet manager sees one row per
# MAGIC machine with current KPIs, updated daily after this notebook runs.

# COMMAND ----------
from pathlib import Path
import os
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.pipeline_core_spark import gold_processing
from utils.config_loader import get_paths

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if not IS_DATABRICKS:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("IncrementalBatchPipeline")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.hadoop.native.lib", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()


if IS_DATABRICKS:
    SILVER_SOURCE = "workspace.default.inc_batch_silver"
    GOLD_TARGET = "workspace.default.inc_batch_gold"
else:
    paths = get_paths()
    SILVER_SOURCE = paths["silver"]
    GOLD_TARGET = paths["gold"]
# COMMAND ----------

print("=" * 80)
print("GOLD LAYER - KPI AGGREGATION")
print("=" * 80)
print(f"Source : {SILVER_SOURCE}")
print(f"Target : {GOLD_TARGET}")

start_time = time.time()

# COMMAND ----------

try:
    gold_df, gold_count = gold_processing(
        spark=spark,
        silver_source=SILVER_SOURCE,
        gold_target=GOLD_TARGET,
    )

    if gold_count == 0:
        raise RuntimeError("Gold aggregation produced no rows.")

    print(f"Gold KPI rows : {gold_count:,}")

    gold_df.printSchema()

    if IS_DATABRICKS:
        display(
            gold_df.orderBy(
                "fault_events",
                ascending=False,
            ).limit(20)
        )
    else:
        gold_df.orderBy(
            "fault_events",
            ascending=False,
        ).show(20, truncate=False)

except Exception as e:
    print(f"Gold processing failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample business query: top 10 machines by fault events

# COMMAND ----------

gold_df.select(
    "customer_id",
    "machine_id",
    "fault_events",
    "avg_fuel_level",
    "avg_payload_t",
).orderBy("fault_events", ascending=False).show(10, truncate=False)

# COMMAND ----------
elapsed = time.time() - start_time

print("=" * 80)
print(f"Execution Time : {elapsed:.2f} seconds")
print("=" * 80)

if IS_DATABRICKS:
    dbutils.notebook.exit(str(gold_count))
else:
    print(f"Gold processing completed: {gold_count:,} rows")
