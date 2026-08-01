# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Snapshot-Comparison Change Detection + Delta MERGE Upsert
# MAGIC This is the core of the incremental design: rather than reprocessing every
# MAGIC row in the daily snapshot file, we compare against the existing Silver
# MAGIC current-state table (keyed on `reading_id`, which maps to the business key
# MAGIC `customer_id + machine_id + event_ts`) and only MERGE the rows that are
# MAGIC actually new or changed. Unchanged rows are skipped entirely.

# COMMAND ----------

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.pipeline.pipeline_core_spark import (
    silver_data_quality_gate,
    silver_change_detection,
    delta_merge,
    
)
from src.framework.quarantine import QuarantineManager

from utils.config_loader import (
    get_paths,
    get_config,
    get_metadata,
    get_environment,
    get_pipeline_name,
)

config = get_config()
metadata = get_metadata()
paths = get_paths()
environment = get_environment()

IS_DATABRICKS = environment == "databricks"
if not IS_DATABRICKS:
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("IncrementalBatchPipeline")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

if IS_DATABRICKS:
    dbutils.widgets.text("snapshot_day", "0", "Day index being processed")
    snapshot_day = dbutils.widgets.get("snapshot_day")
else:
    snapshot_day = sys.argv[1] if len(sys.argv) > 1 else "0"

paths = get_paths()

quarantine_manager = QuarantineManager(quarantine_path=paths["quarantine"])

if IS_DATABRICKS:
    BRONZE_TABLE = paths["bronze"]
    SILVER_TABLE = paths["silver"]
    RECON_TABLE = paths["reconciliation"]
else:
    BRONZE_PATH = paths["bronze"]
    SILVER_PATH = paths["silver"]
    RECON_PATH = paths["reconciliation"]

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    BooleanType,
)
if IS_DATABRICKS:
    bronze_df = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("_source_file") == f"snapshot_day{snapshot_day}")
    )
else:
    bronze_df = (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("_source_file") == f"snapshot_day{snapshot_day}.csv")
    )
silver_candidate, dq_dropped = silver_data_quality_gate(
    bronze_df,
    pipeline_name=get_pipeline_name(),
    quarantine_manager=quarantine_manager,
)
before_ct = bronze_df.count()
after_ct = silver_candidate.count()

conservation_ok = (before_ct == after_ct + dq_dropped)

if not conservation_ok:
    print(
        f"ROW CONSERVATION CHECK FAILED at silver DQ gate: "
        f"before={before_ct}, after={after_ct}, dq_dropped={dq_dropped}, "
        f"missing={before_ct - (after_ct + dq_dropped)}"
    )
else:
    print(
        f"Row conservation OK at silver DQ gate: {before_ct:,} = "
        f"{after_ct:,} clean + {dq_dropped:,} quarantined"
    )

print(
    f"Data quality gate: {dq_dropped:,} rows dropped "
    f"({before_ct:,} -> {after_ct:,})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Snapshot comparison — first run vs. subsequent runs

# COMMAND ----------



silver_target = SILVER_TABLE if IS_DATABRICKS else SILVER_PATH

to_merge, new_ct, changed_ct, unchanged_ct = silver_change_detection(
    silver_candidate,
    silver_target,
    spark,
)

print(
    f"NEW: {new_ct:,} | "
    f"CHANGED: {changed_ct:,} | "
    f"UNCHANGED: {unchanged_ct:,}"
)

print("=" * 60)
print("SOURCE COLUMNS")
print(to_merge.columns)

if DeltaTable.isDeltaTable(spark, silver_target):
    print("=" * 60)
    print("TARGET COLUMNS")
    spark.read.format("delta").load(silver_target).printSchema()

print("=" * 60)

delta_merge(
    spark,
    to_merge,
    silver_target,
)
incremental_volume = new_ct + changed_ct
reduction_pct = round((1 - incremental_volume / before_ct) * 100, 2) if before_ct else None

schema = StructType([
    StructField("snapshot_day", LongType(), False),
    StructField("total_rows_in_snapshot", LongType(), False),
    StructField("new_rows", LongType(), False),
    StructField("changed_rows", LongType(), False),
    StructField("unchanged_rows_skipped", LongType(), False),
    StructField("incremental_volume_processed", LongType(), False),
    StructField("reprocessing_reduction_pct", DoubleType(), True),
    StructField("dq_dropped_rows", LongType(), False),
    StructField("row_conservation_passed", BooleanType(), False),
])

recon_row = spark.createDataFrame(
    [Row(
        snapshot_day=int(snapshot_day),
        total_rows_in_snapshot=int(before_ct),
        new_rows=int(new_ct),
        changed_rows=int(changed_ct),
        unchanged_rows_skipped=int(unchanged_ct),
        incremental_volume_processed=int(incremental_volume),
        reprocessing_reduction_pct=float(reduction_pct) if reduction_pct is not None else None,
        dq_dropped_rows=int(dq_dropped),
        row_conservation_passed=bool(conservation_ok),
    )],
    schema=schema,
).withColumn(
    "run_ts",
    F.current_timestamp()
)
if IS_DATABRICKS:
    recon_row.write.mode("append").saveAsTable(RECON_TABLE)
else:
    (
        recon_row.write
        .mode("append")
        .format("delta")
        .save(RECON_PATH)
    )

print(f"Reprocessing reduction vs. full reload: {reduction_pct}%")
if IS_DATABRICKS:
    dbutils.notebook.exit(str(reduction_pct))
else:
    print(f"Reprocessing reduction: {reduction_pct}%")
