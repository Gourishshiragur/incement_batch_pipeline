# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Telemetry Ingestion
# MAGIC Reads the daily snapshot CSV extract, appends ingestion metadata, and writes
# MAGIC to a Delta Bronze table with schema enforcement + audit logging.

# COMMAND ----------

import os
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.pipeline_core_spark import bronze_processing
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
    dbutils.widgets.text("snapshot_day", "0", "Day index of snapshot file to ingest")
    snapshot_day = dbutils.widgets.get("snapshot_day")
else:
    snapshot_day = sys.argv[1] if len(sys.argv) > 1 else "0"

paths = get_paths()


if IS_DATABRICKS:
    RAW_PATH = f"{paths['raw']}/snapshot_day{snapshot_day}"
else:
    RAW_PATH = f"{paths['raw']}/snapshot_day{snapshot_day}.csv"

if IS_DATABRICKS:
    BRONZE_TABLE = paths["bronze"]
    AUDIT_TABLE = paths["audit_table"]
else:
    BRONZE_PATH = paths["bronze"]
    AUDIT_PATH = paths["audit"]

# COMMAND ----------

bronze_target = BRONZE_TABLE if IS_DATABRICKS else BRONZE_PATH
audit_target = AUDIT_TABLE if IS_DATABRICKS else AUDIT_PATH

try:
    bronze_df, record_count = bronze_processing(
        spark=spark,
        raw_path=RAW_PATH,
        bronze_path=bronze_target,
        audit_path=audit_target,
        snapshot_day=int(snapshot_day),
    )
except Exception as exc:
    print(f"ERROR: Bronze ingestion failed.\n{exc}")
    raise

if IS_DATABRICKS:
    display(bronze_df.limit(10))
else:
    bronze_df.show(10, truncate=False)

bronze_df.printSchema()

print("=" * 60)
print("Bronze Preview")
print(f"Rows Read : {record_count:,}")
print("=" * 60)





# COMMAND ----------



if IS_DATABRICKS:
    dbutils.notebook.exit(str(record_count))
else:
    print("=" * 60)
    print("Bronze Ingestion Completed Successfully")
    print(f"Rows Ingested : {record_count:,}")
    print("=" * 60)