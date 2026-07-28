# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Telemetry Ingestion
# MAGIC Reads the daily snapshot CSV extract, appends ingestion metadata, and writes
# MAGIC to a Delta Bronze table with schema enforcement + audit logging.

# COMMAND ----------

dbutils.widgets.text("snapshot_day", "0", "Day index of snapshot file to ingest")
snapshot_day = dbutils.widgets.get("snapshot_day")

RAW_PATH = f"/Volumes/workspace/default/incremental_batch/raw/snapshot_day{snapshot_day}.csv"

BRONZE_TABLE = "workspace.default.inc_batch_bronze"
AUDIT_TABLE = "workspace.default.inc_batch_audit"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

schema = StructType([
    StructField("reading_id", LongType(), False),
    StructField("customer_id", StringType(), False),
    StructField("machine_id", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("gps_lat", DoubleType(), True),
    StructField("gps_lon", DoubleType(), True),
    StructField("fuel_level", DoubleType(), True),
    StructField("payload_weight_t", DoubleType(), True),
    StructField("fault_code", StringType(), True),
])

raw_df = (
    spark.read.option("header", True).schema(schema).csv(RAW_PATH)
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit(f"snapshot_day{snapshot_day}.csv"))
)
display(raw_df.limit(10))
raw_df.printSchema()

record_count = raw_df.count()
print(f"Bronze ingest: {record_count:,} rows read from snapshot_day{snapshot_day}.csv")

# COMMAND ----------

# Append to Bronze Delta table (append-only, raw fidelity preserved)
(
    raw_df.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

# COMMAND ----------

# Audit log entry — every ingestion run is tracked for reconciliation & replay
audit_row = spark.createDataFrame([{
    "snapshot_day": int(snapshot_day),
    "source_file": f"snapshot_day{snapshot_day}.csv",
    "record_count": record_count,
    "ingestion_ts": None,
    "status": "SUCCESS",
}]).withColumn("ingestion_ts", F.current_timestamp())

audit_row.write.mode("append").saveAsTable(AUDIT_TABLE)

dbutils.notebook.exit(str(record_count))
