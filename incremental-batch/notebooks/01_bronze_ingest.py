# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Telemetry Ingestion
# MAGIC Reads the daily snapshot CSV extract, appends ingestion metadata, and writes
# MAGIC to a Delta Bronze table with schema enforcement + audit logging.

# COMMAND ----------

dbutils.widgets.text("snapshot_day", "0", "Day index of snapshot file to ingest")
snapshot_day = dbutils.widgets.get("snapshot_day")

RAW_PATH = f"/FileStore/mining_telemetry/raw/snapshot_day{snapshot_day}.csv"
BRONZE_PATH = "/FileStore/mining_telemetry/delta/bronze_telemetry"
AUDIT_PATH = "/FileStore/mining_telemetry/delta/audit_ingestion_log"

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

record_count = raw_df.count()
print(f"Bronze ingest: {record_count:,} rows read from snapshot_day{snapshot_day}.csv")

# COMMAND ----------

# Append to Bronze Delta table (append-only, raw fidelity preserved)
(
    raw_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
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

audit_row.write.format("delta").mode("append").save(AUDIT_PATH)

dbutils.notebook.exit(str(record_count))
