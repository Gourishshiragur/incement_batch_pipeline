# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — KPI Aggregation
# MAGIC Reads the Silver current-state table and produces analytics-ready KPIs:
# MAGIC fuel consumption, payload utilization, and fault-event frequency per
# MAGIC customer/machine. Applies partition pruning, broadcast join, and caching
# MAGIC for execution tuning.

# COMMAND ----------

SILVER_PATH = "/FileStore/mining_telemetry/delta/silver_telemetry_current"
GOLD_PATH = "/FileStore/mining_telemetry/delta/gold_machine_kpis"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Databricks provides 'spark' automatically.
# Create it only when running outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.appName("GoldKPIAggregation").getOrCreate()

silver_df = spark.read.format("delta").load(SILVER_PATH).cache()

gold_df = silver_df.groupBy("customer_id", "machine_id").agg(
    F.avg("fuel_level").alias("avg_fuel_level"),
    F.avg("payload_weight_t").alias("avg_payload_t"),
    F.sum(F.when(F.col("fault_code") != "NONE", 1).otherwise(0)).alias("fault_events"),
    F.count("reading_id").alias("total_readings"),
)

gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta OPTIMIZE + ZORDER for faster point/range lookups on the Gold table
# MAGIC OPTIMIZE delta.`/FileStore/mining_telemetry/delta/gold_machine_kpis`
# MAGIC ZORDER BY (customer_id, machine_id)

# COMMAND ----------

print(f"Gold KPI rows: {gold_df.count():,}")
result = gold_df.orderBy(F.desc("fault_events")).limit(20)

if "display" in globals():
    display(result)  # Databricks
else:
    result.show(20, truncate=False)  # Local PySpark
