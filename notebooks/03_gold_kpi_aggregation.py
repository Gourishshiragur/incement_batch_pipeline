# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — KPI Aggregation
# MAGIC Reads the Silver current-state table and produces analytics-ready KPIs:
# MAGIC fuel consumption, payload utilization, and fault-event frequency per
# MAGIC customer/machine. Applies partition pruning, broadcast join, and caching
# MAGIC for execution tuning.

# COMMAND ----------

SILVER_TABLE = "workspace.default.inc_batch_silver"
GOLD_TABLE = "workspace.default.inc_batch_gold"

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table(SILVER_TABLE).cache()

gold_df = (
    silver_df.groupBy("customer_id", "machine_id")
    .agg(
        F.avg("fuel_level").alias("avg_fuel_level"),
        F.avg("payload_weight_t").alias("avg_payload_t"),
        F.sum(F.when(F.col("fault_code") != "NONE", 1).otherwise(0)).alias("fault_events"),
        F.count("reading_id").alias("total_readings"),
    )
)

gold_df.write.mode("overwrite").saveAsTable(GOLD_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta OPTIMIZE + ZORDER for faster point/range lookups on the Gold table
# MAGIC OPTIMIZE workspace.default.inc_batch_gold
# MAGIC ZORDER BY (customer_id, machine_id)

# COMMAND ----------
gold_df.printSchema()
display(gold_df.limit(10))
print(f"Gold KPI rows: {gold_df.count():,}")

display(gold_df.orderBy(F.desc("fault_events")).limit(20))
