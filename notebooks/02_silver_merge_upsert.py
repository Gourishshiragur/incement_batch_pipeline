# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Snapshot-Comparison Change Detection + Delta MERGE Upsert
# MAGIC This is the core of the incremental design: rather than reprocessing every
# MAGIC row in the daily snapshot file, we compare against the existing Silver
# MAGIC current-state table (keyed on `reading_id`, which maps to the business key
# MAGIC `customer_id + machine_id + event_ts`) and only MERGE the rows that are
# MAGIC actually new or changed. Unchanged rows are skipped entirely.

# COMMAND ----------

dbutils.widgets.text("snapshot_day", "0", "Day index being processed")
snapshot_day = dbutils.widgets.get("snapshot_day")

BRONZE_TABLE = "workspace.default.inc_batch_bronze"
SILVER_TABLE = "workspace.default.inc_batch_silver"
RECON_TABLE = "workspace.default.inc_batch_reconciliation"

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

bronze_df = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("_source_file") == f"snapshot_day{snapshot_day}.csv")
)

# --- Data quality gate ---
before_ct = bronze_df.count()
silver_candidate = (
    bronze_df
    .dropna(subset=["customer_id", "machine_id", "reading_id"])
    .filter((F.col("fuel_level").between(0, 100)) & (F.col("payload_weight_t").between(0, 60)))
    .dropDuplicates(["reading_id"])
)
after_ct = silver_candidate.count()
dq_dropped = before_ct - after_ct
print(f"Data quality gate: {dq_dropped:,} rows dropped ({before_ct:,} -> {after_ct:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Snapshot comparison — first run vs. subsequent runs

# COMMAND ----------

if not spark.catalog.tableExists(SILVER_TABLE):
    # First run: everything is new
    silver_candidate.write.mode("overwrite").saveAsTable(SILVER_TABLE)
    new_ct, changed_ct, unchanged_ct = after_ct, 0, 0
else:
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    prior_df = silver_table.toDF().select(
        "reading_id", "fuel_level", "payload_weight_t", "fault_code"
    )

    tracked = ["fuel_level", "payload_weight_t", "fault_code"]
    joined = silver_candidate.alias("cur").join(
        prior_df.alias("prior"), on="reading_id", how="left"
    )

    changed_expr = F.lit(False)
    for f in tracked:
        changed_expr = changed_expr | (F.col(f"cur.{f}") != F.col(f"prior.{f}"))

    classified = joined.withColumn(
        "_change_type",
        F.when(F.col("prior.fuel_level").isNull(), "NEW")
         .when(changed_expr, "CHANGED")
         .otherwise("UNCHANGED"),
    )

    counts = classified.groupBy("_change_type").count().collect()
    count_map = {r["_change_type"]: r["count"] for r in counts}
    new_ct = count_map.get("NEW", 0)
    changed_ct = count_map.get("CHANGED", 0)
    unchanged_ct = count_map.get("UNCHANGED", 0)

    to_merge = classified.filter(F.col("_change_type").isin("NEW", "CHANGED")).select("cur.*")
    display(classified.groupBy("_change_type").count())
    display(to_merge.limit(10))

    print(f"NEW: {new_ct:,} | CHANGED: {changed_ct:,} | UNCHANGED (skipped): {unchanged_ct:,}")

    # --- Real Delta Lake MERGE INTO upsert, keyed on reading_id ---
    (
        silver_table.alias("t")
        .merge(to_merge.alias("s"), "t.reading_id = s.reading_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

incremental_volume = new_ct + changed_ct
reduction_pct = round((1 - incremental_volume / before_ct) * 100, 2) if before_ct else None

recon_row = spark.createDataFrame([{
    "snapshot_day": int(snapshot_day),
    "total_rows_in_snapshot": before_ct,
    "new_rows": new_ct,
    "changed_rows": changed_ct,
    "unchanged_rows_skipped": unchanged_ct,
    "incremental_volume_processed": incremental_volume,
    "reprocessing_reduction_pct": reduction_pct,
    "dq_dropped_rows": dq_dropped,
    "run_ts": None,
}]).withColumn("run_ts", F.current_timestamp())

recon_row.write.mode("append").saveAsTable(RECON_TABLE)

print(f"Reprocessing reduction vs. full reload: {reduction_pct}%")
dbutils.notebook.exit(str(reduction_pct))
