"""
pipeline_core_spark.py — Production PySpark implementation.

This is the production engine that runs on Databricks / any Spark cluster.
It mirrors every function in pipeline_core.py exactly — same names, same
logic, same behaviour — but uses PySpark DataFrames and real Delta Lake
MERGE instead of pandas.

Why two files:
  pipeline_core.py       → pandas, fast unit tests, no Spark startup, CI
  pipeline_core_spark.py → PySpark, real Delta MERGE, runs on Databricks

Both implement identical business logic. If you change a rule in one,
change it in the other too. The unit tests in tests/ cover the pandas
version; the Databricks notebooks exercise this file on a real cluster.
"""

from __future__ import annotations
import os
from typing import Optional, Tuple
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
# ── Schema ──────────────────────────────────────────────────────────────────

RAW_SCHEMA = StructType([
    StructField("reading_id",       LongType(),   nullable=False),
    StructField("customer_id",      StringType(), nullable=False),
    StructField("machine_id",       StringType(), nullable=False),
    StructField("event_ts",         StringType(), nullable=True),
    StructField("gps_lat",          DoubleType(), nullable=True),
    StructField("gps_lon",          DoubleType(), nullable=True),
    StructField("fuel_level",       DoubleType(), nullable=True),
    StructField("payload_weight_t", DoubleType(), nullable=True),
    StructField("fault_code",       StringType(), nullable=True),
])

# Columns whose values are compared to detect a change vs. prior state.
# Must match TRACKED_FIELDS in pipeline_core.py exactly.
TRACKED_FIELDS = ["fuel_level", "payload_weight_t", "fault_code"]

RECONCILIATION_SCHEMA = StructType([
    StructField("snapshot_day",                LongType(),    nullable=False),
    StructField("total_rows_in_snapshot",      LongType(),    nullable=False),
    StructField("dq_dropped_rows",             LongType(),    nullable=False),
    StructField("new_rows",                    LongType(),    nullable=False),
    StructField("changed_rows",                LongType(),    nullable=False),
    StructField("unchanged_rows_skipped",      LongType(),    nullable=False),
    StructField("incremental_volume_processed",LongType(),    nullable=False),
    StructField("reprocessing_reduction_pct",  DoubleType(),  nullable=True),
    StructField("merged_state_row_count",      LongType(),    nullable=False),
    StructField("gold_rows",                   LongType(),    nullable=False),
    StructField("run_ts",                      TimestampType(),nullable=True),
])


# ── Bronze ───────────────────────────────────────────────────────────────────

def bronze_processing(
    spark: SparkSession,
    raw_path: str,
    bronze_path: str,
    audit_path: str,
    snapshot_day: int,
) -> Tuple[DataFrame, int]:
    """
    Read a raw CSV snapshot, enforce schema, attach ingestion metadata,
    and append to the Bronze Delta table.

    Returns:
        (bronze_df, record_count)
    """
    raw_df = (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(raw_path)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file",  F.lit(f"snapshot_day{snapshot_day}.csv"))
        .withColumn("_snapshot_day", F.lit(snapshot_day))
    )

    record_count = raw_df.count()
    # Append-only Bronze: raw fidelity preserved, nothing dropped here
    if IS_DATABRICKS:
        (
            raw_df.write
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_path)
        )
    else:
        (
            raw_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )
    # Audit log — every ingestion is traceable for replay / reconciliation
    audit_row = spark.createDataFrame(
        [(snapshot_day, f"snapshot_day{snapshot_day}.csv", record_count, None, "SUCCESS")],
        schema=StructType([
            StructField("snapshot_day",  LongType(),    nullable=False),
            StructField("source_file",   StringType(),  nullable=False),
            StructField("record_count",  LongType(),    nullable=False),
            StructField("ingestion_ts",  TimestampType(),nullable=True),
            StructField("status",        StringType(),  nullable=False),
        ])
    ).withColumn("ingestion_ts", F.current_timestamp())

    if IS_DATABRICKS:
        audit_row.write.mode("append").saveAsTable(audit_path)
    else:
        audit_row.write.format("delta").mode("append").save(audit_path)
        
     
            
    return raw_df, record_count


# ── Silver: DQ gate ──────────────────────────────────────────────────────────

def silver_data_quality_gate(
    bronze_df: DataFrame,
) -> Tuple[DataFrame, int]:
    """
    Drop rows that would corrupt the Silver layer:
      - Null business keys (customer_id, machine_id, reading_id)
      - Sensor readings outside valid physical ranges
      - Duplicate reading_id (keep last-ingested)

    Returns:
        (clean_df, rows_dropped)
    """
    before_count = bronze_df.count()

    clean_df = (
        bronze_df
        .dropna(subset=["customer_id", "machine_id", "reading_id"])
        .filter(F.col("fuel_level").between(0, 100))
        .filter(F.col("payload_weight_t").between(0, 60))
    )

    # Dedup on reading_id — keep the row with the latest ingestion timestamp
    # (handles re-transmits where the source resends a corrected reading)
    window = (
        Window.partitionBy("reading_id")
        .orderBy(F.col("_ingestion_ts").desc())
    )
    clean_df = (
        clean_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    rows_dropped = before_count - clean_df.count()
    return clean_df, rows_dropped


# ── Silver: change detection ─────────────────────────────────────────────────

def silver_change_detection(
    silver_candidate: DataFrame,
    silver_path: str,
    spark: SparkSession,
) -> Tuple[DataFrame, int, int, int]:
    """
    Compare incoming rows against the current Silver state table on
    TRACKED_FIELDS (joined on reading_id) and classify each row as:
      NEW       — reading_id not present in Silver
      CHANGED   — reading_id present but ≥1 tracked field value differs
      UNCHANGED — reading_id present and all tracked fields match

    Only NEW + CHANGED rows are returned for merging; UNCHANGED are skipped.

    Returns:
        (to_merge_df, new_count, changed_count, unchanged_count)
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        # First run: entire snapshot is new
        to_merge = silver_candidate.withColumn("_change_type", F.lit("NEW"))
        return to_merge, silver_candidate.count(), 0, 0

    prior_df = (
        spark.read.format("delta").load(silver_path)
        .select(["reading_id"] + TRACKED_FIELDS)
        .alias("prior")
    )

    # Left join: rows with no prior match → NEW; rows with a match → compare
    joined = (
        silver_candidate.alias("cur")
        .join(prior_df, on="reading_id", how="left")
    )

    # Build change expression: any tracked field differs between cur and prior
    change_expr = F.lit(False)
    for field in TRACKED_FIELDS:
        change_expr = change_expr | (
            F.col(f"cur.{field}") != F.col(f"prior.{field}")
        )

    classified = joined.withColumn(
        "_change_type",
        F.when(F.col("prior.fuel_level").isNull(), "NEW")
         .when(change_expr, "CHANGED")
         .otherwise("UNCHANGED"),
    ).select("cur.*", "_change_type")

    counts = (
        classified
        .groupBy("_change_type")
        .count()
        .collect()
    )
    count_map = {r["_change_type"]: r["count"] for r in counts}
    new_ct       = count_map.get("NEW",       0)
    changed_ct   = count_map.get("CHANGED",   0)
    unchanged_ct = count_map.get("UNCHANGED", 0)

    to_merge = (
        classified
        .filter(F.col("_change_type").isin("NEW", "CHANGED"))
        .drop("_change_type")
    )
    return to_merge, new_ct, changed_ct, unchanged_ct


# ── Silver: Delta MERGE upsert ───────────────────────────────────────────────

def delta_merge(
    spark: SparkSession,
    to_merge: DataFrame,
    silver_path: str,
) -> None:
    """
    Real Delta Lake MERGE INTO:
      - Matched on reading_id
      - whenMatchedUpdateAll  → overwrite CHANGED rows
      - whenNotMatchedInsertAll → insert NEW rows
      - UNCHANGED rows were never passed in, so they are untouched automatically

    Idempotent: running the same to_merge twice converges to the same state.
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        # First run: no existing table — write directly
        to_merge.write.format("delta").mode("overwrite").save(silver_path)
        return

    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("target")
        .merge(
            to_merge.alias("source"),
            "target.reading_id = source.reading_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ── Gold ─────────────────────────────────────────────────────────────────────

def gold_processing(
    spark: SparkSession,
    silver_source: str,
    gold_target: str,
) -> Tuple[DataFrame, int]:
    """
    Read the Silver current-state table and aggregate to Gold KPIs
    per customer + machine:
      - avg_fuel_level
      - avg_payload_t
      - fault_events     (count of readings where fault_code != 'NONE')
      - total_readings

    Applies:
      - .cache() on the Silver read (avoids double scan for count + agg)
      - OPTIMIZE + ZORDER on the Gold table for fast downstream lookups
      - Overwrite mode (Gold is always a complete, current view — not additive)

    Returns:
        (gold_df, gold_row_count)
    """
    if IS_DATABRICKS :
         silver_df = spark.table(silver_source).cache()
    else:
         silver_df = spark.read.format("delta").load(silver_source).cache()

    gold_df = (
        silver_df
        .groupBy("customer_id", "machine_id")
        .agg(
            F.avg("fuel_level").alias("avg_fuel_level"),
            F.avg("payload_weight_t").alias("avg_payload_t"),
            F.sum(
                F.when(F.col("fault_code") != "NONE", 1).otherwise(0)
            ).alias("fault_events"),
            F.count("reading_id").alias("total_readings"),
            F.max("_ingestion_ts").alias("last_updated_ts"),
        )
    )

    if IS_DATABRICKS:
         gold_df.write.mode("overwrite").saveAsTable(gold_target)
    else:
        gold_df.write.format("delta").mode("overwrite").save(gold_target)

    # OPTIMIZE + ZORDER so BI tools can do fast point/range lookups
    if IS_DATABRICKS:
        spark.sql(f"""
        OPTIMIZE {gold_target}
        ZORDER BY (customer_id, machine_id)
    """)
    else:
        print("Skipping OPTIMIZE/ZORDER in local Spark.")
    gold_count = gold_df.count()
    silver_df.unpersist()
    return gold_df, gold_count


# ── Reconciliation ────────────────────────────────────────────────────────────

def reconciliation(
    spark: SparkSession,
    recon_path: str,
    snapshot_day: int,
    total_rows: int,
    dq_dropped: int,
    new_ct: int,
    changed_ct: int,
    unchanged_ct: int,
    merged_state_count: int,
    gold_count: int,
) -> float:
    """
    Write a reconciliation log entry to a Delta table and return the
    reprocessing reduction % for this run.

    The reconciliation table is the audit trail that proves the pipeline
    is working correctly: every run's counts are recorded and can be
    queried to verify the reduction numbers in the README.
    """
    incremental_volume = new_ct + changed_ct
    reduction_pct = (
        round((1 - incremental_volume / total_rows) * 100, 2)
        if total_rows > 0 else None
    )

    recon_row = spark.createDataFrame(
        [(
            snapshot_day,
            total_rows,
            dq_dropped,
            new_ct,
            changed_ct,
            unchanged_ct,
            incremental_volume,
            reduction_pct,
            merged_state_count,
            gold_count,
            None,
        )],
        schema=RECONCILIATION_SCHEMA,
    ).withColumn("run_ts", F.current_timestamp())

    recon_row.write.format("delta").mode("append").save(recon_path)

    return reduction_pct
