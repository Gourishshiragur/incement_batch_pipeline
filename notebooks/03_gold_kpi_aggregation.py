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
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.pipeline_core_spark import gold_processing
from src.framework.context import FrameworkContext
from utils.config_loader import (
    get_paths,
    get_environment,
    get_config,
    get_metadata,
    get_pipeline_name,
)
from src.framework.constants import (
    STATUS_SKIPPED,
    STATUS_FAILED,
    STATUS_STARTED,
)

pipeline_name = get_pipeline_name()

config = get_config()
metadata = get_metadata()
paths = get_paths()
environment = get_environment()

IS_DATABRICKS = environment == "databricks"

if not IS_DATABRICKS:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(pipeline_name)
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
    dbutils.widgets.text("snapshot_day", "0", "Day index being processed")
    snapshot_day = dbutils.widgets.get("snapshot_day")
else:
    snapshot_day = sys.argv[1] if len(sys.argv) > 1 else "0"
context = FrameworkContext(
    spark=spark,
    pipeline_name=pipeline_name,
    pipeline_type=metadata["pipeline"]["type"],
    control_path=paths["control"],
    quarantine_path=paths["quarantine"],
    schema_history_path=paths["schema_history"],
    schema_changes_path=paths["schema_changes"],
)

context.logger.pipeline_started()
context.audit.start_run()
context.control.start_run(
    pipeline_name=pipeline_name,
    pipeline_type=metadata["pipeline"]["type"],
    run_id=context.audit.get_run_id(),
)

previous_status = context.control.last_stage_status(
    pipeline_name=pipeline_name,
    source_file=f"{paths['landing']}/snapshot_day{snapshot_day}.csv",
)

if previous_status == STATUS_SKIPPED:

    context.logger.info("Silver skipped. Skipping Gold.")

    context.control.skip_run(
        pipeline_name=pipeline_name,
        run_id=context.audit.get_run_id(),
    )

    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

elif previous_status == STATUS_FAILED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=context.audit.get_run_id(),
        error_message="Silver stage failed.",
    )

    raise RuntimeError("Silver stage failed.")

elif previous_status == STATUS_STARTED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=context.audit.get_run_id(),
        error_message="Silver stage did not complete.",
    )

    raise RuntimeError("Silver stage did not complete.")
context.logger.debug(f"Resolved paths: {paths}")

SILVER_SOURCE = paths["silver"]
GOLD_TARGET = paths["gold"]

if IS_DATABRICKS:
    AUDIT_TABLE = paths["audit_table"]
else:
    AUDIT_PATH = paths["audit"]

audit_target = AUDIT_TABLE if IS_DATABRICKS else AUDIT_PATH
# COMMAND ----------

context.logger.info("=" * 80)
context.logger.info("GOLD LAYER - KPI AGGREGATION")
context.logger.info("=" * 80)
context.logger.info(f"Source : {SILVER_SOURCE}")

context.logger.info(f"Target : {GOLD_TARGET}")

silver_count = spark.read.format("delta").load(SILVER_SOURCE).count()
context.logger.info(f"Silver input rows : {silver_count:,}")

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

    context.logger.info(f"Gold KPI rows : {gold_count:,}")

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
except Exception as exc:

    failed_record = context.audit.fail_run(
        stage="gold",
        exception=exc,
        timer_start=start_time,
    )

    context.audit.write_record(
        audit_path=audit_target,
        record=failed_record,
        is_databricks=IS_DATABRICKS,
    )

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=context.audit.get_run_id(),
        duration_seconds=round(time.time() - start_time),
    )

    context.logger.pipeline_failed(str(exc))
    context.exception("Gold processing failed", exc)

    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample business query: top 10 machines by fault events

# COMMAND ----------
context.logger.info("Displaying top 10 machines by fault events.")
top_faults = gold_df.select(
    "customer_id",
    "machine_id",
    "fault_events",
    "avg_fuel_level",
    "avg_payload_t",
).orderBy("fault_events", ascending=False)

if IS_DATABRICKS:
    display(top_faults.limit(10))
else:
    top_faults.show(10, truncate=False)

# COMMAND ----------
elapsed = time.time() - start_time

context.logger.info("=" * 80)
context.logger.info(f"Execution Time : {elapsed:.2f} seconds")
context.logger.info("=" * 80)

context.logger.info(f"Gold processing completed with {gold_count:,} KPI rows.")

audit_record = context.audit.finish_run(
    stage="gold",
    rows_read=silver_count,
    rows_written=gold_count,
    rows_rejected=0,
    source_path=SILVER_SOURCE,
    target_path=GOLD_TARGET,
    timer_start=start_time,
)

context.audit.write_record(
    audit_path=audit_target,
    record=audit_record,
    is_databricks=IS_DATABRICKS,
)
context.logger.info("KPI aggregation completed successfully.")
context.control.finish_run(
    pipeline_name=pipeline_name,
    run_id=context.audit.get_run_id(),
    rows_read=silver_count,
    rows_written=gold_count,
    duration_seconds=round(time.time() - start_time),
)

context.logger.pipeline_completed()

if IS_DATABRICKS:
    dbutils.notebook.exit(str(gold_count))
else:
    context.logger.info("Gold notebook finished successfully.")
