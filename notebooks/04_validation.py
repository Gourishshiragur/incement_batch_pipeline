"""
04_validation.py

Enterprise Pipeline Validation

Validates:

✓ Bronze table exists
✓ Silver table exists
✓ Gold table exists
✓ Audit table exists
✓ Reconciliation table exists

✓ Row count consistency
✓ DQ drop count
✓ Incremental processing metrics
✓ Gold output
✓ Audit success
✓ Data loss detection

Outputs:

reports/
    validation_report.json
"""

from __future__ import annotations

import os
import sys
import json
from pathlib import Path
from datetime import datetime, UTC
import time

# Local only: add project root for imports
if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
    PROJECT_ROOT = Path.cwd()

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.framework.context import FrameworkContext
from utils.config_loader import (
    get_paths,
    get_environment,
    get_config,
    get_metadata,
    get_pipeline_name,
)
from src.framework.constants import (
    STATUS_SUCCESS,
    STATUS_SKIPPED,
    STATUS_FAILED,
    STATUS_STARTED,
)

# ------------------------------------------------------------------


config = get_config()
metadata = get_metadata()
pipeline_type = metadata.get("pipeline", {}).get("type", "batch")
paths = get_paths()
environment = get_environment()
pipeline_name = get_pipeline_name()


IS_DATABRICKS = environment == "databricks"


if not IS_DATABRICKS:

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
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()


if IS_DATABRICKS:

    dbutils.widgets.text("snapshot_day", "0")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("execution_id", "")

    snapshot_day = dbutils.widgets.get("snapshot_day")
    shared_run_id = dbutils.widgets.get("run_id") or None
    shared_execution_id = dbutils.widgets.get("execution_id") or None

else:

    snapshot_day = sys.argv[1] if len(sys.argv) > 1 else "0"
    shared_run_id = sys.argv[2] if len(sys.argv) > 2 else None
    shared_execution_id = sys.argv[3] if len(sys.argv) > 3 else None


context = FrameworkContext(
    spark=spark,
    pipeline_name=pipeline_name,
    pipeline_type=pipeline_type,
    control_path=paths["control"],
    control_table=paths["control_table"],
    quarantine_path=paths["quarantine"],
    schema_history_path=paths["schema_history"],
    schema_history_table=paths["schema_history_table"],
    schema_changes_path=paths["schema_changes"],
    schema_changes_table=paths["schema_changes_table"],
    run_id=shared_run_id,
    execution_id=shared_execution_id,
)

context.logger.pipeline_started()
context.logger.debug(f"Resolved paths: {paths}")

run_id = context.audit.start_run()

SOURCE_FILE = f"snapshot_day{snapshot_day}.csv"

previous_status = context.control.last_stage_status(
    pipeline_name=pipeline_name,
    source_file=SOURCE_FILE,
    stage="gold",
)

if previous_status == STATUS_SUCCESS:
    pass

elif previous_status == STATUS_SKIPPED:

    context.logger.info("Gold skipped. Skipping Validation.")

    context.control.skip_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
    )

    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

elif previous_status == STATUS_FAILED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="Gold stage failed.",
    )

    raise RuntimeError("Gold stage failed.")

elif previous_status == STATUS_STARTED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="Gold stage incomplete.",
    )

    raise RuntimeError("Gold stage incomplete.")

else:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message=f"Unexpected Gold status: {previous_status}",
    )

    raise RuntimeError(f"Unexpected Gold status: {previous_status}")

# Skip if Validation has already run successfully for this exact source file --
# without this check (and the stage= filter on already_processed), Validation
# would re-run and rewrite validation_report.json on every run for the same day.
if context.control.already_processed(
    pipeline_name,
    SOURCE_FILE,
    stage="validation",
):
    context.logger.info(f"Validation already processed for: {SOURCE_FILE}")

    context.control.skip_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
    )

    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

context.control.start_run(
    pipeline_name=pipeline_name,
    pipeline_type=pipeline_type,
    run_id=run_id,
    execution_id=context.audit.get_execution_id(),
    stage="validation",
    source_file=SOURCE_FILE,
)

# ------------------------------------------------------------------

if IS_DATABRICKS:

    BRONZE_PATH = paths["bronze_table"]
    SILVER_PATH = paths["silver_table"]
    GOLD_PATH = paths["gold_table"]
    RECON_PATH = paths["reconciliation_table"]
    CONTROL_PATH = paths["control_table"]
    AUDIT_PATH = paths["audit_table"]
    SCHEMA_PATH = paths["schema_history_table"]
    QUARANTINE_PATH = paths["quarantine_table"]

else:

    BRONZE_PATH = paths["bronze"]
    SILVER_PATH = paths["silver"]
    GOLD_PATH = paths["gold"]
    RECON_PATH = paths["reconciliation"]
    CONTROL_PATH = paths["control"]
    AUDIT_PATH = paths["audit"]
    SCHEMA_PATH = paths["schema_history"]
    QUARANTINE_PATH = paths["quarantine"]

os.makedirs(REPORT_DIR, exist_ok=True)

# ------------------------------------------------------------------


def delta_exists(path):

    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


# ------------------------------------------------------------------
start_time = time.time()
try:
    validation = {}

    validation["execution_time"] = datetime.now(UTC).isoformat()

    if IS_DATABRICKS:
        validation["bronze_exists"] = spark.catalog.tableExists(BRONZE_PATH)
        validation["silver_exists"] = spark.catalog.tableExists(SILVER_PATH)
        validation["gold_exists"] = spark.catalog.tableExists(GOLD_PATH)
        validation["reconciliation_exists"] = spark.catalog.tableExists(RECON_PATH)
    else:
        validation["bronze_exists"] = delta_exists(BRONZE_PATH)
        validation["silver_exists"] = delta_exists(SILVER_PATH)
        validation["gold_exists"] = delta_exists(GOLD_PATH)
        validation["reconciliation_exists"] = delta_exists(RECON_PATH)

    # ------------------------------------------------------------------

    errors = []

    if not validation["bronze_exists"]:
        errors.append("Bronze table missing")

    if not validation["silver_exists"]:
        errors.append("Silver table missing")

    if not validation["gold_exists"]:
        errors.append("Gold table missing")

    # if not validation["audit_exists"]:
    # errors.append("Audit table missing")

    if not validation["reconciliation_exists"]:
        errors.append("Reconciliation table missing")

    # ------------------------------------------------------------------

    bronze_rows = 0
    silver_rows = 0
    gold_rows = 0

    if validation["bronze_exists"]:
        if IS_DATABRICKS:
            bronze_rows = spark.table(BRONZE_PATH).count()
        else:
            bronze_rows = spark.read.format("delta").load(BRONZE_PATH).count()
    if validation["silver_exists"]:
        if IS_DATABRICKS:
            silver_rows = spark.table(SILVER_PATH).count()
        else:
            silver_rows = spark.read.format("delta").load(SILVER_PATH).count()

    if validation["gold_exists"]:
        if IS_DATABRICKS:
            gold_rows = spark.table(GOLD_PATH).count()
        else:
            gold_rows = spark.read.format("delta").load(GOLD_PATH).count()

    validation["bronze_rows"] = bronze_rows
    validation["silver_rows"] = silver_rows
    validation["gold_rows"] = gold_rows

    if IS_DATABRICKS:
        validation["control_exists"] = spark.catalog.tableExists(CONTROL_PATH)
    else:
        validation["control_exists"] = delta_exists(CONTROL_PATH)

    current_run_id = None
    if validation["control_exists"]:

        if IS_DATABRICKS:
            control = spark.table(CONTROL_PATH)
        else:
            control = spark.read.format("delta").load(CONTROL_PATH)

        # Filtered by source_file + stage + SUCCESS status -- without these,
        # this can grab any stage's most-recently-touched row for this
        # pipeline (including Validation's own STARTED row inserted just
        # above), producing None-vs-None comparisons and misleading results.
        latest_rows = (
            control.filter(F.col("pipeline_name") == pipeline_name)
            .filter(F.col("source_file") == SOURCE_FILE)
            .filter(F.col("stage") == "gold")
            .filter(F.col("status") == "SUCCESS")
            .orderBy(F.desc("updated_at"))
            .limit(1)
            .collect()
        )

        if latest_rows:

            latest = latest_rows[0]

            current_run_id = latest["run_id"]

            validation["current_run_id"] = current_run_id

            validation["control_status"] = latest["status"]
            validation["control_source_file"] = latest["source_file"]
            validation["control_rows_read"] = latest["rows_read"]
            validation["control_rows_written"] = latest["rows_written"]

            if (
                latest["rows_written"] is not None
                and latest["rows_read"] is not None
                and latest["rows_written"] > latest["rows_read"]
            ):
                errors.append("Control table reports more rows written than rows read.")

            if latest["status"] != "SUCCESS":
                errors.append("Latest control table run is not SUCCESS.")

        else:
            errors.append("Control table exists but contains no records.")

    else:
        errors.append("Control table missing.")

    if validation["reconciliation_exists"] and current_run_id:

        if IS_DATABRICKS:
            recon_rows = (
                spark.table(RECON_PATH)
                .filter(F.col("run_id") == current_run_id)
                .limit(1)
                .collect()
            )

        else:
            recon_rows = (
                spark.read.format("delta")
                .load(RECON_PATH)
                .filter(F.col("run_id") == current_run_id)
                .limit(1)
                .collect()
            )

        if recon_rows:

            recon = recon_rows[0]

            validation["snapshot_day"] = recon["snapshot_day"]
            validation["snapshot_rows"] = recon["total_rows_in_snapshot"]
            validation["dq_dropped"] = recon["dq_dropped_rows"]
            validation["new_rows"] = recon["new_rows"]
            validation["changed_rows"] = recon["changed_rows"]
            validation["unchanged_rows"] = recon["unchanged_rows_skipped"]
            validation["incremental_volume"] = recon["incremental_volume_processed"]
            validation["reduction_pct"] = recon["reprocessing_reduction_pct"]
            validation["row_conservation_passed"] = recon["row_conservation_passed"]

            if not recon["row_conservation_passed"]:
                errors.append(
                    "Row conservation check failed at silver DQ gate -- rows may have been lost."
                )

        else:
            errors.append(
                f"No reconciliation record found for run_id={current_run_id}."
            )

    else:
        errors.append("Reconciliation table missing.")
    # --------------------------------------------------------
    # ------------------------------------------------------------------

    if IS_DATABRICKS:
        validation["audit_exists"] = spark.catalog.tableExists(AUDIT_PATH)
    else:
        validation["audit_exists"] = delta_exists(AUDIT_PATH)

    if validation["audit_exists"] and current_run_id:

        if IS_DATABRICKS:
            audit = spark.table(AUDIT_PATH)
        else:
            audit = spark.read.format("delta").load(AUDIT_PATH)

        current_audit = audit.filter(F.col("run_id") == current_run_id)

        audit_count = current_audit.count()

        validation["audit_records"] = audit_count

        if audit_count == 0:

            errors.append(f"No audit records found for run_id={current_run_id}.")

        else:

            failed = current_audit.filter(F.upper(F.col("status")) != "SUCCESS").count()

            validation["failed_audits"] = failed

            if failed > 0:
                errors.append(f"{failed} failed audit entries for current execution.")

    else:
        errors.append("Audit table missing.")

    if IS_DATABRICKS:
        validation["schema_history_exists"] = spark.catalog.tableExists(SCHEMA_PATH)
    else:
        validation["schema_history_exists"] = delta_exists(SCHEMA_PATH)

    if validation["schema_history_exists"]:

        if IS_DATABRICKS:
            schema_versions = spark.table(SCHEMA_PATH).count()
        else:
            schema_versions = spark.read.format("delta").load(SCHEMA_PATH).count()

        validation["schema_versions"] = schema_versions

    else:
        errors.append("Schema history missing.")

    if IS_DATABRICKS:
        validation["quarantine_exists"] = spark.catalog.tableExists(QUARANTINE_PATH)
    else:
        validation["quarantine_exists"] = delta_exists(QUARANTINE_PATH)

    if validation["quarantine_exists"]:

        if IS_DATABRICKS:
            quarantine_rows = spark.table(QUARANTINE_PATH).count()
        else:
            quarantine_rows = spark.read.format("delta").load(QUARANTINE_PATH).count()

        validation["quarantine_rows"] = quarantine_rows

        if "dq_dropped" in validation and quarantine_rows != validation["dq_dropped"]:
            errors.append(
                "Quarantine row count does not match reconciliation DQ count."
            )

    else:
        errors.append("Quarantine table missing.")
    # ------------------------------------------------------------------

    if bronze_rows < silver_rows:

        errors.append("Silver contains more rows than Bronze.")

    if silver_rows == 0:

        errors.append("Silver table empty.")

    if gold_rows == 0:

        errors.append("Gold table empty.")

    # ------------------------------------------------------------------

    validation["status"] = "PASS"

    if errors:
        validation["status"] = "FAIL"

    validation["errors"] = errors

    # ------------------------------------------------------------------

    report_file = os.path.join(
        REPORT_DIR,
        "validation_report.json",
    )

    with open(report_file, "w") as f:

        json.dump(
            validation,
            f,
            indent=4,
            default=str,
        )

        context.logger.info(f"Validation report written to: {report_file}")

    # ------------------------------------------------------------------

    context.logger.info("=" * 60)
    context.logger.info("PIPELINE VALIDATION")
    context.logger.info("=" * 60)

    context.logger.info("=" * 60)
    context.logger.info(f"Overall Status : {validation['status']}")
    context.logger.info(f"Bronze Rows    : {bronze_rows:,}")
    context.logger.info(f"Silver Rows    : {silver_rows:,}")
    context.logger.info(f"Gold Rows      : {gold_rows:,}")
    context.logger.info("=" * 60)

    context.logger.info("=" * 60)

    if validation["status"] == "PASS":
        context.logger.info("VALIDATION PASSED")
    else:
        context.logger.warning("VALIDATION FAILED")

    context.logger.info("=" * 60)

    context.logger.info(f"Validation report written to: {report_file}")

    context.control.finish_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        rows_read=gold_rows,
        rows_written=gold_rows,
        duration_seconds=round(time.time() - start_time),
    )
    context.logger.pipeline_completed()

except Exception as exc:

    failed_record = context.audit.fail_run(
        stage="validation",
        exception=exc,
        timer_start=start_time,
    )

    context.audit.write_record(
        audit_path=AUDIT_PATH,
        record=failed_record,
        is_databricks=IS_DATABRICKS,
    )

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        duration_seconds=round(time.time() - start_time),
    )

    context.logger.pipeline_failed(str(exc))
    context.logger.exception(f"Validation failed: {exc}")

    raise

if IS_DATABRICKS:
    dbutils.notebook.exit(validation["status"])
else:
    context.logger.info(f"Validation completed with status: {validation['status']}")
