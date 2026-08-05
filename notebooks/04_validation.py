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
import sys
from pathlib import Path
from datetime import datetime, UTC
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


import json
import os


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
    STATUS_SKIPPED,
    STATUS_FAILED,
    STATUS_STARTED,
)

# ------------------------------------------------------------------


config = get_config()
metadata = get_metadata()
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
context.logger.debug(f"Resolved paths: {paths}")

context.audit.start_run()

context.control.start_run(
    pipeline_name=pipeline_name,
    pipeline_type=metadata["pipeline"]["type"],
    run_id=context.audit.get_run_id(),
)
if IS_DATABRICKS:
    dbutils.widgets.text("snapshot_day", "0", "Day index being processed")
    snapshot_day = dbutils.widgets.get("snapshot_day")
else:
    snapshot_day = sys.argv[1] if len(sys.argv) > 1 else "0"

previous_status = context.control.last_stage_status(
    pipeline_name=pipeline_name,
    source_file=f"{paths['landing']}/snapshot_day{snapshot_day}.csv",
)

if previous_status == STATUS_SKIPPED:

    context.logger.info("Gold skipped. Skipping Validation.")

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
        error_message="Gold stage failed.",
    )

    raise RuntimeError("Gold stage failed.")

elif previous_status == STATUS_STARTED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=context.audit.get_run_id(),
        error_message="Gold stage incomplete.",
    )

    raise RuntimeError("Gold stage incomplete.")

# ------------------------------------------------------------------

BRONZE_PATH = paths["bronze"]
SILVER_PATH = paths["silver"]
GOLD_PATH = paths["gold"]
RECON_PATH = paths["reconciliation_table"] if IS_DATABRICKS else paths["reconciliation"]

REPORT_DIR = config["reports_directory"]

os.makedirs(REPORT_DIR, exist_ok=True)

# ------------------------------------------------------------------


def delta_exists(path):

    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


# ------------------------------------------------------------------

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

CONTROL_PATH = paths["control"]

if IS_DATABRICKS:
    validation["control_exists"] = spark.catalog.tableExists(CONTROL_PATH)
else:
    validation["control_exists"] = delta_exists(CONTROL_PATH)

if validation["control_exists"]:

    if IS_DATABRICKS:
        control = spark.table(CONTROL_PATH)
    else:
        control = spark.read.format("delta").load(CONTROL_PATH)

    latest_rows = (
        control.filter(F.col("pipeline_name") == pipeline_name)
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

        if latest["rows_written"] > latest["rows_read"]:
            errors.append("Control table reports more rows written than rows read.")

        if latest["status"] != "SUCCESS":
            errors.append("Latest control table run is not SUCCESS.")

    else:
        errors.append("Control table exists but contains no records.")

else:
    errors.append("Control table missing.")


if validation["reconciliation_exists"]:

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
        f"No reconciliation record found for run_id={current_run_id}."

else:
    errors.append("Reconciliation table missing.")
# --------------------------------------------------------
# ------------------------------------------------------------------

AUDIT_PATH = paths["audit_table"] if IS_DATABRICKS else paths["audit"]

if IS_DATABRICKS:
    validation["audit_exists"] = spark.catalog.tableExists(AUDIT_PATH)
else:
    validation["audit_exists"] = delta_exists(AUDIT_PATH)

if validation["audit_exists"]:

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


SCHEMA_PATH = paths["schema_history"]

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

QUARANTINE_PATH = paths["quarantine"]

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
        errors.append("Quarantine row count does not match reconciliation DQ count.")

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
    run_id=context.audit.get_run_id(),
    rows_read=gold_rows,
    rows_written=gold_rows,
)
context.logger.pipeline_completed()

if IS_DATABRICKS:
    dbutils.notebook.exit(validation["status"])
else:
    context.logger.info(f"Validation completed with status: {validation['status']}")
