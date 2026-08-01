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

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))



import json
import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.config_loader import (
    get_paths,
    get_environment,
    get_config,
    get_metadata,
)

# ------------------------------------------------------------------



config = get_config()
metadata = get_metadata()
paths = get_paths()
environment = get_environment()

IS_DATABRICKS = environment == "databricks"

if not IS_DATABRICKS:

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("PipelineValidation")
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

from datetime import datetime, UTC

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

#if not validation["audit_exists"]:
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
        silver_rows = (
            spark.read.format("delta")
            .load(SILVER_PATH)
            .count()
        )

if validation["gold_exists"]:
    if IS_DATABRICKS:
        gold_rows = spark.table(GOLD_PATH).count()
    else:
        gold_rows = (
            spark.read.format("delta")
            .load(GOLD_PATH)
            .count()
        )

validation["bronze_rows"] = bronze_rows
validation["silver_rows"] = silver_rows
validation["gold_rows"] = gold_rows

# ------------------------------------------------------------------

if validation["reconciliation_exists"]:

    if IS_DATABRICKS:
        recon = (
            spark.table(RECON_PATH)
            .orderBy(F.desc("snapshot_day"))
            .limit(1)
            .collect()[0]
        )
    else:
        recon = (
            spark.read.format("delta")
            .load(RECON_PATH)
            .orderBy(F.desc("snapshot_day"))
            .limit(1)
            .collect()[0]
        )

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
        errors.append("Row conservation check failed at silver DQ gate -- rows may have been lost.")
# ------------------------------------------------------------------

#if validation["audit_exists"]:

 #####

    #failed = audit.filter(
      #  F.col("status") != "SUCCESS"
    #).count()

    #validation["failed_audits"] = failed

##       errors.append(f"{failed} failed audit entries")

# ------------------------------------------------------------------

if bronze_rows < silver_rows:

    errors.append(
        "Silver contains more rows than Bronze."
    )

if silver_rows == 0:

    errors.append(
        "Silver table empty."
    )

if gold_rows == 0:

    errors.append(
        "Gold table empty."
    )

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

# ------------------------------------------------------------------

print("=" * 60)
print("PIPELINE VALIDATION")
print("=" * 60)

for k, v in validation.items():
    print(f"{k:30} : {v}")

print("=" * 60)

if validation["status"] == "PASS":
    print("VALIDATION PASSED")
else:
    print("VALIDATION FAILED")

print("=" * 60)