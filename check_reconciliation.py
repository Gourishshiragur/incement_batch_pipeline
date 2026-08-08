from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("data/reconciliation")
df.select(
    "snapshot_day", "total_rows_in_snapshot", "dq_dropped_rows",
    "new_rows", "changed_rows", "unchanged_rows_skipped",
    "row_conservation_passed"
).orderBy("snapshot_day").show(truncate=False)
