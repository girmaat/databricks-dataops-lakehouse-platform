# Read landed employee snapshot CSV deliveries from a Unity Catalog Volume using Auto Loader and create a managed Bronze streaming table.

import dlt
from pyspark.sql import functions as F

CATALOG_NAME = "adb_classic_compute_catalog"


EMPLOYEE_LANDING_PATH = (
    "/Volumes/adb_classic_compute_catalog/landing/source_deliveries/"
    "employee_directory_snapshot/"
)

@dlt.table(
    name="employee_directory_snapshot_raw_dlt",
    comment=(
        "Raw employee directory snapshot records ingested from the landing volume using Lakeflow/DLT Auto Loader."
    ),
    table_properties={
        "quality": "bronze",
        "source_dataset": "employee_directory_snapshot",
        "pipelines.autoOptimize.managed": "true",
    },
)
def employee_directory_snapshot_raw_dlt():
    raw_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(EMPLOYEE_LANDING_PATH)
    )

    bronze_df = (
        raw_df
        .select(
            F.col("employee_id").cast("string").alias("employee_id"),
            F.col("user_principal_name").cast("string").alias("user_principal_name"),
            F.col("employment_status").cast("string").alias("employment_status"),
            F.col("department_id").cast("string").alias("department_id"),
            F.col("region_code").cast("string").alias("region_code"),
            F.col("hire_date").cast("string").alias("hire_date"),
            F.col("termination_date").cast("string").alias("termination_date"),
            F.col("record_effective_start_date").cast("string").alias("record_effective_start_date"),
            F.col("record_effective_end_date").cast("string").alias("record_effective_end_date"),
            F.col("snapshot_date").cast("string").alias("snapshot_date"),

            F.lit("employee_directory_snapshot").alias("source_dataset"),
            F.col("_metadata.file_name").alias("source_file_name"),
            F.col("_metadata.file_path").alias("source_file_path"),
            F.current_timestamp().alias("ingestion_timestamp_utc"),
            F.col("_rescued_data").alias("_rescued_data"),
        )
    )

    return bronze_df
