import dlt
from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"

ASSIGNMENT_LANDING_PATH = (
    "/Volumes/adb_classic_compute_catalog/landing/source_deliveries/"
    "license_assignment_change_event/"
)


@dlt.table(
    name="license_assignment_change_event_raw_dlt",
    comment=(
        "Raw license assignment change events ingested from landing using "
        "Lakeflow/DLT Auto Loader. Bronze preserves source fields as strings "
        "and adds file/source metadata."
    ),
    table_properties={
        "quality": "bronze",
        "source_dataset": "license_assignment_change_event",
        "pipelines.autoOptimize.managed": "true",
    },
)
def license_assignment_change_event_raw_dlt():
    raw_assignment_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(ASSIGNMENT_LANDING_PATH)
    )

    bronze_df = (
        raw_assignment_stream
        .select(
            F.col("assignment_event_id").cast("string").alias("assignment_event_id"),
            F.col("assignment_id").cast("string").alias("assignment_id"),
            F.col("operation_code").cast("string").alias("operation_code"),
            F.col("operation_sequence").cast("string").alias("operation_sequence"),
            F.col("operation_timestamp").cast("string").alias("operation_timestamp"),
            F.col("effective_timestamp").cast("string").alias("effective_timestamp"),
            F.col("employee_id").cast("string").alias("employee_id"),
            F.col("application_id").cast("string").alias("application_id"),
            F.col("license_tier_id").cast("string").alias("license_tier_id"),
            F.col("delivery_batch_id").cast("string").alias("delivery_batch_id"),
            F.col("delivery_date").cast("string").alias("delivery_date"),

            F.lit("license_assignment_change_event").alias("source_dataset"),
            F.col("_metadata.file_name").alias("source_file_name"),
            F.col("_metadata.file_path").alias("source_file_path"),
            F.current_timestamp().alias("ingestion_timestamp_utc"),
            F.col("_rescued_data").alias("_rescued_data"),
        )
    )

    return bronze_df