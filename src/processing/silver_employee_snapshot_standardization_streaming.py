from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"

BRONZE_EMPLOYEE_TABLE = (
    f"{CATALOG_NAME}.bronze.employee_directory_snapshot_raw_dlt"
)

STANDARDIZED_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_standardized_stream"
)

CHECKPOINT_PATH = (
    "/Volumes/adb_classic_compute_catalog/monitoring/checkpoints/"
    "silver_employee_snapshot_standardization_streaming"
)

def parse_date(column_name: str):
    """
    Convert common source date string formats into DATE.

    Examples handled:
      - 7/19/2021
      - 04/30/2026
      - 2026-02-30

    Invalid dates, such as 2026-02-30, become NULL.
    Quality validation will handle those NULLs later.
    """
    return F.coalesce(
        F.to_date(F.col(column_name), "M/d/yyyy"),
        F.to_date(F.col(column_name), "MM/dd/yyyy"),
        F.to_date(F.col(column_name), "yyyy-MM-dd"),
    )


raw_employee_stream = spark.readStream.table(BRONZE_EMPLOYEE_TABLE)


standardized_employee_stream = (
    raw_employee_stream
    .select(
        F.upper(F.trim(F.col("employee_id"))).alias("employee_id"),

        F.lower(F.trim(F.col("user_principal_name"))).alias("user_principal_name"),

        F.upper(F.trim(F.col("employment_status"))).alias("employment_status"),

        F.upper(F.trim(F.col("department_id"))).alias("department_id"),

        F.upper(F.trim(F.col("region_code"))).alias("region_code"),

        parse_date("hire_date").alias("hire_date"),

        parse_date("termination_date").alias("termination_date"),

        parse_date("record_effective_start_date").alias(
            "record_effective_start_date"
        ),

        parse_date("record_effective_end_date").alias(
            "record_effective_end_date"
        ),

        parse_date("snapshot_date").alias("snapshot_date"),

        F.col("source_dataset"),
        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp_utc"),

        F.current_timestamp().alias("silver_processed_timestamp_utc"),

        F.col("_rescued_data").alias("_rescued_data"),
    )
)


query = (
    standardized_employee_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("silver_employee_snapshot_standardization_streaming")
    .toTable(STANDARDIZED_STREAM_TABLE)
)

query.awaitTermination()

print(
    "Completed employee snapshot Structured Streaming standardization. "
    f"Output table: {STANDARDIZED_STREAM_TABLE}"
)