from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"

BRONZE_ASSIGNMENT_TABLE = (
    f"{CATALOG_NAME}.bronze.license_assignment_change_event_raw_dlt"
)

STANDARDIZED_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.assignment_event_standardized_stream"
)

CHECKPOINT_PATH = (
    "/Volumes/adb_classic_compute_catalog/monitoring/checkpoints/"
    "silver_assignment_event_standardization_streaming"
)


def parse_timestamp(column_name: str):
    return F.coalesce(
        F.expr(f"try_to_timestamp({column_name}, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({column_name}, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"),
        F.expr(f"try_to_timestamp({column_name})"),
    )


def parse_date(column_name: str):
    return F.coalesce(
        F.expr(f"try_to_date({column_name}, 'M/d/yyyy')"),
        F.expr(f"try_to_date({column_name}, 'MM/dd/yyyy')"),
        F.expr(f"try_to_date({column_name}, 'yyyy-MM-dd')"),
    )


raw_assignment_stream = spark.readStream.table(BRONZE_ASSIGNMENT_TABLE)


standardized_assignment_stream = (
    raw_assignment_stream
    .select(
        F.upper(F.trim(F.col("assignment_event_id"))).alias("assignment_event_id"),
        F.upper(F.trim(F.col("assignment_id"))).alias("assignment_id"),
        F.upper(F.trim(F.col("operation_code"))).alias("operation_code"),

        F.expr("try_cast(operation_sequence as int)").alias("operation_sequence"),

        parse_timestamp("operation_timestamp").alias("operation_timestamp"),
        parse_timestamp("effective_timestamp").alias("effective_timestamp"),

        F.upper(F.trim(F.col("employee_id"))).alias("employee_id"),
        F.upper(F.trim(F.col("application_id"))).alias("application_id"),
        F.upper(F.trim(F.col("license_tier_id"))).alias("license_tier_id"),

        F.trim(F.col("delivery_batch_id")).alias("delivery_batch_id"),
        parse_date("delivery_date").alias("delivery_date"),

        F.col("source_dataset"),
        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp_utc"),
        F.current_timestamp().alias("silver_processed_timestamp_utc"),
        F.col("_rescued_data").alias("_rescued_data"),
    )
)


query = (
    standardized_assignment_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .queryName("silver_assignment_event_standardization_streaming")
    .toTable(STANDARDIZED_STREAM_TABLE)
)

query.awaitTermination()

print(
    "Completed assignment event Structured Streaming standardization. "
    f"Output table: {STANDARDIZED_STREAM_TABLE}"
)