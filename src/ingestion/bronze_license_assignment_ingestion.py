#   Read landed raw license assignment CSV deliveries with Auto Loader and append every received row to a governed Bronze Delta table.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

# Dataset-specific governed locations

SOURCE_DATASET = "license_assignment_change_event"

SOURCE_PATH = (
    "/Volumes/adb_classic_compute_catalog/landing/source_deliveries/"
    "license_assignment_change_event/"
)

SCHEMA_LOCATION = (
    "/Volumes/adb_classic_compute_catalog/bronze/ingestion_state/"
    "license_assignment_change_event/schema"
)

CHECKPOINT_LOCATION = (
    "/Volumes/adb_classic_compute_catalog/bronze/ingestion_state/"
    "license_assignment_change_event/checkpoint"
)

TARGET_TABLE = (
    "adb_classic_compute_catalog.bronze."
    "license_assignment_change_event_raw"
)

# Read newly arrived CSV files with Auto Loader

raw_assignment_stream: DataFrame = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("header", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
)

# Add Bronze technical metadata

bronze_assignment_stream: DataFrame = (
    raw_assignment_stream
    .withColumn("source_dataset", lit(SOURCE_DATASET))
    .withColumn("source_file_name", col("_metadata.file_name"))
    .withColumn("source_file_path", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp_utc", current_timestamp())
)


# Write newly discovered files to the managed Bronze Delta table

assignment_ingestion_query = (
    bronze_assignment_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

assignment_ingestion_query.awaitTermination()

# Validate the Bronze result

display(
    spark.sql(
        f"""
        SELECT
            COUNT(*) AS bronze_row_count,
            COUNT(DISTINCT source_file_path) AS source_file_count,
            source_file_name,
            source_dataset
        FROM {TARGET_TABLE}
        GROUP BY
            source_file_name,
            source_dataset
        ORDER BY
            source_file_name
        """
    )
)