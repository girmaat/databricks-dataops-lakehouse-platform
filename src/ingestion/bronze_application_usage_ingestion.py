# Read landed raw application usage CSV deliveries with Auto Loader and append every received row to a governed Bronze Delta table.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

SOURCE_DATASET = "application_usage_event"

SOURCE_PATH = (
    "/Volumes/adb_classic_compute_catalog/landing/source_deliveries/"
    "application_usage_event/"
)

SCHEMA_LOCATION = (
    "/Volumes/adb_classic_compute_catalog/bronze/ingestion_state/"
    "application_usage_event/schema"
)

CHECKPOINT_LOCATION = (
    "/Volumes/adb_classic_compute_catalog/bronze/ingestion_state/"
    "application_usage_event/checkpoint"
)

TARGET_TABLE = (
    "adb_classic_compute_catalog.bronze."
    "application_usage_event_raw"
)

raw_usage_stream: DataFrame = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
)

bronze_usage_stream: DataFrame = (
    raw_usage_stream
    .withColumn("source_dataset", lit(SOURCE_DATASET))
    .withColumn("source_file_name", col("_metadata.file_name"))
    .withColumn("source_file_path", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp_utc", current_timestamp())
)

usage_ingestion_query = (
    bronze_usage_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

usage_ingestion_query.awaitTermination()

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