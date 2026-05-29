# Reads the landed employee snapshot CSV through Auto Loader and writes the raw delivered rows to a managed Bronze Delta table.

from pyspark.sql import functions as F

SOURCE_PATH = (
    "/Volumes/adb_classic_compute_catalog/landing/source_deliveries/"
    "employee_directory_snapshot/"
)

SCHEMA_LOCATION = (
    "/Volumes/adb_classic_compute_catalog/bronze/ingestion_state/"
    "employee_directory_snapshot/schema"
)

CHECKPOINT_LOCATION = (
    "/Volumes/adb_classic_compute_catalog/bronze/ingestion_state/"
    "employee_directory_snapshot/checkpoint"
)

TARGET_TABLE = "adb_classic_compute_catalog.bronze.employee_directory_snapshot_raw"


employee_raw_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
    .withColumn("source_dataset", F.lit("employee_directory_snapshot"))
    .withColumn("source_file_name", F.col("_metadata.file_name"))
    .withColumn("source_file_path", F.col("_metadata.file_path"))
    .withColumn("ingestion_timestamp_utc", F.current_timestamp())
)

query = (
    employee_raw_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

""" Validation """

display(
    spark.sql(
        """
        SELECT *
        FROM adb_classic_compute_catalog.bronze.employee_directory_snapshot_raw
        ORDER BY employee_id
        """
    )
)

display(
    spark.sql(
        """
        SELECT
            COUNT(*) AS bronze_row_count,
            COUNT(DISTINCT source_file_name) AS source_file_count,
            MIN(source_file_name) AS source_file_name,
            MIN(source_dataset) AS source_dataset
        FROM adb_classic_compute_catalog.bronze.employee_directory_snapshot_raw
        """
    )
)