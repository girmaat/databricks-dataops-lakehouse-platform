# Read landed application usage CSV deliveries from a Unity Catalog Volume using Auto Loader and create a managed Bronze streaming table.

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit


SOURCE_DATASET = "application_usage_event"

SOURCE_PATH = (
    "/Volumes/adb_classic_compute_catalog/landing/source_deliveries/"
    "application_usage_event/"
)


@dp.table(
    name="application_usage_event_raw_dlt",
    comment=(
        "Bronze application usage events ingested from landing volume using "
        "Auto Loader inside a Lakeflow Spark Declarative Pipeline."
    ),
    table_properties={
        "quality": "bronze",
        "source_dataset": SOURCE_DATASET,
    },
)
def application_usage_event_raw_dlt() -> DataFrame:
    raw_usage_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
    )

    return (
        raw_usage_stream
        .withColumn("source_dataset", lit(SOURCE_DATASET))
        .withColumn("source_file_name", col("_metadata.file_name"))
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("ingestion_timestamp_utc", current_timestamp())
    )