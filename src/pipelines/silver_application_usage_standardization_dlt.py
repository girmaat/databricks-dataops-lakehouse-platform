# Databricks Lakeflow / DLT Silver pipeline

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="application_usage_event_standardized_dlt",
    comment=(
        "Silver standardized application usage events from DLT Bronze. "
        "This table normalizes strings, parses timestamps/dates, casts numeric fields, "
        "and preserves Bronze lineage metadata before quality routing."
    )
)
def application_usage_event_standardized_dlt():
    bronze_df = spark.readStream.table("adb_classic_compute_catalog.bronze.application_usage_event_raw_dlt"
)

    standardized_df = (
        bronze_df
        .withColumn("usage_event_id_raw", F.col("usage_event_id"))
        .withColumn("employee_id_raw", F.col("employee_id"))
        .withColumn("application_id_raw", F.col("application_id"))
        .withColumn("event_type_code_raw", F.col("event_type_code"))
        .withColumn("event_timestamp_utc_raw", F.col("event_timestamp_utc"))
        .withColumn("event_units_raw", F.col("event_units"))
        .withColumn("delivery_date_raw", F.col("delivery_date"))

        .withColumn("usage_event_id", F.upper(F.trim(F.col("usage_event_id"))))
        .withColumn("employee_id", F.upper(F.trim(F.col("employee_id"))))
        .withColumn("application_id", F.upper(F.trim(F.col("application_id"))))
        .withColumn("event_type_code", F.upper(F.trim(F.col("event_type_code"))))

        .withColumn(
            "event_timestamp_utc",
            F.to_timestamp(F.trim(F.col("event_timestamp_utc")))
        )
        .withColumn(
            "event_units",
            F.trim(F.col("event_units")).cast("int")
        )
        .withColumn(
            "delivery_date",
            F.to_date(F.trim(F.col("delivery_date")))
        )
    )

    return standardized_df