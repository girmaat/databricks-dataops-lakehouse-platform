import dlt
from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"
BRONZE_USAGE_TABLE = f"{CATALOG_NAME}.bronze.application_usage_event_raw_dlt"


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


@dlt.table(
    name="application_usage_event_standardized_dlt",
    comment=(
        "Silver standardized application usage events from DLT Bronze. "
        "This table normalizes strings, parses timestamps/dates, casts numeric fields, "
        "preserves raw input values for quality diagnostics, and keeps Bronze lineage metadata."
    ),
)
def application_usage_event_standardized_dlt():
    bronze_df = spark.readStream.table(BRONZE_USAGE_TABLE)

    standardized_df = (
        bronze_df
        .select(
            F.col("usage_event_id").alias("usage_event_id_raw"),
            F.col("employee_id").alias("employee_id_raw"),
            F.col("application_id").alias("application_id_raw"),
            F.col("event_type_code").alias("event_type_code_raw"),
            F.col("event_timestamp_utc").alias("event_timestamp_utc_raw"),
            F.col("event_units").alias("event_units_raw"),
            F.col("delivery_date").alias("delivery_date_raw"),

            F.upper(F.trim(F.col("usage_event_id"))).alias("usage_event_id"),
            F.upper(F.trim(F.col("employee_id"))).alias("employee_id"),
            F.upper(F.trim(F.col("application_id"))).alias("application_id"),
            F.upper(F.trim(F.col("event_type_code"))).alias("event_type_code"),

            parse_timestamp("event_timestamp_utc").alias("event_timestamp_utc"),
            F.expr("try_cast(event_units as int)").alias("event_units"),
            parse_date("delivery_date").alias("delivery_date"),

            F.trim(F.col("delivery_batch_id")).alias("delivery_batch_id"),

            F.col("source_dataset"),
            F.col("source_file_name"),
            F.col("source_file_path"),
            F.col("ingestion_timestamp_utc"),
            F.current_timestamp().alias("silver_processed_timestamp_utc"),
            F.col("_rescued_data").alias("_rescued_data"),
        )
    )

    return standardized_df