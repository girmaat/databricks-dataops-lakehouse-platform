# Databricks Lakeflow / DLT Silver pipeline

import dlt
from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"
BRONZE_EMPLOYEE_TABLE = f"{CATALOG_NAME}.bronze.employee_directory_snapshot_raw_dlt"


def parse_date(column_name: str):
    """
    Parse common source date formats into DATE.

    Why this helper exists:
    Employee source rows currently contain values like:
      - 7/19/2021
      - 1/1/2024
      - 2026-02-30

    Invalid dates such as 2026-02-30 should become NULL after parsing.
    The quality layer will decide whether that NULL means the row should
    be quarantined.
    """
    return F.coalesce(
        F.to_date(F.col(column_name), "M/d/yyyy"),
        F.to_date(F.col(column_name), "MM/dd/yyyy"),
        F.to_date(F.col(column_name), "yyyy-MM-dd"),
    )


@dlt.table(
    name="employee_snapshot_standardized_dlt",
    comment=(
        "Standardized employee snapshot records from Bronze. "
        "This table normalizes keys, status values, email casing, and date fields. "
    ),
)
def employee_snapshot_standardized_dlt():
    bronze_df = spark.read.table(BRONZE_EMPLOYEE_TABLE)

    standardized_df = (
        bronze_df
        .select(
            F.upper(F.trim(F.col("employee_id"))).alias("employee_id"),
            F.lower(F.trim(F.col("user_principal_name"))).alias("user_principal_name"),
            F.upper(F.trim(F.col("employment_status"))).alias("employment_status"),
            F.upper(F.trim(F.col("department_id"))).alias("department_id"),
            F.upper(F.trim(F.col("region_code"))).alias("region_code"),

            parse_date("hire_date").alias("hire_date"),
            parse_date("termination_date").alias("termination_date"),
            parse_date("record_effective_start_date").alias("record_effective_start_date"),
            parse_date("record_effective_end_date").alias("record_effective_end_date"),
            parse_date("snapshot_date").alias("snapshot_date"),

            F.col("source_dataset"),
            F.col("source_file_name"),
            F.col("source_file_path"),
            F.col("ingestion_timestamp_utc"),
            F.current_timestamp().alias("silver_processed_timestamp_utc"),
        )
    )

    return standardized_df