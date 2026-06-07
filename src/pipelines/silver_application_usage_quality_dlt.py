import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

from quality_rules import (
    is_blank,
    add_quality_columns,
    valid_rows,
    quarantine_rows,
)


CATALOG_NAME = "adb_classic_compute_catalog"

EMPLOYEE_VALID_DLT_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_valid_dlt"
)

SUPPORTED_APPLICATION_IDS = ["APP-PBI"]

SUPPORTED_EVENT_TYPES = [
    "REPORT_VIEWED",
    "DASHBOARD_SHARED",
    "DATA_MODEL_AUTHORED",
    "PAGINATED_REPORT_PUBLISHED",
    "SIGN_IN",
]


@dlt.table(
    name="application_usage_event_quality_dlt",
    comment=(
        "Application usage event Silver quality table. Applies reusable S05.02 quality rules, "
        "adds quality_reasons, quality_reason_text, and is_quarantined."
    ),
)
def application_usage_event_quality_dlt():
    standardized_df = dlt.read("application_usage_event_standardized_dlt")

    employee_valid_df = (
        spark.read.table(EMPLOYEE_VALID_DLT_TABLE)
        .select(F.col("employee_id").alias("valid_employee_id"))
        .dropDuplicates(["valid_employee_id"])
    )

    usage_with_employee_df = (
        standardized_df.alias("u")
        .join(
            employee_valid_df.alias("e"),
            F.col("u.employee_id") == F.col("e.valid_employee_id"),
            "left",
        )
    )

    duplicate_event_window = Window.partitionBy("usage_event_id")

    usage_with_context_df = (
        usage_with_employee_df
        .withColumn(
            "usage_event_id_record_count",
            F.when(
                F.col("usage_event_id").isNotNull(),
                F.count("*").over(duplicate_event_window),
            ).otherwise(F.lit(None)),
        )
    )

    rules = [
        ("missing_usage_event_id", is_blank("usage_event_id")),
        (
            "duplicate_usage_event_id",
            F.col("usage_event_id_record_count") > 1,
        ),

        ("missing_employee_id", is_blank("employee_id")),
        (
            "unknown_employee_reference",
            F.col("employee_id").isNotNull()
            & F.col("valid_employee_id").isNull(),
        ),

        ("missing_application_id", is_blank("application_id")),
        (
            "unsupported_application_id",
            F.col("application_id").isNotNull()
            & (~F.col("application_id").isin(SUPPORTED_APPLICATION_IDS)),
        ),

        ("missing_event_type_code", is_blank("event_type_code")),
        (
            "unsupported_event_type_code",
            F.col("event_type_code").isNotNull()
            & (~F.col("event_type_code").isin(SUPPORTED_EVENT_TYPES)),
        ),

        ("invalid_event_timestamp_utc", F.col("event_timestamp_utc").isNull()),
        ("invalid_event_units", F.col("event_units").isNull()),
        ("negative_event_units", F.col("event_units") < 0),
    ]

    quality_df = (
        add_quality_columns(usage_with_context_df, rules)
        .withColumn("quality_checked_timestamp_utc", F.current_timestamp())
    )

    return quality_df


@dlt.table(
    name="application_usage_event_valid_dlt",
    comment=(
        "Valid Silver application usage events after reusable S05.02 quality validation. "
        "Gold usage analytics should read from this table."
    ),
)
def application_usage_event_valid_dlt():
    return valid_rows(dlt.read("application_usage_event_quality_dlt"))


@dlt.table(
    name="application_usage_event_quarantine_dlt",
    comment=(
        "Quarantined Silver application usage events with reusable S05.02 quality reasons. "
        "These records are excluded from valid analytics but preserved for investigation."
    ),
)
def application_usage_event_quarantine_dlt():
    return quarantine_rows(dlt.read("application_usage_event_quality_dlt"))