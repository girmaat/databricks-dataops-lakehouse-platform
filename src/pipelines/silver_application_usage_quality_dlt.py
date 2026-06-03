# Reads standardized application usage events from the Silver standardized DLT table, applies quality rules, and splits records into valid and quarantine outputs.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SUPPORTED_EVENT_TYPES = [
    "REPORT_VIEWED",
    "DASHBOARD_SHARED",
    "DATA_MODEL_AUTHORED",
    "PAGINATED_REPORT_PUBLISHED",
    "SIGN_IN",
]


@dlt.table(
    name="application_usage_event_quality_dlt",
    comment="""
    Quality-classified Silver application usage events.
    Preserves all standardized records and adds quality_reasons
    and is_quarantined.
    """
)
def application_usage_event_quality_dlt():
    standardized_df = dlt.read("application_usage_event_standardized_dlt")

    duplicate_window = Window.partitionBy("usage_event_id")

    quality_df = (
        standardized_df
        .withColumn(
            "usage_event_id_record_count",
            F.when(
                F.col("usage_event_id").isNotNull(),
                F.count("*").over(duplicate_window)
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "quality_reasons",
            F.filter(
                F.array(
                    F.when(
                        F.col("usage_event_id").isNull() |
                        (F.trim(F.col("usage_event_id")) == ""),
                        F.lit("missing_usage_event_id")
                    ),
                    F.when(
                        F.col("employee_id").isNull() |
                        (F.trim(F.col("employee_id")) == ""),
                        F.lit("missing_employee_id")
                    ),
                    F.when(
                        F.col("application_id").isNull() |
                        (F.trim(F.col("application_id")) == ""),
                        F.lit("missing_application_id")
                    ),
                    F.when(
                        F.col("event_type_code").isNull() |
                        (F.trim(F.col("event_type_code")) == ""),
                        F.lit("missing_event_type_code")
                    ),
                    F.when(
                        F.col("event_type_code").isNotNull() &
                        (~F.col("event_type_code").isin(SUPPORTED_EVENT_TYPES)),
                        F.lit("unsupported_event_type_code")
                    ),
                    F.when(
                        F.col("event_timestamp_utc").isNull(),
                        F.lit("invalid_event_timestamp_utc")
                    ),
                    F.when(
                        F.col("event_units").isNull(),
                        F.lit("invalid_event_units")
                    ),
                    F.when(
                        F.col("event_units") < 0,
                        F.lit("negative_event_units")
                    ),
                    F.when(
                        F.col("usage_event_id_record_count") > 1,
                        F.lit("duplicate_usage_event_id")
                    )
                ),
                lambda reason: reason.isNotNull()
            )
        )
        .withColumn(
            "is_quarantined",
            F.size(F.col("quality_reasons")) > 0
        )
        .withColumn(
            "quality_checked_timestamp_utc",
            F.current_timestamp()
        )
    )

    return quality_df


@dlt.table(
    name="application_usage_event_valid_dlt",
    comment="""
    Valid Silver application usage events that passed all quality rules.
    Gold usage analytics should read from this table.
    """
)
def application_usage_event_valid_dlt():
    return (
        dlt.read("application_usage_event_quality_dlt")
        .filter(F.col("is_quarantined") == F.lit(False))
    )


@dlt.table(
    name="application_usage_event_quarantine_dlt",
    comment="""
    Quarantined Silver application usage events that failed one or more
    quality rules. These records are excluded from valid analytics but
    preserved for investigation.
    """
)
def application_usage_event_quarantine_dlt():
    return (
        dlt.read("application_usage_event_quality_dlt")
        .filter(F.col("is_quarantined") == F.lit(True))
    )