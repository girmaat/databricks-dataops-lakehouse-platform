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

SUPPORTED_OPERATION_CODES = ["ASSIGN", "CHANGE_TIER", "REVOKE"]
SUPPORTED_APPLICATION_IDS = ["APP-PBI"]
SUPPORTED_LICENSE_TIER_IDS = ["PBI-PRO", "PBI-PPU"]


@dlt.table(
    name="assignment_event_quality_dlt",
    comment=(
        "Assignment event Silver quality table. Applies reusable S05.02 quality rules, "
        "adds quality_reasons, quality_reason_text, and is_quarantined."
    ),
)
def assignment_event_quality_dlt():
    assignment_df = dlt.read("assignment_event_standardized_dlt")

    employee_valid_df = (
        spark.read.table(EMPLOYEE_VALID_DLT_TABLE)
        .select(F.col("employee_id").alias("valid_employee_id"))
        .dropDuplicates(["valid_employee_id"])
    )

    assignment_with_employee_df = (
        assignment_df.alias("a")
        .join(
            employee_valid_df.alias("e"),
            F.col("a.employee_id") == F.col("e.valid_employee_id"),
            "left",
        )
    )

    duplicate_event_window = Window.partitionBy("assignment_event_id")
    assignment_lifecycle_window = Window.partitionBy("assignment_id")

    assignment_with_context_df = (
        assignment_with_employee_df
        .withColumn(
            "assignment_event_id_occurrence_count",
            F.when(
                F.col("assignment_event_id").isNotNull(),
                F.count("*").over(duplicate_event_window),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "assignment_has_assign_event",
            F.max(
                F.when(
                    F.col("operation_code") == F.lit("ASSIGN"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).over(assignment_lifecycle_window),
        )
    )

    rules = [
        ("missing_assignment_event_id", is_blank("assignment_event_id")),
        (
            "duplicate_assignment_event_id",
            F.col("assignment_event_id_occurrence_count") > 1,
        ),

        ("missing_assignment_id", is_blank("assignment_id")),

        ("missing_operation_code", is_blank("operation_code")),
        (
            "unsupported_operation_code",
            F.col("operation_code").isNotNull()
            & (~F.col("operation_code").isin(SUPPORTED_OPERATION_CODES)),
        ),

        (
            "invalid_operation_sequence",
            F.col("operation_sequence").isNull()
            | (F.col("operation_sequence") <= 0),
        ),

        ("invalid_operation_timestamp", F.col("operation_timestamp").isNull()),
        ("invalid_effective_timestamp", F.col("effective_timestamp").isNull()),

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

        ("missing_license_tier_id", is_blank("license_tier_id")),
        (
            "unsupported_license_tier_id",
            F.col("license_tier_id").isNotNull()
            & (~F.col("license_tier_id").isin(SUPPORTED_LICENSE_TIER_IDS)),
        ),

        (
            "orphan_revoke",
            (F.col("operation_code") == F.lit("REVOKE"))
            & (F.col("assignment_has_assign_event") == F.lit(0)),
        ),
    ]

    return add_quality_columns(assignment_with_context_df, rules)


@dlt.table(
    name="assignment_event_valid_dlt",
    comment="Valid assignment events after reusable S05.02 Silver quality validation.",
)
def assignment_event_valid_dlt():
    return valid_rows(dlt.read("assignment_event_quality_dlt"))


@dlt.table(
    name="assignment_event_quarantine_dlt",
    comment="Quarantined assignment events with reusable S05.02 quality reasons.",
)
def assignment_event_quarantine_dlt():
    return quarantine_rows(dlt.read("assignment_event_quality_dlt"))