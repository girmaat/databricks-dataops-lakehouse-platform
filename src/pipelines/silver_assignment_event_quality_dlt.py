
import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F


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
        "Assignment event Silver quality table. Adds quality reasons and "
        "quarantine flag to standardized assignment events."
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
                F.when(F.col("operation_code") == F.lit("ASSIGN"), F.lit(1)).otherwise(F.lit(0))
            ).over(assignment_lifecycle_window),
        )
    )

    quality_df = (
        assignment_with_context_df
        .withColumn(
            "quality_reasons_raw",
            F.array(
                F.when(F.col("assignment_event_id").isNull(), F.lit("missing_assignment_event_id")),
                F.when(F.col("assignment_event_id_occurrence_count") > 1, F.lit("duplicate_assignment_event_id")),

                F.when(F.col("assignment_id").isNull(), F.lit("missing_assignment_id")),

                F.when(F.col("operation_code").isNull(), F.lit("missing_operation_code")),
                F.when(
                    F.col("operation_code").isNotNull()
                    & (~F.col("operation_code").isin(SUPPORTED_OPERATION_CODES)),
                    F.lit("unsupported_operation_code"),
                ),

                F.when(
                    F.col("operation_sequence").isNull() | (F.col("operation_sequence") <= 0),
                    F.lit("invalid_operation_sequence"),
                ),

                F.when(F.col("operation_timestamp").isNull(), F.lit("invalid_operation_timestamp")),
                F.when(F.col("effective_timestamp").isNull(), F.lit("invalid_effective_timestamp")),

                F.when(F.col("employee_id").isNull(), F.lit("missing_employee_id")),
                F.when(
                    F.col("employee_id").isNotNull()
                    & F.col("valid_employee_id").isNull(),
                    F.lit("unknown_employee_id"),
                ),

                F.when(F.col("application_id").isNull(), F.lit("missing_application_id")),
                F.when(
                    F.col("application_id").isNotNull()
                    & (~F.col("application_id").isin(SUPPORTED_APPLICATION_IDS)),
                    F.lit("unsupported_application_id"),
                ),

                F.when(F.col("license_tier_id").isNull(), F.lit("missing_license_tier_id")),
                F.when(
                    F.col("license_tier_id").isNotNull()
                    & (~F.col("license_tier_id").isin(SUPPORTED_LICENSE_TIER_IDS)),
                    F.lit("unsupported_license_tier_id"),
                ),

                F.when(
                    (F.col("operation_code") == F.lit("REVOKE"))
                    & (F.col("assignment_has_assign_event") == F.lit(0)),
                    F.lit("orphan_revoke"),
                ),
            ),
        )
        .withColumn(
            "quality_reasons",
            F.expr("filter(quality_reasons_raw, reason -> reason is not null)"),
        )
        .withColumn(
            "is_quarantined",
            F.size(F.col("quality_reasons")) > 0,
        )
        .drop("quality_reasons_raw")
    )

    return quality_df


@dlt.table(
    name="assignment_event_valid_dlt",
    comment="Valid assignment events after Silver DLT quality validation.",
)
def assignment_event_valid_dlt():
    return (
        dlt.read("assignment_event_quality_dlt")
        .where(F.col("is_quarantined") == False)
    )


@dlt.table(
    name="assignment_event_quarantine_dlt",
    comment="Quarantined assignment events with quality reasons.",
)
def assignment_event_quarantine_dlt():
    return (
        dlt.read("assignment_event_quality_dlt")
        .where(F.col("is_quarantined") == True)
    )