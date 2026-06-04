from pyspark.sql import Window
from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"

STANDARDIZED_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.assignment_event_standardized_stream"
)

EMPLOYEE_VALID_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_valid_stream"
)

QUALITY_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.assignment_event_quality_stream"
)

VALID_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.assignment_event_valid_stream"
)

QUARANTINE_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.assignment_event_quarantine_stream"
)

CHECKPOINT_PATH = (
    "/Volumes/adb_classic_compute_catalog/monitoring/checkpoints/"
    "silver_assignment_event_quality_streaming"
)


SUPPORTED_OPERATION_CODES = ["ASSIGN", "CHANGE_TIER", "REVOKE"]
SUPPORTED_APPLICATION_IDS = ["APP-PBI"]
SUPPORTED_LICENSE_TIER_IDS = ["PBI-PRO", "PBI-PPU"]


def apply_assignment_quality_rules(standardized_df):
    employee_valid_df = (
        spark.read.table(EMPLOYEE_VALID_STREAM_TABLE)
        .select(F.col("employee_id").alias("valid_employee_id"))
        .dropDuplicates(["valid_employee_id"])
    )

    assignment_with_employee_df = (
        standardized_df.alias("a")
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


def process_assignment_quality_microbatch(batch_df, batch_id: int):
    print(f"Starting assignment quality microbatch: {batch_id}")

    full_standardized_df = spark.read.table(STANDARDIZED_STREAM_TABLE)

    quality_df = apply_assignment_quality_rules(full_standardized_df)

    valid_df = quality_df.where(F.col("is_quarantined") == False)
    quarantine_df = quality_df.where(F.col("is_quarantined") == True)

    (
        quality_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(QUALITY_STREAM_TABLE)
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(VALID_STREAM_TABLE)
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_STREAM_TABLE)
    )

    print(f"Completed assignment quality microbatch: {batch_id}")


standardized_assignment_stream = spark.readStream.table(STANDARDIZED_STREAM_TABLE)


query = (
    standardized_assignment_stream
    .writeStream
    .foreachBatch(process_assignment_quality_microbatch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("silver_assignment_event_quality_streaming")
    .start()
)

query.awaitTermination()

print(
    "Completed assignment event Structured Streaming quality routing. "
    f"Output tables: {QUALITY_STREAM_TABLE}, {VALID_STREAM_TABLE}, "
    f"{QUARANTINE_STREAM_TABLE}"
)