"""
Build Assignment Current and Assignment History Silver tables.

reads trusted valid assignment events from Silver and builds:
1. silver.assignment_history
2. silver.assignment_current
intentionally implemented as batch PySpark for the first current/history phase.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "adb_classic_compute_catalog"
SILVER_SCHEMA = "silver"

SOURCE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.assignment_event_valid_dlt"
HISTORY_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.assignment_history"
CURRENT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.assignment_current"



# Helper functions

def add_assignment_status_columns(df):
    """
    Add business status columns based on operation_code.
    """
    return (
        df
        .withColumn(
            "assignment_status_after_event",
            F.when(F.col("operation_code") == "ASSIGN", F.lit("ACTIVE"))
             .when(F.col("operation_code") == "CHANGE_TIER", F.lit("ACTIVE"))
             .when(F.col("operation_code") == "REVOKE", F.lit("REVOKED"))
             .otherwise(F.lit("UNKNOWN"))
        )
        .withColumn(
            "is_active_after_event",
            F.when(F.col("operation_code").isin("ASSIGN", "CHANGE_TIER"), F.lit(True))
             .when(F.col("operation_code") == "REVOKE", F.lit(False))
             .otherwise(F.lit(False))
        )
    )


def build_assignment_history(valid_events_df):
    """
    Build one lifecycle row per valid assignment event.

    For each assignment_id, events are ordered by:
    1. operation_sequence
    2. effective_timestamp
    3. operation_timestamp
    4. assignment_event_id

    """
    assignment_window = Window.partitionBy("assignment_id").orderBy(
        F.col("operation_sequence").asc(),
        F.col("effective_timestamp").asc(),
        F.col("operation_timestamp").asc(),
        F.col("assignment_event_id").asc()
    )

    reverse_assignment_window = Window.partitionBy("assignment_id").orderBy(
        F.col("operation_sequence").desc(),
        F.col("effective_timestamp").desc(),
        F.col("operation_timestamp").desc(),
        F.col("assignment_event_id").desc()
    )

    history_df = (
        valid_events_df
        .transform(add_assignment_status_columns)
        .withColumn(
            "next_effective_timestamp",
            F.lead("effective_timestamp").over(assignment_window)
        )
        .withColumn(
            "current_version_rank",
            F.row_number().over(reverse_assignment_window)
        )
        .withColumn(
            "is_current_version",
            F.when(F.col("current_version_rank") == 1, F.lit(True)).otherwise(F.lit(False))
        )
        .withColumn(
            "history_processed_timestamp_utc",
            F.current_timestamp()
        )
        .select(
            "assignment_id",
            "assignment_event_id",
            "employee_id",
            "application_id",
            "license_tier_id",
            "operation_code",
            "operation_sequence",
            "operation_timestamp",
            "effective_timestamp",
            "next_effective_timestamp",
            "is_current_version",
            "is_active_after_event",
            "assignment_status_after_event",
            "delivery_batch_id",
            "delivery_date",
            "source_file_name",
            "source_file_path",
            "history_processed_timestamp_utc"
        )
    )

    return history_df


def build_assignment_current(history_df):
    """
    Build the current active assignment table.

    - Keep only the latest event per assignment_id.
    - Keep it only if the latest event leaves the assignment active.
    - Exclude assignments where the latest event is REVOKE.

    """
    current_df = (
        history_df
        .filter(F.col("is_current_version") == True)
        .filter(F.col("is_active_after_event") == True)
        .select(
            "assignment_id",
            "employee_id",
            "application_id",
            "license_tier_id",
            F.col("assignment_status_after_event").alias("assignment_status"),
            F.col("effective_timestamp").alias("current_effective_timestamp"),
            F.col("assignment_event_id").alias("latest_assignment_event_id"),
            F.col("operation_code").alias("latest_operation_code"),
            F.col("operation_sequence").alias("latest_operation_sequence"),
            F.col("operation_timestamp").alias("latest_operation_timestamp"),
            "delivery_batch_id",
            "delivery_date",
            "source_file_name",
            "source_file_path"
        )
        .withColumn(
            "current_processed_timestamp_utc",
            F.current_timestamp()
        )
    )

    return current_df



# -Main processing-


print(f"Reading trusted Silver assignment events from: {SOURCE_TABLE}")

valid_assignment_events_df = spark.table(SOURCE_TABLE)

print("Building assignment history...")
assignment_history_df = build_assignment_history(valid_assignment_events_df)

print(f"Writing assignment history to: {HISTORY_TABLE}")
(
    assignment_history_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(HISTORY_TABLE)
)

print("Building assignment current...")
assignment_current_df = build_assignment_current(assignment_history_df)

print(f"Writing assignment current to: {CURRENT_TABLE}")
(
    assignment_current_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CURRENT_TABLE)
)

history_count = spark.table(HISTORY_TABLE).count()
current_count = spark.table(CURRENT_TABLE).count()

print(f"Assignment history rows written: {history_count}")
print(f"Assignment current rows written: {current_count}")
print("Assignment Current/History Silver processing complete.")