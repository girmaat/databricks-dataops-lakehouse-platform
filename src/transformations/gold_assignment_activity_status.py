"""
Create one business-readable Gold row per current Power BI assignment.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = "adb_classic_compute_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

EMPLOYEE_VALID_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.employee_snapshot_valid_dlt"
ASSIGNMENT_EVENT_VALID_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.assignment_event_valid_dlt"
USAGE_EVENT_VALID_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.application_usage_event_valid_dlt"

GOLD_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.assignment_activity_status"

REPORT_AS_OF_DATE = "2026-05-31"
RECENT_ACTIVITY_DAYS = 30

QUALIFYING_EVENT_TYPES = [
    "REPORT_VIEWED",
    "DASHBOARD_SHARED",
    "DATA_MODEL_AUTHORED",
    "PAGINATED_REPORT_PUBLISHED",
]

SIGN_IN_EVENT_TYPES = [
    "SIGN_IN",
]


def build_current_assignments(assignment_events_df):
    """
    Build current active assignments from trusted valid assignment events.

    The assignment source is event-based. One assignment_id can have more than
    one event over time. Gold should use the latest valid event per assignment_id.

    Ordering logic:
    1. operation_sequence
    2. effective_timestamp
    3. operation_timestamp
    4. assignment_event_id

    Latest ASSIGN or CHANGE_TIER means the assignment is active.
    Latest REVOKE means the assignment is not current and should not appear in Gold.
    """

    latest_assignment_window = Window.partitionBy("assignment_id").orderBy(
        F.col("operation_sequence").desc(),
        F.col("effective_timestamp").desc(),
        F.col("operation_timestamp").desc(),
        F.col("assignment_event_id").desc(),
    )

    current_assignments_df = (
        assignment_events_df
        .withColumn(
            "assignment_rank",
            F.row_number().over(latest_assignment_window),
        )
        .filter(F.col("assignment_rank") == 1)
        .withColumn(
            "current_assignment_status",
            F.when(
                F.col("operation_code").isin("ASSIGN", "CHANGE_TIER"),
                F.lit("ACTIVE"),
            )
            .when(
                F.col("operation_code") == "REVOKE",
                F.lit("REVOKED"),
            )
            .otherwise(F.lit("UNKNOWN")),
        )
        .filter(F.col("current_assignment_status") == "ACTIVE")
        .select(
            "assignment_id",
            "employee_id",
            "application_id",
            "license_tier_id",
            "current_assignment_status",
            F.col("assignment_event_id").alias("latest_assignment_event_id"),
            F.col("operation_code").alias("latest_operation_code"),
            F.col("operation_sequence").alias("latest_operation_sequence"),
            F.col("operation_timestamp").alias("latest_operation_timestamp"),
            F.col("effective_timestamp").alias("current_effective_timestamp"),
        )
    )

    return current_assignments_df


def build_usage_summary(usage_events_df):
    """
    Aggregate valid usage events to one row per employee_id and application_id.

    aggregate before joining to assignments so the Gold table stays one row
    per current assignment instead of one row per usage event.
    """

    usage_classified_df = (
        usage_events_df
        .withColumn(
            "is_qualifying_activity",
            F.col("event_type_code").isin(QUALIFYING_EVENT_TYPES),
        )
        .withColumn(
            "is_sign_in_only_activity",
            F.col("event_type_code").isin(SIGN_IN_EVENT_TYPES),
        )
    )

    usage_summary_df = (
        usage_classified_df
        .groupBy(
            "employee_id",
            "application_id",
        )
        .agg(
            F.max(
                F.when(
                    F.col("is_qualifying_activity"),
                    F.col("event_timestamp_utc"),
                )
            ).alias("last_qualifying_activity_timestamp_utc"),

            F.count(
                F.when(
                    F.col("is_qualifying_activity"),
                    F.lit(1),
                )
            ).alias("qualifying_activity_count"),

            F.count(
                F.when(
                    F.col("is_sign_in_only_activity"),
                    F.lit(1),
                )
            ).alias("sign_in_only_count"),

            F.count("*").alias("accepted_usage_event_count"),

            F.max("event_timestamp_utc").alias("last_any_activity_timestamp_utc"),
        )
    )

    return usage_summary_df


def build_gold_assignment_activity_status(
    current_assignments_df,
    employee_df,
    usage_summary_df,
):
    """
    Join current assignments, employee context, and summarized usage evidence.

    Then classify each assignment into one activity_status.
    """

    report_as_of_date = F.to_date(F.lit(REPORT_AS_OF_DATE))
    recent_cutoff_date = F.date_sub(report_as_of_date, RECENT_ACTIVITY_DAYS)

    gold_df = (
        current_assignments_df.alias("a")
        .join(
            employee_df.alias("e"),
            F.col("a.employee_id") == F.col("e.employee_id"),
            "left",
        )
        .join(
            usage_summary_df.alias("u"),
            (
                (F.col("a.employee_id") == F.col("u.employee_id"))
                & (F.col("a.application_id") == F.col("u.application_id"))
            ),
            "left",
        )
        .select(
            F.col("a.assignment_id"),
            F.col("a.employee_id"),
            F.col("e.user_principal_name"),
            F.col("e.employment_status"),
            F.col("e.department_id"),
            F.col("e.region_code"),
            F.col("a.application_id"),
            F.col("a.license_tier_id"),
            F.col("a.current_assignment_status"),
            F.col("a.latest_assignment_event_id"),
            F.col("a.latest_operation_code"),
            F.col("a.latest_operation_sequence"),
            F.col("a.latest_operation_timestamp"),
            F.col("a.current_effective_timestamp"),

            F.col("u.last_qualifying_activity_timestamp_utc"),
            F.coalesce(F.col("u.qualifying_activity_count"), F.lit(0)).alias(
                "qualifying_activity_count"
            ),
            F.coalesce(F.col("u.sign_in_only_count"), F.lit(0)).alias(
                "sign_in_only_count"
            ),
            F.coalesce(F.col("u.accepted_usage_event_count"), F.lit(0)).alias(
                "accepted_usage_event_count"
            ),
            F.col("u.last_any_activity_timestamp_utc"),

            report_as_of_date.alias("report_as_of_date"),
            recent_cutoff_date.alias("recent_activity_cutoff_date"),
        )
        .withColumn(
            "activity_status",
            F.when(
                F.col("employment_status") == "TERMINATED",
                F.lit("terminated_employee_review"),
            )
            .when(
                F.col("last_qualifying_activity_timestamp_utc") >= F.col(
                    "recent_activity_cutoff_date"
                ),
                F.lit("recent_qualifying_activity"),
            )
            .when(
                F.col("qualifying_activity_count") > 0,
                F.lit("older_qualifying_activity"),
            )
            .when(
                (F.col("qualifying_activity_count") == 0)
                & (F.col("sign_in_only_count") > 0),
                F.lit("sign_in_only_activity"),
            )
            .when(
                F.col("accepted_usage_event_count") == 0,
                F.lit("no_observed_activity"),
            )
            .otherwise(F.lit("unknown_or_review")),
        )
        .withColumn(
            "activity_status_reason",
            F.when(
                F.col("activity_status") == "terminated_employee_review",
                F.lit(
                    "Employee is terminated but has a current active assignment; review is required."
                ),
            )
            .when(
                F.col("activity_status") == "recent_qualifying_activity",
                F.concat(
                    F.lit("Latest qualifying activity is on or after recent cutoff date "),
                    F.col("recent_activity_cutoff_date").cast("string"),
                    F.lit("."),
                ),
            )
            .when(
                F.col("activity_status") == "older_qualifying_activity",
                F.concat(
                    F.lit("Qualifying activity exists, but latest qualifying activity is before "),
                    F.col("recent_activity_cutoff_date").cast("string"),
                    F.lit("."),
                ),
            )
            .when(
                F.col("activity_status") == "sign_in_only_activity",
                F.lit(
                    "Only sign-in activity was observed; no qualifying Power BI usage event was found."
                ),
            )
            .when(
                F.col("activity_status") == "no_observed_activity",
                F.lit(
                    "No accepted usage events were found for this employee and application assignment."
                ),
            )
            .otherwise(
                F.lit("Unexpected assignment/activity combination; review is required.")
            ),
        )
        .withColumn(
            "gold_processed_timestamp_utc",
            F.current_timestamp(),
        )
        .select(
            "assignment_id",
            "employee_id",
            "user_principal_name",
            "employment_status",
            "department_id",
            "region_code",
            "application_id",
            "license_tier_id",
            "current_assignment_status",
            "latest_assignment_event_id",
            "latest_operation_code",
            "latest_operation_sequence",
            "latest_operation_timestamp",
            "current_effective_timestamp",
            "last_qualifying_activity_timestamp_utc",
            "qualifying_activity_count",
            "sign_in_only_count",
            "accepted_usage_event_count",
            "last_any_activity_timestamp_utc",
            "activity_status",
            "activity_status_reason",
            "report_as_of_date",
            "recent_activity_cutoff_date",
            "gold_processed_timestamp_utc",
        )
    )

    return gold_df


print("Creating Gold schema if it does not exist...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

print(f"Reading trusted employee input from: {EMPLOYEE_VALID_TABLE}")
employee_valid_df = spark.table(EMPLOYEE_VALID_TABLE)

print(f"Reading trusted assignment input from: {ASSIGNMENT_EVENT_VALID_TABLE}")
assignment_event_valid_df = spark.table(ASSIGNMENT_EVENT_VALID_TABLE)

print(f"Reading trusted usage input from: {USAGE_EVENT_VALID_TABLE}")
usage_event_valid_df = spark.table(USAGE_EVENT_VALID_TABLE)

print("Building current active assignment set...")
current_assignments_df = build_current_assignments(assignment_event_valid_df)

print("Building usage summary...")
usage_summary_df = build_usage_summary(usage_event_valid_df)

print("Building Gold assignment activity status...")
gold_assignment_activity_status_df = build_gold_assignment_activity_status(
    current_assignments_df=current_assignments_df,
    employee_df=employee_valid_df,
    usage_summary_df=usage_summary_df,
)

print(f"Writing Gold table to: {GOLD_TABLE}")
(
    gold_assignment_activity_status_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

gold_count = spark.table(GOLD_TABLE).count()

print(f"Gold assignment activity status rows written: {gold_count}")
print("Gold assignment activity status processing complete.")