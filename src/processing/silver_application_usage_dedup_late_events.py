from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = "adb_classic_compute_catalog"
SILVER_SCHEMA = "silver"

SOURCE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.application_usage_event_valid_dlt"
DEDUP_TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.application_usage_event_deduplicated"
LATE_REJECT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.application_usage_event_late_rejects"

REPORT_AS_OF_DATE = "2026-05-21"
ACCEPTED_LATENESS_DAYS = 14

QUALIFYING_EVENT_TYPES = [
    "REPORT_VIEWED",
    "DASHBOARD_SHARED",
    "DATA_MODEL_AUTHORED",
    "PAGINATED_REPORT_PUBLISHED",
]


def accepted_late_threshold():
    report_date = datetime.strptime(REPORT_AS_OF_DATE, "%Y-%m-%d")
    threshold_date = report_date - timedelta(days=ACCEPTED_LATENESS_DAYS)
    return threshold_date.strftime("%Y-%m-%d 00:00:00")


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

usage_df = spark.table(SOURCE_TABLE)

usage_prepared_df = (
    usage_df
    .withColumn(
        "activity_dedup_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("employee_id"), F.lit("")),
                F.coalesce(F.col("application_id"), F.lit("")),
                F.coalesce(F.col("event_type_code"), F.lit("")),
                F.coalesce(F.col("event_timestamp_utc").cast("string"), F.lit("")),
                F.coalesce(F.col("event_units").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn(
        "accepted_late_threshold_timestamp",
        F.to_timestamp(F.lit(accepted_late_threshold())),
    )
    .withColumn(
        "is_very_late",
        F.col("event_timestamp_utc") < F.col("accepted_late_threshold_timestamp"),
    )
    .withColumn(
        "is_qualifying_activity",
        F.col("event_type_code").isin(QUALIFYING_EVENT_TYPES),
    )
    .withColumn(
        "is_sign_in_activity",
        F.col("event_type_code") == F.lit("SIGN_IN"),
    )
)

late_rejects_df = (
    usage_prepared_df
    .where(F.col("is_very_late") == True)
    .withColumn(
        "late_reject_reason",
        F.lit("event_older_than_s04_05_accepted_lateness_window"),
    )
    .withColumn("rejected_timestamp_utc", F.current_timestamp())
)

accepted_candidate_df = (
    usage_prepared_df
    .where(F.col("is_very_late") == False)
)

usage_event_id_window = Window.partitionBy("usage_event_id").orderBy(
    F.col("ingestion_timestamp_utc").asc_nulls_last(),
    F.col("source_file_name").asc_nulls_last(),
)

activity_window = Window.partitionBy("activity_dedup_key").orderBy(
    F.col("ingestion_timestamp_utc").asc_nulls_last(),
    F.col("source_file_name").asc_nulls_last(),
)

deduplicated_df = (
    accepted_candidate_df
    .withColumn("usage_event_id_rank", F.row_number().over(usage_event_id_window))
    .where(F.col("usage_event_id_rank") == 1)
    .drop("usage_event_id_rank")
    .withColumn("activity_dedup_rank", F.row_number().over(activity_window))
    .where(F.col("activity_dedup_rank") == 1)
    .drop("activity_dedup_rank")
    .withColumn("s04_05_processed_timestamp_utc", F.current_timestamp())
)

(
    deduplicated_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DEDUP_TARGET_TABLE)
)

(
    late_rejects_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(LATE_REJECT_TABLE)
)

print(f"Wrote deduplicated usage table: {DEDUP_TARGET_TABLE}")
print(f"Wrote late reject table: {LATE_REJECT_TABLE}")

print(f"Deduplicated usage rows: {spark.table(DEDUP_TARGET_TABLE).count()}")
print(f"Very-late rejected rows: {spark.table(LATE_REJECT_TABLE).count()}")

display(
    spark.table(DEDUP_TARGET_TABLE)
    .orderBy("employee_id", "event_timestamp_utc", "usage_event_id")
)

display(
    spark.table(LATE_REJECT_TABLE)
    .orderBy("event_timestamp_utc", "usage_event_id")
)