from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession


spark = SparkSession.builder.appName("silver_quality_reconciliation_summary").getOrCreate()

CATALOG = "adb_classic_compute_catalog"
SILVER_SCHEMA = "silver"

TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_quality_reconciliation_summary"

DATASETS = [
    {
        "dataset": "employee",
        "bronze_table": f"{CATALOG}.bronze.employee_directory_snapshot_raw_dlt",
        "valid_table": f"{CATALOG}.silver.employee_snapshot_valid_dlt",
        "quarantine_table": f"{CATALOG}.silver.employee_snapshot_quarantine_dlt",
    },
    {
        "dataset": "assignment",
        "bronze_table": f"{CATALOG}.bronze.license_assignment_change_event_raw_dlt",
        "valid_table": f"{CATALOG}.silver.assignment_event_valid_dlt",
        "quarantine_table": f"{CATALOG}.silver.assignment_event_quarantine_dlt",
    },
    {
        "dataset": "usage",
        "bronze_table": f"{CATALOG}.bronze.application_usage_event_raw_dlt",
        "valid_table": f"{CATALOG}.silver.application_usage_event_valid_dlt",
        "quarantine_table": f"{CATALOG}.silver.application_usage_event_quarantine_dlt",
    },
]


def get_table_count(table_name):
    return spark.table(table_name).count()


def build_reconciliation_rows():
    checked_at_utc = datetime.now(timezone.utc)
    rows = []

    for dataset_config in DATASETS:
        dataset = dataset_config["dataset"]
        bronze_table = dataset_config["bronze_table"]
        valid_table = dataset_config["valid_table"]
        quarantine_table = dataset_config["quarantine_table"]

        bronze_count = get_table_count(bronze_table)
        valid_count = get_table_count(valid_table)
        quarantine_count = get_table_count(quarantine_table)

        silver_outcome_count = valid_count + quarantine_count
        count_difference = bronze_count - silver_outcome_count

        if count_difference == 0:
            reconciliation_status = "PASS"
        else:
            reconciliation_status = "REVIEW"

        rows.append(
            Row(
                dataset=dataset,
                bronze_table=bronze_table,
                valid_table=valid_table,
                quarantine_table=quarantine_table,
                bronze_count=bronze_count,
                valid_count=valid_count,
                quarantine_count=quarantine_count,
                silver_outcome_count=silver_outcome_count,
                count_difference=count_difference,
                reconciliation_status=reconciliation_status,
                reconciliation_checked_at_utc=checked_at_utc,
            )
        )

    return rows


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

reconciliation_rows = build_reconciliation_rows()
reconciliation_df = spark.createDataFrame(reconciliation_rows)

reconciliation_df.write.mode("overwrite").format("delta").saveAsTable(TARGET_TABLE)

reconciliation_df.orderBy("dataset").show(truncate=False)