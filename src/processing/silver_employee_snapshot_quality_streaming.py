from pyspark.sql import Window
from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"

STANDARDIZED_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_standardized_stream"
)

REFERENCE_TABLE = (
    f"{CATALOG_NAME}.governance.department_region_reference"
)

QUALITY_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_quality_stream"
)

VALID_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_valid_stream"
)

QUARANTINE_STREAM_TABLE = (
    f"{CATALOG_NAME}.silver.employee_snapshot_quarantine_stream"
)

CHECKPOINT_PATH = (
    "/Volumes/adb_classic_compute_catalog/monitoring/checkpoints/"
    "silver_employee_snapshot_quality_streaming"
)



# Quality-rule constants

SUPPORTED_EMPLOYMENT_STATUSES = ["ACTIVE", "TERMINATED"]

UPN_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"



# Helper: load active department-region reference values

def load_active_department_region_reference():
    """
    Load department/region reference data.

    This table is used to validate whether an employee's department_id
    and region_code are recognized by the governed reference data.

    is_active is cast to string before comparison so this works whether
    the column is stored as BOOLEAN true or STRING 'true'.
    """
    reference_df = (
        spark.read.table(REFERENCE_TABLE)
        .select(
            F.upper(F.trim(F.col("department_id"))).alias("ref_department_id"),
            F.upper(F.trim(F.col("region_code"))).alias("ref_region_code"),
            F.col("department_name"),
            F.col("business_unit"),
            F.col("region_name"),
            F.col("region_group"),
            F.col("is_active").alias("reference_is_active"),
        )
        .where(F.lower(F.col("reference_is_active").cast("string")) == F.lit("true"))
        .dropDuplicates(["ref_department_id", "ref_region_code"])
    )

    return reference_df



# Helper: apply employee quality rules

def apply_employee_quality_rules(standardized_df):
    """
    Add quality_reasons and is_quarantined to standardized employee rows.

    This function does not drop rows.
    Every standardized row continues into the quality table.
    Rows with one or more quality reasons are later routed to quarantine.
    Rows with no quality reasons are routed to valid.
    """

    reference_df = load_active_department_region_reference()

    joined_df = (
        standardized_df.alias("emp")
        .join(
            reference_df.alias("ref"),
            (
                (F.col("emp.department_id") == F.col("ref.ref_department_id"))
                & (F.col("emp.region_code") == F.col("ref.ref_region_code"))
            ),
            "left",
        )
    )

    duplicate_window = Window.partitionBy("employee_id")

    quality_df = (
        joined_df
        .withColumn(
            "employee_id_occurrence_count",
            F.when(
                F.col("employee_id").isNotNull(),
                F.count("*").over(duplicate_window),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "quality_reasons_raw",
            F.array(
                # Employee identifier rules
                F.when(
                    F.col("employee_id").isNull(),
                    F.lit("missing_employee_id"),
                ),
                F.when(
                    F.col("employee_id_occurrence_count") > 1,
                    F.lit("duplicate_employee_id"),
                ),

                # User principal name rules
                F.when(
                    F.col("user_principal_name").isNull(),
                    F.lit("missing_user_principal_name"),
                ),
                F.when(
                    F.col("user_principal_name").isNotNull()
                    & (~F.col("user_principal_name").rlike(UPN_REGEX)),
                    F.lit("invalid_user_principal_name"),
                ),

                # Employment status rules
                F.when(
                    F.col("employment_status").isNull(),
                    F.lit("missing_employment_status"),
                ),
                F.when(
                    F.col("employment_status").isNotNull()
                    & (~F.col("employment_status").isin(SUPPORTED_EMPLOYMENT_STATUSES)),
                    F.lit("unsupported_employment_status"),
                ),

                # Date rules
                F.when(
                    F.col("hire_date").isNull(),
                    F.lit("invalid_hire_date"),
                ),
                F.when(
                    F.col("termination_date").isNull()
                    & (F.col("employment_status") == F.lit("TERMINATED")),
                    F.lit("terminated_employee_missing_termination_date"),
                ),
                F.when(
                    F.col("termination_date").isNotNull()
                    & (F.col("employment_status") == F.lit("ACTIVE")),
                    F.lit("active_employee_has_termination_date"),
                ),
                F.when(
                    F.col("termination_date").isNotNull()
                    & F.col("hire_date").isNotNull()
                    & (F.col("termination_date") < F.col("hire_date")),
                    F.lit("invalid_termination_date"),
                ),

                # Department/region reference rules
                F.when(
                    F.col("department_id").isNull(),
                    F.lit("missing_department_id"),
                ),
                F.when(
                    F.col("region_code").isNull(),
                    F.lit("missing_region_code"),
                ),
                F.when(
                    F.col("department_id").isNotNull()
                    & F.col("region_code").isNotNull()
                    & F.col("ref_department_id").isNull(),
                    F.lit("unknown_department_region_reference"),
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



# foreachBatch processor

def process_employee_quality_microbatch(batch_df, batch_id: int):
    """
    Process one Structured Streaming microbatch.

    Important design choice:
    Instead of using only batch_df, this function reads the full current
    standardized stream table as a batch table.

    Why?
    Employee snapshot quality rules, especially duplicate_employee_id,
    should be evaluated across the whole current snapshot, not only across
    one microbatch.
    """

    print(f"Starting employee quality microbatch: {batch_id}")

    full_standardized_df = spark.read.table(STANDARDIZED_STREAM_TABLE)

    quality_df = apply_employee_quality_rules(full_standardized_df)

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

    print(f"Completed employee quality microbatch: {batch_id}")



# Read standardized employee table as a stream

standardized_employee_stream = spark.readStream.table(STANDARDIZED_STREAM_TABLE)



# Trigger quality processing

query = (
    standardized_employee_stream
    .writeStream
    .foreachBatch(process_employee_quality_microbatch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("silver_employee_snapshot_quality_streaming")
    .start()
)

query.awaitTermination()

print(
    "Completed employee snapshot Structured Streaming quality routing. "
    f"Output tables: {QUALITY_STREAM_TABLE}, {VALID_STREAM_TABLE}, "
    f"{QUARANTINE_STREAM_TABLE}"
)