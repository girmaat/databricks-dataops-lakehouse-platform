import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F


CATALOG_NAME = "adb_classic_compute_catalog"
REFERENCE_TABLE = f"{CATALOG_NAME}.governance.department_region_reference"

SUPPORTED_EMPLOYMENT_STATUSES = ["ACTIVE", "TERMINATED"]

UPN_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


@dlt.table(
    name="employee_snapshot_quality_dlt",
    comment=(
        "Employee Silver quality table. Adds quality_reasons and is_quarantined flags to standardized employee records."
    ),
)
def employee_snapshot_quality_dlt():
    employee_df = dlt.read("employee_snapshot_standardized_dlt")

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
        .where(F.col("reference_is_active") == True)
        .dropDuplicates(["ref_department_id", "ref_region_code"])
    )

    joined_df = (
        employee_df.alias("emp")
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
                F.when(F.col("employee_id").isNull(), F.lit("missing_employee_id")),
                F.when(F.col("employee_id_occurrence_count") > 1, F.lit("duplicate_employee_id")),

                F.when(F.col("user_principal_name").isNull(), F.lit("missing_user_principal_name")),
                F.when(
                    F.col("user_principal_name").isNotNull()
                    & (~F.col("user_principal_name").rlike(UPN_REGEX)),
                    F.lit("invalid_user_principal_name"),
                ),

                F.when(F.col("employment_status").isNull(), F.lit("missing_employment_status")),
                F.when(
                    F.col("employment_status").isNotNull()
                    & (~F.col("employment_status").isin(SUPPORTED_EMPLOYMENT_STATUSES)),
                    F.lit("unsupported_employment_status"),
                ),

                F.when(F.col("hire_date").isNull(), F.lit("invalid_hire_date")),

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

                F.when(F.col("department_id").isNull(), F.lit("missing_department_id")),
                F.when(F.col("region_code").isNull(), F.lit("missing_region_code")),
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


@dlt.table(
    name="employee_snapshot_valid_dlt",
    comment="Valid employee Silver records after employee quality validation.",
)
def employee_snapshot_valid_dlt():
    return (
        dlt.read("employee_snapshot_quality_dlt")
        .where(F.col("is_quarantined") == False)
    )


@dlt.table(
    name="employee_snapshot_quarantine_dlt",
    comment="Quarantined employee Silver records with quality reasons.",
)
def employee_snapshot_quarantine_dlt():
    return (
        dlt.read("employee_snapshot_quality_dlt")
        .where(F.col("is_quarantined") == True)
    )