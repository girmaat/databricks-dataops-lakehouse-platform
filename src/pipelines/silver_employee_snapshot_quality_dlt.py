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
REFERENCE_TABLE = f"{CATALOG_NAME}.governance.department_region_reference"

SUPPORTED_EMPLOYMENT_STATUSES = ["ACTIVE", "TERMINATED"]

UPN_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


@dlt.table(
    name="employee_snapshot_quality_dlt",
    comment=(
        "Employee Silver quality table. Applies reusable S05.02 quality rules, "
        "adds quality_reasons, quality_reason_text, and is_quarantined."
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

    employee_with_context_df = (
        joined_df
        .withColumn(
            "employee_id_occurrence_count",
            F.when(
                F.col("employee_id").isNotNull(),
                F.count("*").over(duplicate_window),
            ).otherwise(F.lit(None)),
        )
    )

    rules = [
        ("missing_employee_id", is_blank("employee_id")),
        ("duplicate_employee_id", F.col("employee_id_occurrence_count") > 1),

        ("missing_user_principal_name", is_blank("user_principal_name")),
        (
            "invalid_user_principal_name",
            F.col("user_principal_name").isNotNull()
            & (~F.col("user_principal_name").rlike(UPN_REGEX)),
        ),

        ("missing_employment_status", is_blank("employment_status")),
        (
            "unsupported_employment_status",
            F.col("employment_status").isNotNull()
            & (~F.col("employment_status").isin(SUPPORTED_EMPLOYMENT_STATUSES)),
        ),

        ("invalid_hire_date", F.col("hire_date").isNull()),

        (
            "terminated_employee_missing_termination_date",
            F.col("termination_date").isNull()
            & (F.col("employment_status") == F.lit("TERMINATED")),
        ),
        (
            "active_employee_has_termination_date",
            F.col("termination_date").isNotNull()
            & (F.col("employment_status") == F.lit("ACTIVE")),
        ),
        (
            "invalid_termination_date",
            F.col("termination_date").isNotNull()
            & F.col("hire_date").isNotNull()
            & (F.col("termination_date") < F.col("hire_date")),
        ),

        ("missing_department_id", is_blank("department_id")),
        ("missing_region_code", is_blank("region_code")),
        (
            "unknown_department_region_reference",
            F.col("department_id").isNotNull()
            & F.col("region_code").isNotNull()
            & F.col("ref_department_id").isNull(),
        ),
    ]

    return add_quality_columns(employee_with_context_df, rules)


@dlt.table(
    name="employee_snapshot_valid_dlt",
    comment="Valid employee Silver records after reusable S05.02 employee quality validation.",
)
def employee_snapshot_valid_dlt():
    return valid_rows(dlt.read("employee_snapshot_quality_dlt"))


@dlt.table(
    name="employee_snapshot_quarantine_dlt",
    comment="Quarantined employee Silver records with reusable S05.02 quality reasons.",
)
def employee_snapshot_quarantine_dlt():
    return quarantine_rows(dlt.read("employee_snapshot_quality_dlt"))