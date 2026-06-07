from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


def is_blank(column_name: str) -> Column:
    """
    Returns true when a column is NULL or only whitespace.

    This keeps missing-value checks consistent across employee,
    assignment, and usage quality pipelines.
    """
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


def is_not_blank(column_name: str) -> Column:
    """
    Returns true when a column has a non-empty value.
    """
    return ~is_blank(column_name)


def add_quality_columns(df: DataFrame, rules: list[tuple[str, Column]]) -> DataFrame:
    """
    Applies reusable Silver quality rules.

    Input:
        df:
            DataFrame to classify.

        rules:
            List of tuples:
                ("reason_name", condition)

            Example:
                ("missing_employee_id", is_blank("employee_id"))

    Output columns added:
        quality_reasons:
            ARRAY<STRING> containing all failed rule names.

        quality_reason_text:
            Human-readable semicolon-separated version of quality_reasons.
            This is useful for SQL GROUP BY evidence queries.

        is_quarantined:
            True when one or more rules failed.
    """
    reason_columns = [
        F.when(condition, F.lit(reason_name)).otherwise(F.lit(None))
        for reason_name, condition in rules
    ]

    return (
        df
        .withColumn("quality_reasons_raw", F.array(*reason_columns))
        .withColumn(
            "quality_reasons",
            F.expr("filter(quality_reasons_raw, reason -> reason is not null)")
        )
        .withColumn(
            "quality_reason_text",
            F.array_join(F.col("quality_reasons"), "; ")
        )
        .withColumn(
            "is_quarantined",
            F.size(F.col("quality_reasons")) > 0
        )
        .drop("quality_reasons_raw")
    )


def valid_rows(df: DataFrame) -> DataFrame:
    """
    Keeps only accepted Silver rows.
    """
    return df.where(F.col("is_quarantined") == F.lit(False))


def quarantine_rows(df: DataFrame) -> DataFrame:
    """
    Keeps only rejected Silver rows.
    """
    return df.where(F.col("is_quarantined") == F.lit(True))