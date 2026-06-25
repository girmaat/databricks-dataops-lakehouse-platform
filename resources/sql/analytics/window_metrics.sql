USE CATALOG adb_classic_compute_catalog;
USE SCHEMA gold;

CREATE OR REPLACE VIEW assignment_activity_group_window_metrics_vw AS
WITH grouped AS (
  SELECT
    license_tier_id,
    department_id,
    region_code,
    COUNT(*) AS assignment_count,
    SUM(CASE WHEN is_recent_qualifying_activity_as_of_report_date THEN 1 ELSE 0 END)
      AS recent_qualifying_assignment_count,
    SUM(qualifying_activity_count) AS total_qualifying_activity_count
  FROM assignment_activity_report_as_of_vw
  GROUP BY
    license_tier_id,
    department_id,
    region_code
)
SELECT
  license_tier_id,
  department_id,
  region_code,
  assignment_count,
  recent_qualifying_assignment_count,
  total_qualifying_activity_count,

  ROW_NUMBER() OVER (
    PARTITION BY license_tier_id
    ORDER BY assignment_count DESC, department_id ASC, region_code ASC
  ) AS row_number_within_license_tier,

  RANK() OVER (
    PARTITION BY license_tier_id
    ORDER BY assignment_count DESC
  ) AS rank_within_license_tier,

  DENSE_RANK() OVER (
    PARTITION BY license_tier_id
    ORDER BY assignment_count DESC
  ) AS dense_rank_within_license_tier,

  SUM(assignment_count) OVER (
    PARTITION BY license_tier_id
    ORDER BY assignment_count DESC, department_id ASC, region_code ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_assignment_count_within_license_tier,

  SUM(assignment_count) OVER (
    PARTITION BY license_tier_id
  ) AS total_assignment_count_within_license_tier,

  ROUND(
    CAST(assignment_count AS DOUBLE)
    / NULLIF(SUM(assignment_count) OVER (PARTITION BY license_tier_id), 0),
    4
  ) AS assignment_percent_of_license_tier

FROM grouped;


CREATE OR REPLACE VIEW assignment_activity_assignment_window_metrics_vw AS
SELECT
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  qualifying_activity_count,
  last_qualifying_activity_timestamp_utc,
  days_since_last_qualifying_activity,

  ROW_NUMBER() OVER (
    PARTITION BY employee_id
    ORDER BY
      last_qualifying_activity_timestamp_utc DESC NULLS LAST,
      assignment_id ASC
  ) AS employee_assignment_recency_row_number,

  DENSE_RANK() OVER (
    PARTITION BY region_code
    ORDER BY qualifying_activity_count DESC
  ) AS assignment_activity_dense_rank_within_region,

  SUM(qualifying_activity_count) OVER (
    PARTITION BY employee_id
    ORDER BY
      last_qualifying_activity_timestamp_utc ASC NULLS LAST,
      assignment_id ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_qualifying_activity_count_by_employee

FROM assignment_activity_report_as_of_vw;


CREATE OR REPLACE VIEW assignment_activity_5_minute_bucket_vw AS
SELECT
  license_tier_id,
  department_id,
  region_code,
  activity_window.start AS bucket_start_timestamp_utc,
  activity_window.end AS bucket_end_timestamp_utc,
  COUNT(*) AS assignment_count_in_bucket,
  SUM(qualifying_activity_count) AS qualifying_activity_count_in_bucket
FROM (
  SELECT
    license_tier_id,
    department_id,
    region_code,
    qualifying_activity_count,
    window(
      last_any_activity_timestamp_utc,
      '5 minutes',
      '5 minutes',
      '2 minutes'
    ) AS activity_window
  FROM assignment_activity_report_as_of_vw
  WHERE last_any_activity_timestamp_utc IS NOT NULL
) bucketed
GROUP BY
  license_tier_id,
  department_id,
  region_code,
  activity_window.start,
  activity_window.end;