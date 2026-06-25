USE CATALOG adb_classic_compute_catalog;
USE SCHEMA gold;

CREATE OR REPLACE VIEW assignment_activity_report_as_of_vw AS
WITH report_control AS (
  SELECT
    DATE '2026-05-15' AS report_as_of_date
),
base AS (
  SELECT
    assignment_id,
    employee_id,
    department_id,
    region_code,
    license_tier_id,
    activity_status,
    last_qualifying_activity_timestamp_utc,
    last_any_activity_timestamp_utc,
    qualifying_activity_count,
    latest_operation_code,
    latest_operation_timestamp,
    latest_assignment_event_id,
    gold_processed_timestamp_utc
  FROM assignment_activity_status
)
SELECT
  b.assignment_id,
  b.employee_id,
  b.department_id,
  b.region_code,
  b.license_tier_id,
  b.activity_status,
  b.last_qualifying_activity_timestamp_utc,
  b.last_any_activity_timestamp_utc,
  b.qualifying_activity_count,
  b.latest_operation_code,
  b.latest_operation_timestamp,
  b.latest_assignment_event_id,
  b.gold_processed_timestamp_utc,
  r.report_as_of_date,

  DATEDIFF(
    r.report_as_of_date,
    TO_DATE(b.last_qualifying_activity_timestamp_utc)
  ) AS days_since_last_qualifying_activity,

  CASE
    WHEN b.last_qualifying_activity_timestamp_utc IS NULL
      THEN 'NO_QUALIFYING_ACTIVITY'
    WHEN DATEDIFF(r.report_as_of_date, TO_DATE(b.last_qualifying_activity_timestamp_utc)) <= 7
      THEN 'RECENT_0_7_DAYS'
    WHEN DATEDIFF(r.report_as_of_date, TO_DATE(b.last_qualifying_activity_timestamp_utc)) <= 30
      THEN 'RECENT_8_30_DAYS'
    WHEN DATEDIFF(r.report_as_of_date, TO_DATE(b.last_qualifying_activity_timestamp_utc)) <= 90
      THEN 'OLDER_31_90_DAYS'
    ELSE 'OLDER_THAN_90_DAYS'
  END AS recency_bucket,

  CASE
    WHEN b.last_qualifying_activity_timestamp_utc IS NOT NULL
     AND DATEDIFF(r.report_as_of_date, TO_DATE(b.last_qualifying_activity_timestamp_utc)) <= 30
      THEN TRUE
    ELSE FALSE
  END AS is_recent_qualifying_activity_as_of_report_date

FROM base b
CROSS JOIN report_control r;


SELECT *
FROM assignment_activity_report_as_of_vw
ORDER BY assignment_id;


CREATE OR REPLACE VIEW dim_license_tier_vw AS
SELECT
  license_tier_id,

  CASE
    WHEN license_tier_id = 'PBI-PRO' THEN 'Power BI Pro'
    WHEN license_tier_id = 'PBI-PPU' THEN 'Power BI Premium Per User'
    ELSE 'Unknown License Tier'
  END AS license_tier_name,

  CASE
    WHEN license_tier_id = 'PBI-PRO' THEN 'Standard self-service BI entitlement'
    WHEN license_tier_id = 'PBI-PPU' THEN 'Premium-per-user BI entitlement'
    ELSE 'Unmapped entitlement'
  END AS license_tier_description,

  COUNT(*) AS assignment_count
FROM assignment_activity_report_as_of_vw
GROUP BY license_tier_id;


CREATE OR REPLACE VIEW dim_department_region_vw AS
SELECT
  department_id,
  region_code,
  CONCAT(department_id, ' / ', region_code) AS department_region_name,
  COUNT(DISTINCT employee_id) AS employee_count,
  COUNT(DISTINCT assignment_id) AS assignment_count,
  SUM(CASE WHEN is_recent_qualifying_activity_as_of_report_date THEN 1 ELSE 0 END)
    AS recent_qualifying_assignment_count
FROM assignment_activity_report_as_of_vw
GROUP BY
  department_id,
  region_code;


CREATE OR REPLACE VIEW dim_assignment_vw AS
SELECT
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  report_as_of_date,
  days_since_last_qualifying_activity,
  is_recent_qualifying_activity_as_of_report_date
FROM assignment_activity_report_as_of_vw;


