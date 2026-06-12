USE CATALOG adb_classic_compute_catalog;
USE SCHEMA gold;

CREATE OR REPLACE TABLE adb_classic_compute_catalog.gold.assignment_activity_kpi_snapshot AS
SELECT
  license_tier_id,
  department_id,
  region_code,

  COUNT(*) AS assignment_count,

  SUM(CASE WHEN activity_status = 'recent_qualifying_activity' THEN 1 ELSE 0 END)
    AS recent_qualifying_assignment_count,

  SUM(CASE WHEN activity_status = 'older_qualifying_activity' THEN 1 ELSE 0 END)
    AS older_qualifying_assignment_count,

  SUM(CASE WHEN activity_status = 'sign_in_only_activity' THEN 1 ELSE 0 END)
    AS sign_in_only_assignment_count,

  SUM(CASE WHEN activity_status = 'no_observed_activity' THEN 1 ELSE 0 END)
    AS no_observed_activity_assignment_count,

  SUM(CASE WHEN activity_status = 'terminated_employee_review' THEN 1 ELSE 0 END)
    AS terminated_employee_review_count,

  ROUND(
    100.0 * SUM(CASE WHEN activity_status = 'recent_qualifying_activity' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
  ) AS recent_activity_pct,

  current_timestamp() AS snapshot_created_at_utc
FROM adb_classic_compute_catalog.gold.assignment_activity_status
GROUP BY
  license_tier_id,
  department_id,
  region_code;

