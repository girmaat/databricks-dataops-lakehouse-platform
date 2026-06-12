USE CATALOG adb_classic_compute_catalog;
USE SCHEMA gold;

CREATE OR REPLACE VIEW adb_classic_compute_catalog.gold.assignment_activity_status_serving_vw AS
SELECT
  assignment_id,
  employee_id,
  application_id,
  license_tier_id,
  department_id,
  region_code,
  employment_status,
  activity_status,

  CASE
    WHEN activity_status = 'recent_qualifying_activity' THEN 'Recently used'
    WHEN activity_status = 'older_qualifying_activity' THEN 'Used, but not recently'
    WHEN activity_status = 'sign_in_only_activity' THEN 'Signed in only'
    WHEN activity_status = 'no_observed_activity' THEN 'No observed activity'
    WHEN activity_status = 'terminated_employee_review' THEN 'Terminated employee review'
    ELSE 'Needs review'
  END AS activity_status_label,

  CASE
    WHEN activity_status = 'recent_qualifying_activity' THEN 1
    ELSE 0
  END AS is_recently_active,

  CASE
    WHEN activity_status = 'terminated_employee_review' THEN 1
    ELSE 0
  END AS requires_access_review,

  last_qualifying_activity_timestamp_utc,
  last_any_activity_timestamp_utc,
  qualifying_activity_count,

  latest_operation_code,
  latest_operation_timestamp,
  latest_assignment_event_id,

  gold_processed_timestamp_utc,
  current_timestamp() AS served_at_utc
FROM adb_classic_compute_catalog.gold.assignment_activity_status;


CREATE OR REPLACE VIEW adb_classic_compute_catalog.gold.assignment_activity_kpi_vw AS
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

  current_timestamp() AS served_at_utc
FROM adb_classic_compute_catalog.gold.assignment_activity_status
GROUP BY
  license_tier_id,
  department_id,
  region_code;