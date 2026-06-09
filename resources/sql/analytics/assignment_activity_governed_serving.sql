USE CATALOG adb_classic_compute_catalog;
USE SCHEMA gold;

CREATE OR REPLACE VIEW adb_classic_compute_catalog.gold.assignment_activity_governed_vw AS
SELECT
  g.assignment_id,
  g.employee_id,

  CASE
    WHEN e.user_principal_name IS NULL THEN NULL
    WHEN instr(e.user_principal_name, '@') > 1 THEN
      concat(
        substr(e.user_principal_name, 1, 1),
        '***',
        substr(e.user_principal_name, instr(e.user_principal_name, '@'))
      )
    ELSE '***'
  END AS masked_user_principal_name,

  CAST(NULL AS STRING)
    AS full_user_principal_name_for_admin_review,

  g.application_id,
  g.license_tier_id,
  g.department_id,
  g.region_code,
  g.employment_status,
  g.activity_status,

  CASE
    WHEN g.activity_status = 'recent_qualifying_activity' THEN 'Recently used'
    WHEN g.activity_status = 'older_qualifying_activity' THEN 'Used, but not recently'
    WHEN g.activity_status = 'sign_in_only_activity' THEN 'Signed in only'
    WHEN g.activity_status = 'no_observed_activity' THEN 'No observed activity'
    WHEN g.activity_status = 'terminated_employee_review' THEN 'Terminated employee review'
    ELSE 'Needs review'
  END AS activity_status_label,

  CASE
    WHEN g.activity_status = 'recent_qualifying_activity' THEN 1
    ELSE 0
  END AS is_recently_active,

  CASE
    WHEN g.activity_status = 'terminated_employee_review' THEN 1
    ELSE 0
  END AS requires_access_review,

  g.last_qualifying_activity_timestamp_utc,
  g.last_any_activity_timestamp_utc,
  g.qualifying_activity_count,

  g.latest_operation_code,
  g.latest_operation_timestamp,
  g.latest_assignment_event_id,

  g.gold_processed_timestamp_utc,
  current_timestamp() AS served_at_utc

FROM adb_classic_compute_catalog.gold.assignment_activity_status AS g
LEFT JOIN adb_classic_compute_catalog.silver.employee_snapshot_valid_dlt AS e
  ON g.employee_id = e.employee_id;

GRANT USE CATALOG ON CATALOG adb_classic_compute_catalog TO `account users`;

GRANT USE SCHEMA ON SCHEMA adb_classic_compute_catalog.gold TO `account users`;

GRANT SELECT ON VIEW adb_classic_compute_catalog.gold.assignment_activity_status_serving_vw TO `account users`;

GRANT SELECT ON VIEW adb_classic_compute_catalog.gold.assignment_activity_kpi_vw TO `account users`;

GRANT SELECT ON TABLE adb_classic_compute_catalog.gold.assignment_activity_kpi_snapshot TO `account users`;

GRANT SELECT ON VIEW adb_classic_compute_catalog.gold.assignment_activity_governed_vw TO `account users`;