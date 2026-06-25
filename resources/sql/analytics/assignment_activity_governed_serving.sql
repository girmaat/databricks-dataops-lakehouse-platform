SELECT
  employee_id,
  masked_user_principal_name,
  full_user_principal_name_for_admin_review,
  activity_status,
  region_code,
  department_id
FROM adb_classic_compute_catalog.gold.assignment_activity_governed_vw
ORDER BY employee_id;