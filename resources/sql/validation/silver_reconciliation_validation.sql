SELECT
  'employee' AS dataset,
  'bronze' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.bronze.employee_directory_snapshot_raw_dlt

UNION ALL

SELECT
  'employee' AS dataset,
  'valid' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.silver.employee_snapshot_valid_dlt

UNION ALL

SELECT
  'employee' AS dataset,
  'quarantine' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.silver.employee_snapshot_quarantine_dlt

UNION ALL

SELECT
  'assignment' AS dataset,
  'bronze' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.bronze.license_assignment_change_event_raw_dlt

UNION ALL

SELECT
  'assignment' AS dataset,
  'valid' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.silver.assignment_event_valid_dlt

UNION ALL

SELECT
  'assignment' AS dataset,
  'quarantine' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.silver.assignment_event_quarantine_dlt

UNION ALL

SELECT
  'usage' AS dataset,
  'bronze' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.bronze.application_usage_event_raw_dlt

UNION ALL

SELECT
  'usage' AS dataset,
  'valid' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.silver.application_usage_event_valid_dlt

UNION ALL

SELECT
  'usage' AS dataset,
  'quarantine' AS layer,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.silver.application_usage_event_quarantine_dlt;