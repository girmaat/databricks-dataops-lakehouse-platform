-- ============================================================
-- Performance Layout Optimization Lab
-- Purpose:
--   Compare unoptimized and Liquid Clustered Delta layouts
--   for assignment activity-history reporting queries.
--
-- Main concepts:
--   - Delta file pruning
--   - Data skipping
--   - Liquid Clustering
--   - Manual OPTIMIZE
--   - Small-file compaction
--   - Optimized writes vs auto compaction vs manual OPTIMIZE
-- ============================================================


-- ============================================================
-- 0. Setup
-- ============================================================

USE CATALOG adb_classic_compute_catalog;

CREATE SCHEMA IF NOT EXISTS perf_lab;

USE SCHEMA perf_lab;

SET use_cached_result = false;


-- ============================================================
-- 1. Confirm source Gold reporting data
-- ============================================================

SELECT
  license_tier_id,
  department_id,
  region_code,
  COUNT(*) AS assignment_count,
  SUM(CASE WHEN activity_status = 'recent_qualifying_activity' THEN 1 ELSE 0 END) AS recent_qualifying_assignment_count,
  SUM(CASE WHEN activity_status = 'older_qualifying_activity' THEN 1 ELSE 0 END) AS older_qualifying_assignment_count,
  SUM(CASE WHEN activity_status = 'sign_in_only_activity' THEN 1 ELSE 0 END) AS sign_in_only_assignment_count,
  SUM(CASE WHEN activity_status = 'no_observed_activity' THEN 1 ELSE 0 END) AS no_observed_activity_assignment_count,
  SUM(CASE WHEN activity_status = 'terminated_employee_review' THEN 1 ELSE 0 END) AS terminated_employee_review_count
FROM adb_classic_compute_catalog.gold.assignment_activity_status
GROUP BY
  license_tier_id,
  department_id,
  region_code
ORDER BY
  license_tier_id,
  department_id,
  region_code;


SELECT
  report_as_of_date,
  license_tier_id,
  department_id,
  region_code,
  recency_bucket,
  COUNT(*) AS assignment_count,
  SUM(CASE WHEN is_recent_qualifying_activity_as_of_report_date THEN 1 ELSE 0 END) AS recent_assignment_count
FROM adb_classic_compute_catalog.gold.assignment_activity_report_as_of_vw
GROUP BY
  report_as_of_date,
  license_tier_id,
  department_id,
  region_code,
  recency_bucket
ORDER BY
  report_as_of_date,
  license_tier_id,
  department_id,
  region_code,
  recency_bucket;


-- ============================================================
-- 2. Create wide unclustered baseline table
-- ============================================================

DROP TABLE IF EXISTS adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab;

CREATE TABLE adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab
USING DELTA
AS
WITH base_activity AS (
  SELECT
    assignment_id,
    employee_id,
    department_id,
    region_code,
    license_tier_id,
    activity_status,
    report_as_of_date,
    recency_bucket,
    days_since_last_qualifying_activity,
    is_recent_qualifying_activity_as_of_report_date
  FROM adb_classic_compute_catalog.gold.assignment_activity_report_as_of_vw
  WHERE report_as_of_date IS NOT NULL
    AND license_tier_id IS NOT NULL
),
scale_seed AS (
  SELECT
    id AS scale_id
  FROM RANGE(0, 1000000)
),
wide_activity_history AS (
  SELECT
    CONCAT(base_activity.assignment_id, '-WIDE-', CAST(scale_seed.scale_id AS STRING)) AS activity_history_lab_id,

    base_activity.assignment_id,
    base_activity.employee_id,
    base_activity.department_id,
    base_activity.region_code,
    base_activity.license_tier_id,
    base_activity.activity_status,
    base_activity.recency_bucket,

    DATE_ADD(
      base_activity.report_as_of_date,
      -1 * CAST(PMOD(scale_seed.scale_id, 365) AS INT)
    ) AS activity_date,

    base_activity.report_as_of_date,
    base_activity.days_since_last_qualifying_activity,
    base_activity.is_recent_qualifying_activity_as_of_report_date,

    CAST(scale_seed.scale_id AS BIGINT) AS scale_iteration_id,

    CASE
      WHEN PMOD(scale_seed.scale_id, 4) = 0 THEN 'REPORT_VIEWED'
      WHEN PMOD(scale_seed.scale_id, 4) = 1 THEN 'DASHBOARD_SHARED'
      WHEN PMOD(scale_seed.scale_id, 4) = 2 THEN 'SIGN_IN'
      ELSE 'DATA_MODEL_AUTHORED'
    END AS synthetic_event_type_code,

    CASE
      WHEN base_activity.is_recent_qualifying_activity_as_of_report_date THEN 1
      ELSE 0
    END AS synthetic_qualifying_activity_count,

    CONCAT(
      'payload=activity-history-layout-lab|',
      'assignment_id=', base_activity.assignment_id, '|',
      'employee_id=', base_activity.employee_id, '|',
      'department_id=', COALESCE(base_activity.department_id, 'UNKNOWN'), '|',
      'region_code=', COALESCE(base_activity.region_code, 'UNKNOWN'), '|',
      'license_tier_id=', base_activity.license_tier_id, '|',
      'scale_id=', CAST(scale_seed.scale_id AS STRING), '|',
      SHA2(CONCAT(base_activity.assignment_id, '-', CAST(scale_seed.scale_id AS STRING)), 256), '|',
      SHA2(CONCAT(base_activity.employee_id, '-', CAST(scale_seed.scale_id AS STRING), '-A'), 256), '|',
      SHA2(CONCAT(base_activity.employee_id, '-', CAST(scale_seed.scale_id AS STRING), '-B'), 256), '|',
      SHA2(CONCAT(base_activity.employee_id, '-', CAST(scale_seed.scale_id AS STRING), '-C'), 256)
    ) AS synthetic_payload,

    CURRENT_TIMESTAMP() AS lab_created_at_utc
  FROM base_activity
  CROSS JOIN scale_seed
)
SELECT
  *
FROM wide_activity_history
DISTRIBUTE BY
  activity_date,
  license_tier_id,
  department_id,
  region_code;


-- ============================================================
-- 3. Create Liquid Clustered comparison table
-- ============================================================

DROP TABLE IF EXISTS adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;

CREATE TABLE adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab
USING DELTA
CLUSTER BY (
  activity_date,
  license_tier_id,
  department_id,
  region_code
)
AS
SELECT
  activity_history_lab_id,
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  activity_date,
  report_as_of_date,
  days_since_last_qualifying_activity,
  is_recent_qualifying_activity_as_of_report_date,
  scale_iteration_id,
  synthetic_event_type_code,
  synthetic_qualifying_activity_count,
  synthetic_payload,
  lab_created_at_utc
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab;


OPTIMIZE adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;


-- ============================================================
-- 4. Inspect table shape and layout metadata
-- ============================================================

DESCRIBE DETAIL adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab;

DESCRIBE DETAIL adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;

DESCRIBE HISTORY adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;

SHOW TBLPROPERTIES adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;


SELECT
  'activity_history_wide_scale_lab' AS table_name,
  COUNT(*) AS total_row_count,
  COUNT(DISTINCT activity_date) AS distinct_activity_date_count,
  COUNT(DISTINCT license_tier_id) AS distinct_license_tier_count,
  COUNT(DISTINCT department_id) AS distinct_department_count,
  COUNT(DISTINCT region_code) AS distinct_region_count,
  MIN(activity_date) AS min_activity_date,
  MAX(activity_date) AS max_activity_date
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab

UNION ALL

SELECT
  'activity_history_wide_clustered_lab' AS table_name,
  COUNT(*) AS total_row_count,
  COUNT(DISTINCT activity_date) AS distinct_activity_date_count,
  COUNT(DISTINCT license_tier_id) AS distinct_license_tier_count,
  COUNT(DISTINCT department_id) AS distinct_department_count,
  COUNT(DISTINCT region_code) AS distinct_region_count,
  MIN(activity_date) AS min_activity_date,
  MAX(activity_date) AS max_activity_date
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;


-- ============================================================
-- 5. Query Profile comparison
-- Run each query separately and compare Query Profile:
--   - Files read
--   - Files pruned
--   - Bytes read
--   - Bytes pruned
--   - Partitions read
-- ============================================================


-- 5A. Baseline unclustered query

SET use_cached_result = false;

SELECT
  department_id,
  region_code,
  license_tier_id,
  COUNT(*) AS activity_history_row_count,
  SUM(synthetic_qualifying_activity_count) AS qualifying_activity_count,
  COUNT(DISTINCT activity_date) AS active_date_count
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab
WHERE activity_date BETWEEN DATE '2026-04-01' AND DATE '2026-05-15'
  AND license_tier_id = 'PBI-PRO'
GROUP BY
  department_id,
  region_code,
  license_tier_id
ORDER BY
  department_id,
  region_code,
  license_tier_id;


-- 5B. Liquid Clustered query

SET use_cached_result = false;

SELECT
  department_id,
  region_code,
  license_tier_id,
  COUNT(*) AS activity_history_row_count,
  SUM(synthetic_qualifying_activity_count) AS qualifying_activity_count,
  COUNT(DISTINCT activity_date) AS active_date_count
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab
WHERE activity_date BETWEEN DATE '2026-04-01' AND DATE '2026-05-15'
  AND license_tier_id = 'PBI-PRO'
GROUP BY
  department_id,
  region_code,
  license_tier_id
ORDER BY
  department_id,
  region_code,
  license_tier_id;


-- 5C. Detail drill-through query on clustered table

SET use_cached_result = false;

SELECT
  activity_history_lab_id,
  assignment_id,
  employee_id,
  activity_date,
  report_as_of_date,
  license_tier_id,
  department_id,
  region_code,
  activity_status,
  recency_bucket,
  synthetic_event_type_code,
  synthetic_qualifying_activity_count,
  synthetic_payload
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab
WHERE activity_date BETWEEN DATE '2026-04-01' AND DATE '2026-05-15'
  AND license_tier_id = 'PBI-PRO'
  AND department_id = 'DEPT-007'
  AND region_code = 'NA-WEST'
ORDER BY
  activity_date,
  assignment_id,
  activity_history_lab_id
LIMIT 100;


-- ============================================================
-- 6. Write-maintenance and manual OPTIMIZE lab
-- Purpose:
--   Show how repeated writes can create many files and how
--   manual OPTIMIZE compacts existing files.
-- ============================================================

DROP TABLE IF EXISTS adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;

CREATE TABLE adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab
USING DELTA
AS
SELECT
  activity_history_lab_id,
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  activity_date,
  report_as_of_date,
  days_since_last_qualifying_activity,
  is_recent_qualifying_activity_as_of_report_date,
  scale_iteration_id,
  synthetic_event_type_code,
  synthetic_qualifying_activity_count,
  synthetic_payload,
  lab_created_at_utc
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab
WHERE scale_iteration_id BETWEEN 0 AND 999;


INSERT INTO adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab
SELECT
  activity_history_lab_id,
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  activity_date,
  report_as_of_date,
  days_since_last_qualifying_activity,
  is_recent_qualifying_activity_as_of_report_date,
  scale_iteration_id,
  synthetic_event_type_code,
  synthetic_qualifying_activity_count,
  synthetic_payload,
  lab_created_at_utc
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab
WHERE scale_iteration_id BETWEEN 1000 AND 1999;


INSERT INTO adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab
SELECT
  activity_history_lab_id,
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  activity_date,
  report_as_of_date,
  days_since_last_qualifying_activity,
  is_recent_qualifying_activity_as_of_report_date,
  scale_iteration_id,
  synthetic_event_type_code,
  synthetic_qualifying_activity_count,
  synthetic_payload,
  lab_created_at_utc
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab
WHERE scale_iteration_id BETWEEN 2000 AND 2999;


INSERT INTO adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab
SELECT
  activity_history_lab_id,
  assignment_id,
  employee_id,
  department_id,
  region_code,
  license_tier_id,
  activity_status,
  recency_bucket,
  activity_date,
  report_as_of_date,
  days_since_last_qualifying_activity,
  is_recent_qualifying_activity_as_of_report_date,
  scale_iteration_id,
  synthetic_event_type_code,
  synthetic_qualifying_activity_count,
  synthetic_payload,
  lab_created_at_utc
FROM adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab
WHERE scale_iteration_id BETWEEN 3000 AND 3999;


-- Inspect file count before manual OPTIMIZE.

DESCRIBE DETAIL adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;

DESCRIBE HISTORY adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;


-- Manual OPTIMIZE compaction.

OPTIMIZE adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;


-- Inspect file count and history after manual OPTIMIZE.

DESCRIBE DETAIL adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;

DESCRIBE HISTORY adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;


-- ============================================================
-- 7. Closeout cleanup
-- Run this section only after evidence is collected.
-- ============================================================

DROP TABLE IF EXISTS adb_classic_compute_catalog.perf_lab.activity_history_wide_scale_lab;

DROP TABLE IF EXISTS adb_classic_compute_catalog.perf_lab.activity_history_wide_clustered_lab;

DROP TABLE IF EXISTS adb_classic_compute_catalog.perf_lab.activity_history_write_maintenance_lab;

SHOW TABLES IN adb_classic_compute_catalog.perf_lab;