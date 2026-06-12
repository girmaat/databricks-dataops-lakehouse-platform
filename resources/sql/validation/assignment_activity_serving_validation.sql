USE CATALOG adb_classic_compute_catalog;
USE SCHEMA gold;


-- Base Gold row count vs serving view row count vs governed view row count

WITH row_counts AS (
  SELECT
    'base_gold_row_count' AS validation_name,
    COUNT(*) AS row_count
  FROM adb_classic_compute_catalog.gold.assignment_activity_status

  UNION ALL

  SELECT
    'serving_view_row_count' AS validation_name,
    COUNT(*) AS row_count
  FROM adb_classic_compute_catalog.gold.assignment_activity_status_serving_vw

  UNION ALL

  SELECT
    'governed_view_row_count' AS validation_name,
    COUNT(*) AS row_count
  FROM adb_classic_compute_catalog.gold.assignment_activity_governed_vw
),
expected AS (
  SELECT
    MAX(CASE WHEN validation_name = 'base_gold_row_count' THEN row_count END) AS base_gold_count,
    MAX(CASE WHEN validation_name = 'serving_view_row_count' THEN row_count END) AS serving_view_count,
    MAX(CASE WHEN validation_name = 'governed_view_row_count' THEN row_count END) AS governed_view_count
  FROM row_counts
)
SELECT
  r.validation_name,
  r.row_count,
  CASE
    WHEN r.row_count = e.base_gold_count THEN 'PASS'
    ELSE 'REVIEW'
  END AS validation_status
FROM row_counts AS r
CROSS JOIN expected AS e
ORDER BY r.validation_name;



-- Duplicate assignment_id check in the serving view
-- Expected result: no rows

SELECT
  assignment_id,
  COUNT(*) AS row_count
FROM adb_classic_compute_catalog.gold.assignment_activity_status_serving_vw
GROUP BY assignment_id
HAVING COUNT(*) > 1;



-- KPI totals from base Gold vs dynamic KPI view vs KPI snapshot table


WITH kpi_totals AS (
  SELECT
    'gold_base' AS source_name,
    COUNT(*) AS assignment_count,
    SUM(CASE WHEN activity_status = 'recent_qualifying_activity' THEN 1 ELSE 0 END)
      AS recent_qualifying_assignment_count,
    SUM(CASE WHEN activity_status = 'terminated_employee_review' THEN 1 ELSE 0 END)
      AS terminated_employee_review_count
  FROM adb_classic_compute_catalog.gold.assignment_activity_status

  UNION ALL

  SELECT
    'dynamic_kpi_view' AS source_name,
    SUM(assignment_count) AS assignment_count,
    SUM(recent_qualifying_assignment_count) AS recent_qualifying_assignment_count,
    SUM(terminated_employee_review_count) AS terminated_employee_review_count
  FROM adb_classic_compute_catalog.gold.assignment_activity_kpi_vw

  UNION ALL

  SELECT
    'kpi_snapshot_table' AS source_name,
    SUM(assignment_count) AS assignment_count,
    SUM(recent_qualifying_assignment_count) AS recent_qualifying_assignment_count,
    SUM(terminated_employee_review_count) AS terminated_employee_review_count
  FROM adb_classic_compute_catalog.gold.assignment_activity_kpi_snapshot
),
expected AS (
  SELECT
    assignment_count AS expected_assignment_count,
    recent_qualifying_assignment_count AS expected_recent_qualifying_assignment_count,
    terminated_employee_review_count AS expected_terminated_employee_review_count
  FROM kpi_totals
  WHERE source_name = 'gold_base'
)
SELECT
  k.source_name,
  k.assignment_count,
  k.recent_qualifying_assignment_count,
  k.terminated_employee_review_count,
  CASE
    WHEN k.assignment_count = e.expected_assignment_count
     AND k.recent_qualifying_assignment_count = e.expected_recent_qualifying_assignment_count
     AND k.terminated_employee_review_count = e.expected_terminated_employee_review_count
    THEN 'PASS'
    ELSE 'REVIEW'
  END AS validation_status
FROM kpi_totals AS k
CROSS JOIN expected AS e
ORDER BY k.source_name;



-- Governed view masking check

SELECT
  employee_id,
  masked_user_principal_name,
  full_user_principal_name_for_admin_review,
  activity_status,
  region_code,
  department_id
FROM adb_classic_compute_catalog.gold.assignment_activity_governed_vw
ORDER BY employee_id;