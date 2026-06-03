-- External Delta Reference Table

USE CATALOG adb_classic_compute_catalog;
USE SCHEMA governance;

--  confirm whether an external location exists
SHOW EXTERNAL LOCATIONS;

-- Optional, after you identify the location name:
DESCRIBE EXTERNAL LOCATION adb_classic_compute_lab;

-- create external table

CREATE TABLE IF NOT EXISTS adb_classic_compute_catalog.governance.department_region_reference (
  department_id STRING COMMENT 'Business department identifier used by employee records',
  department_name STRING COMMENT 'Business-friendly department name for reporting',
  business_unit STRING COMMENT 'Higher-level business unit grouping',
  region_code STRING COMMENT 'Region code used by employee records',
  region_name STRING COMMENT 'Business-friendly region name',
  region_group STRING COMMENT 'Higher-level geographic reporting group',
  is_active BOOLEAN COMMENT 'Whether the reference mapping is currently active',
  valid_from_date DATE COMMENT 'First date the mapping is valid',
  valid_to_date DATE COMMENT 'Last date the mapping is valid; NULL means open-ended',
  reference_owner STRING COMMENT 'Business owner of this reference data'
)
USING DELTA
LOCATION 'abfss://unity-catalog-storage@dbstoragenfquzccy2q5dk.dfs.core.windows.net/7405613835806371/reference/department_region_reference'
TBLPROPERTIES (
  'project.layer' = 'governance',
  'project.source_type' = 'external_reference',
  'project.business_entity' = 'department_region',
  'project.s03_03_question_evidence' = 'managed_vs_external_delta_tables'
);

COMMENT ON TABLE adb_classic_compute_catalog.governance.department_region_reference IS
'Externally governed department and region reference data used to validate and enrich employee/license/usage analytics.';

-- rerun the script without creating duplicates.


MERGE INTO adb_classic_compute_catalog.governance.department_region_reference AS target
USING (
  SELECT * FROM VALUES
    ('DEPT-001', 'Executive Office',      'Corporate',             'NA-EAST',    'North America East',    'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-002', 'Finance',               'Corporate',             'NA-EAST',    'North America East',    'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-003', 'Engineering',           'Product & Technology',  'NA-WEST',    'North America West',    'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-004', 'Human Resources',       'Corporate',             'NA-EAST',    'North America East',    'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-005', 'Sales',                 'Revenue',               'NA-CENTRAL', 'North America Central', 'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-006', 'Marketing',             'Revenue',               'NA-EAST',    'North America East',    'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-007', 'Security',              'Technology Risk',        'NA-WEST',    'North America West',    'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team'),
    ('DEPT-008', 'Operations',            'Operations',             'NA-CENTRAL', 'North America Central', 'Americas', true, DATE '2026-01-01', CAST(NULL AS DATE), 'reference_data_team')
  AS source (
    department_id,
    department_name,
    business_unit,
    region_code,
    region_name,
    region_group,
    is_active,
    valid_from_date,
    valid_to_date,
    reference_owner
  )
)
ON target.department_id = source.department_id
AND target.region_code = source.region_code

WHEN MATCHED THEN UPDATE SET
  department_name = source.department_name,
  business_unit = source.business_unit,
  region_name = source.region_name,
  region_group = source.region_group,
  is_active = source.is_active,
  valid_from_date = source.valid_from_date,
  valid_to_date = source.valid_to_date,
  reference_owner = source.reference_owner

WHEN NOT MATCHED THEN INSERT (
  department_id,
  department_name,
  business_unit,
  region_code,
  region_name,
  region_group,
  is_active,
  valid_from_date,
  valid_to_date,
  reference_owner
)
VALUES (
  source.department_id,
  source.department_name,
  source.business_unit,
  source.region_code,
  source.region_name,
  source.region_group,
  source.is_active,
  source.valid_from_date,
  source.valid_to_date,
  source.reference_owner
);

-- Validation 

SELECT *
FROM adb_classic_compute_catalog.governance.department_region_reference
ORDER BY department_id, region_code;

DESCRIBE DETAIL adb_classic_compute_catalog.governance.department_region_reference;

DESCRIBE EXTENDED adb_classic_compute_catalog.governance.department_region_reference;

DESCRIBE HISTORY adb_classic_compute_catalog.governance.department_region_reference;

SHOW TBLPROPERTIES adb_classic_compute_catalog.governance.department_region_reference;

-- Preview future Silver/Gold usage, without starting Silver yet.

SELECT
  e.department_id,
  e.region_code,
  r.department_name,
  r.business_unit,
  r.region_name,
  r.region_group,
  COUNT(*) AS bronze_employee_rows
FROM adb_classic_compute_catalog.bronze.employee_directory_snapshot_raw e
LEFT JOIN adb_classic_compute_catalog.governance.department_region_reference r
  ON TRIM(UPPER(e.department_id)) = r.department_id
 AND TRIM(UPPER(e.region_code)) = r.region_code
GROUP BY
  e.department_id,
  e.region_code,
  r.department_name,
  r.business_unit,
  r.region_name,
  r.region_group
ORDER BY e.department_id, e.region_code;



DELETE FROM adb_classic_compute_catalog.governance.department_region_reference
WHERE department_id LIKE 'D-%';
