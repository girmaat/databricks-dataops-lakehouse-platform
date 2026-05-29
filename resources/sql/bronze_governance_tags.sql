-- Apply Unity Catalog tags to a real governed Bronze table.

USE CATALOG adb_classic_compute_catalog;
USE SCHEMA bronze;

-- table-level governance tags

ALTER TABLE employee_directory_snapshot_raw
SET TAGS (
    'data_layer' = 'bronze',
    'source_dataset' = 'employee_directory_snapshot',
    'data_classification' = 'employee_sensitive_raw'
);


-- column-level tags to real sensitive columns.
-- 1. employee_id is an employee identifier.

ALTER TABLE employee_directory_snapshot_raw
ALTER COLUMN employee_id
SET TAGS (
    'sensitivity' = 'employee_identifier',
    'pii_category' = 'identifier'
);

-- 2. user_principal_name contains a user/email-style identity value.

ALTER TABLE employee_directory_snapshot_raw
ALTER COLUMN user_principal_name
SET TAGS (
    'sensitivity' = 'personally_identifiable_information',
    'pii_category' = 'user_principal_name'
);


--  Validate table-level tags.

SELECT
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM adb_classic_compute_catalog.information_schema.table_tags
WHERE schema_name = 'bronze'
  AND table_name = 'employee_directory_snapshot_raw'
ORDER BY tag_name;


--  Validate column-level tags.

SELECT
    catalog_name,
    schema_name,
    table_name,
    column_name,
    tag_name,
    tag_value
FROM adb_classic_compute_catalog.information_schema.column_tags
WHERE schema_name = 'bronze'
  AND table_name = 'employee_directory_snapshot_raw'
ORDER BY column_name, tag_name;