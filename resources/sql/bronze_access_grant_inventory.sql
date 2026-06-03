-- Inspect the current Unity Catalog privilege state before designing
-- safe permitted/denied access tests for real Bronze objects.

-- Catalog-level privileges currently assigned.
SHOW GRANTS ON CATALOG adb_classic_compute_catalog;

-- Bronze schema privileges currently assigned.
SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.bronze;

-- Gold schema privileges currently assigned.
SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.gold;

-- Governance schema privileges currently assigned.
SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.governance;

-- Existing real Bronze table: employee-sensitive table.
SHOW GRANTS ON TABLE
  adb_classic_compute_catalog.bronze.employee_directory_snapshot_raw;

-- Existing real Bronze table: assignment events.
SHOW GRANTS ON TABLE
  adb_classic_compute_catalog.bronze.license_assignment_change_event_raw;

-- Existing real Bronze table: application usage events.
SHOW GRANTS ON TABLE
  adb_classic_compute_catalog.bronze.application_usage_event_raw;