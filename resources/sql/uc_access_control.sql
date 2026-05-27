SHOW GRANTS ON CATALOG adb_classic_compute_catalog;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.bronze;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.gold;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.governance;


-- establish minimum namespace access for the platform engineering role
-- grants permit traversal to the Bronze schema only

GRANT USE CATALOG
ON CATALOG adb_classic_compute_catalog
TO `license_platform_engineers`;

GRANT USE SCHEMA
ON SCHEMA adb_classic_compute_catalog.bronze
TO `license_platform_engineers`;

GRANT USE CATALOG
ON CATALOG adb_classic_compute_catalog
TO `license_reporting_analysts`;

GRANT USE SCHEMA
ON SCHEMA adb_classic_compute_catalog.gold
TO `license_reporting_analysts`;

GRANT USE CATALOG
ON CATALOG adb_classic_compute_catalog
TO `license_governance_admins`;

GRANT USE SCHEMA
ON SCHEMA adb_classic_compute_catalog.governance
TO `license_governance_admins`;

GRANT MANAGE
ON SCHEMA adb_classic_compute_catalog.governance
TO `license_governance_admins`;

GRANT USE CATALOG
ON CATALOG adb_classic_compute_catalog
TO `license_auditors`;

GRANT USE SCHEMA
ON SCHEMA adb_classic_compute_catalog.monitoring
TO `license_auditors`;

-- Validation
SHOW GRANTS ON CATALOG adb_classic_compute_catalog;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.bronze;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.gold;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.governance;

SHOW GRANTS ON SCHEMA adb_classic_compute_catalog.monitoring;